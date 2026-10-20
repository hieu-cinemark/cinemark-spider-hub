"""features/profiles của Threads: parse response hovercard + worker hàng đợi (client/Redis giả, không gọi Threads thật)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from social_crawler.constants.threads import PROFILE_QUEUE_REDIS_KEY, PROFILE_RESULTS_REDIS_KEY
from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient
from social_crawler.spiders.threads.features.profiles import profiles
from social_crawler.spiders.threads.features.profiles.extract import ProfileQueryError, extract_profile

SAMPLE = {
    "data": {
        "user": {
            "pk": "63602824079",
            "id": "63602824079",
            "username": "_ph.hp04_",
            "full_name": "Hoàng Phúc iu Miu",
            "follower_count": 80,
            "is_verified": False,
            "text_post_app_is_private": False,
            "biography": "...",
            "profile_pic_url": "...",
        }
    },
    "extensions": {"is_final": True},
}


def test_extract_profile_sample() -> None:
    assert extract_profile(SAMPLE) == {
        "username": "_ph.hp04_",
        "pk": "63602824079",
        "full_name": "Hoàng Phúc iu Miu",
        "follower_count": 80,
        "is_verified": False,
    }


def test_extract_profile_user_null_is_not_found() -> None:
    assert extract_profile({"data": {"user": None}, "extensions": {}}) is None


@pytest.mark.parametrize(
    "response",
    [
        {"errors": [{"message": "doc_id not found"}], "data": None},
        {"data": {}},
        {"message": "x"},
        "not json",
        {"data": {"user": "oops"}},
    ],
)
def test_extract_profile_unexpected_shape_raises(response: Any) -> None:
    with pytest.raises(ProfileQueryError):
        extract_profile(response)


def test_fetch_profile_sends_hovercard_query() -> None:
    client = ThreadsGraphQLClient.__new__(ThreadsGraphQLClient)
    captured: dict[str, Any] = {}

    def fake_run(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return SAMPLE

    client._run = fake_run  # type: ignore[method-assign]
    assert client.fetch_profile("@_ph.hp04_")["follower_count"] == 80
    assert captured["doc_id"] == "29082997647979831"
    assert captured["friendly_name"] == "BarcelonaUsernameHovercardImplDirectQuery"
    assert captured["overrides"] == {"username": "_ph.hp04_"}
    assert captured["extra_headers"] == {"x-root-field-name": "xdt_text_app_user_by_username"}
    assert captured["template"]["__relay_internal__pv__BarcelonaIsLoggedInrelayprovider"] is True


class FakeCache:
    def __init__(self, queue: list[str]) -> None:
        self.sets: dict[str, set[str]] = {PROFILE_QUEUE_REDIS_KEY: set(queue)}
        self.hashes: dict[str, dict[str, str]] = {}

    def spop(self, key: str) -> str | None:
        members = self.sets.get(key) or set()
        return members.pop() if members else None

    def sadd(self, key: str, *members: str) -> int:
        self.sets.setdefault(key, set()).update(members)
        return len(members)

    def hset_json(self, key: str, field: str, value: Any) -> None:
        self.hashes.setdefault(key, {})[field] = json.dumps(value)

    def expire(self, key: str, ttl_seconds: int) -> None:
        pass

    def incr(self, key: str, amount: int = 1) -> int:
        return 1


class FakeClient:
    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses

    def fetch_profile(self, handle: str) -> dict[str, Any] | None:
        value = self.responses[handle]
        if isinstance(value, Exception):
            raise value
        return value


def _patch(monkeypatch: pytest.MonkeyPatch, cache: FakeCache, client: FakeClient) -> None:
    monkeypatch.setattr(profiles, "RedisCache", lambda: cache)
    monkeypatch.setattr(profiles, "ThreadsGraphQLClient", lambda redis_cache=None: client)
    monkeypatch.setattr(profiles.time, "sleep", lambda _s: None)


def test_run_writes_found_and_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = FakeCache(["a", "b"])
    _patch(monkeypatch, cache, FakeClient({"a": extract_profile(SAMPLE), "b": None}))
    assert profiles.run(10) == 0
    results = {k: json.loads(v) for k, v in cache.hashes[PROFILE_RESULTS_REDIS_KEY].items()}
    assert results["a"]["found"] is True and results["a"]["follower_count"] == 80
    assert results["b"]["found"] is False and "fetched_at" in results["b"]
    assert not cache.sets[PROFILE_QUEUE_REDIS_KEY]


def test_run_stops_and_requeues_on_unexpected_response(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = FakeCache(["a"])
    _patch(monkeypatch, cache, FakeClient({"a": ProfileQueryError("doc_id expired")}))
    assert profiles.run(10) == 1
    assert cache.sets[PROFILE_QUEUE_REDIS_KEY] == {"a"}
    assert PROFILE_RESULTS_REDIS_KEY not in cache.hashes


def test_run_respects_max(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = FakeCache(["a", "b", "c"])
    _patch(monkeypatch, cache, FakeClient({"a": None, "b": None, "c": None}))
    assert profiles.run(2) == 0
    assert len(cache.hashes[PROFILE_RESULTS_REDIS_KEY]) == 2
    assert len(cache.sets[PROFILE_QUEUE_REDIS_KEY]) == 1
