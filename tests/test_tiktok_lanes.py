"""TikTok lanes: each lane holds its own proxy port; one lane (default) keeps the old behaviour."""

from __future__ import annotations

from typing import ClassVar

import pytest

from social_crawler.clients import proxy_provider
from social_crawler.consumer.common import crawl_job_key
from social_crawler.runtime import lanes


class FakeCache:
    store: ClassVar[dict[str, object]] = {}

    def set_nx(self, key, value, ttl_seconds):
        if key in self.store:
            return False
        self.store[key] = value
        return True

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.store[key] = value


ROWS = [{"url": "10.0.0.1:8001"}, {"url": "10.0.0.1:8002"}]


@pytest.fixture(autouse=True)
def _reset_lanes():
    yield
    lanes.set_worker_lane(None)
    lanes.set_current_lane(None)


@pytest.fixture
def cache(monkeypatch):
    FakeCache.store = {}
    monkeypatch.setattr("social_crawler.clients.redis.RedisCache", FakeCache)
    monkeypatch.setattr(lanes, "INSTANCE_ID", "vps")
    return FakeCache.store


def _as_lane(monkeypatch, lane: int) -> None:
    monkeypatch.setattr(lanes, "TIKTOK_LANES", 2)
    lanes.set_worker_lane(lane)


def test_single_lane_keeps_every_port_and_claims_nothing(monkeypatch, cache):
    monkeypatch.setattr(lanes, "TIKTOK_LANES", 1)
    assert proxy_provider._lane_candidates(ROWS) == ROWS
    assert cache == {}


def test_each_lane_holds_one_port_and_keeps_it(monkeypatch, cache):
    _as_lane(monkeypatch, 0)
    assert proxy_provider._lane_candidates(ROWS) == [ROWS[0]]
    _as_lane(monkeypatch, 1)
    assert proxy_provider._lane_candidates(ROWS) == [ROWS[1]]
    _as_lane(monkeypatch, 0)
    assert proxy_provider._lane_candidates(ROWS) == [ROWS[0]]  # own port again, not the other lane's


def test_lane_without_free_port_gets_nothing(monkeypatch, cache):
    _as_lane(monkeypatch, 0)
    assert proxy_provider._lane_candidates(ROWS[:1]) == [ROWS[0]]
    _as_lane(monkeypatch, 1)
    assert proxy_provider._lane_candidates(ROWS[:1]) == []


def test_lease_returns_none_when_lane_has_no_port(monkeypatch, cache):
    _as_lane(monkeypatch, 1)
    cache[proxy_provider._LANE_PORT_KEY.format(url=ROWS[0]["url"])] = "vps:0"
    monkeypatch.setattr(proxy_provider, "get_provider", lambda key: {"key": key, "api_url": "https://reset", "token": ""})
    monkeypatch.setattr(proxy_provider, "_provider_rows", lambda key: ROWS[:1])
    resets = []
    monkeypatch.setattr(proxy_provider, "reset_proxy", lambda *a: resets.append(a) or (True, "1.2.3.4"))
    assert proxy_provider.lease_proxy("tiktok_us") is None
    assert resets == []  # never resets the IP of another lane's port


def test_running_job_key_and_subprocess_lane(monkeypatch):
    lanes.set_current_lane(None)
    lanes.set_worker_lane(1)
    assert crawl_job_key("tiktok") == "crawl_job:tiktok:lane1"
    assert crawl_job_key("facebook") == "crawl_job:facebook"  # Facebook reads its own lane, untouched
    assert lanes.current_lane() is None
    lanes.set_worker_lane(None)
    monkeypatch.setenv(lanes.WORKER_LANE_ENV, "2")
    assert lanes.worker_lane() == 2
