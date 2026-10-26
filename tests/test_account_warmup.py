"""services/warmup.py: tài khoản mới phải nuôi đủ ACCOUNT_WARMUP_DAYS ngày trước khi crawler giao ra."""

from __future__ import annotations

import pytest

from social_crawler.services import warmup


class _FakeCache:
    def __init__(self) -> None:
        self.sets: dict[str, set[str]] = {}
        self.values: dict[str, int] = {}

    def sadd(self, key: str, *members: str) -> int:
        bucket = self.sets.setdefault(key, set())
        added = len(set(members) - bucket)
        bucket.update(members)
        return added

    def expire(self, key: str, ttl_seconds: int) -> None:
        pass

    def incr(self, key: str, amount: int = 1) -> int:
        self.values[key] = self.values.get(key, 0) + amount
        return self.values[key]

    def get(self, key: str):
        return self.values.get(key)


@pytest.fixture
def cache(monkeypatch) -> _FakeCache:
    monkeypatch.setattr(warmup, "WARMUP_DAYS", 3)
    monkeypatch.setattr(warmup, "accounts_created_since", lambda platform, since: {"new1": "n1@x.com", "new2": None})
    monkeypatch.setattr(warmup, "nurture_only_accounts", lambda platform: {})
    return _FakeCache()


def test_same_day_counts_once(cache) -> None:
    assert warmup.record_nurture_day("facebook", "new1", "2026-10-19", cache) == 1
    assert warmup.record_nurture_day("facebook", "new1", "2026-10-19", cache) == 1
    assert warmup.record_nurture_day("facebook", "new1", "2026-10-20", cache) == 2


def test_unwarmed_until_enough_days(cache) -> None:
    for day in ("2026-10-19", "2026-10-20"):
        warmup.record_nurture_day("facebook", "new1", day, cache)
    assert warmup.unwarmed_account_ids("facebook", cache) == {"new1", "new2"}
    warmup.record_nurture_day("facebook", "new1", "2026-10-21", cache)
    assert warmup.unwarmed_account_ids("facebook", cache) == {"new2"}


def test_nurture_only_account_counts_as_warming_even_when_warmed(cache, monkeypatch) -> None:
    """platform_accounts.nurture_only (26/10/2026): tài khoản vừa đăng nhập lại cần nuôi thêm - crawler bỏ qua, job nuôi
    vẫn chạy và xếp nó lên trước."""
    monkeypatch.setattr(warmup, "nurture_only_accounts", lambda platform: {"old1": "Menno@Hotmail.com"})
    assert "old1" in warmup.unwarmed_account_ids("facebook", cache)
    assert warmup.is_warming_key("facebook", "menno@hotmail.com", cache)
    monkeypatch.setattr(warmup, "WARMUP_DAYS", 0)
    assert warmup.unwarmed_account_ids("facebook", cache) == {"old1"}


def test_disabled_with_zero_days(cache, monkeypatch) -> None:
    monkeypatch.setattr(warmup, "WARMUP_DAYS", 0)
    assert warmup.unwarmed_account_ids("facebook", cache) == set()


def test_acquire_account_excludes_warming_accounts(monkeypatch) -> None:
    from social_crawler.services import pool

    seen: dict[str, object] = {}
    monkeypatch.setattr(warmup, "unwarmed_account_ids", lambda platform: {"new1"})
    monkeypatch.setattr(pool, "claim_account", lambda platform, exclude=frozenset(): seen.update(exclude=exclude) or {"id": "old"})
    assert pool.acquire_account("facebook") == {"id": "old"}
    assert seen["exclude"] == {"new1"}


def test_nurture_skips_without_pinned_proxy(monkeypatch) -> None:
    from social_crawler import nurture_accounts as na

    def exhausted(*_a, **_k):
        raise na.pool.ProxyPoolExhaustedError("pinned proxy cooling down")

    monkeypatch.setattr(na.pool, "acquire_proxy_for_account", exhausted)
    with pytest.raises(na.NurtureProxyUnavailable):
        na._proxy_for("facebook", "acct")


def test_active_pointer_by_email_is_warming(cache) -> None:
    assert warmup.is_warming_key("facebook", "N1@x.com", cache)
    for day in ("2026-10-19", "2026-10-20", "2026-10-21"):
        warmup.record_nurture_day("facebook", "new1", day, cache)
    assert not warmup.is_warming_key("facebook", "n1@x.com", cache)
    assert not warmup.is_warming_key("facebook", "old@x.com", cache)
