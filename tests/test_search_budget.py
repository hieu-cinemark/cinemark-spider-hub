"""services.search_budget: hạn mức request Facebook mỗi tài khoản mỗi ngày."""

from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.runtime import search_budget
from tests.support import cfg


class FakeCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.data.pop(key, None)

    def incr(self, key):
        self.data[key] = int(self.data.get(key) or 0) + 1
        return self.data[key]

    def expire(self, key, ttl):
        pass


def test_counts_and_rests_account_over_cap(monkeypatch):
    cfg("fb_daily_request_cap", 2)
    cfg("fb_budget_periods", [])
    rested = []
    monkeypatch.setattr(search_budget, "rest_account_until", lambda platform, key, until: rested.append(key) or True)
    monkeypatch.setattr(search_budget, "get_accounts", lambda platform: [{"id": "2", "email": "b@x"}])
    cache = FakeCache()
    cache.data[ACTIVE_ACCOUNT_REDIS_KEY] = "a@x"
    for _ in range(2):
        assert search_budget.ensure_budget(cache)
        search_budget.count_request("a@x", cache)
    # a@x đã đủ 2: nghỉ, gỡ con trỏ active; còn b@x chưa dùng -> vẫn chạy (bootstrap chọn b@x)
    assert search_budget.ensure_budget(cache)
    assert rested == ["a@x"] and cache.get(ACTIVE_ACCOUNT_REDIS_KEY) is None


def test_all_accounts_capped_skips(monkeypatch):
    cfg("fb_daily_request_cap", 1)
    cfg("fb_budget_periods", [])
    monkeypatch.setattr(search_budget, "rest_account_until", lambda *a: True)
    monkeypatch.setattr(search_budget, "get_accounts", lambda platform: [])
    assert not search_budget.ensure_budget(FakeCache())


def test_hard_cap(monkeypatch):
    cfg("fb_daily_request_cap", 500)
    cfg("fb_budget_periods", [])
    assert search_budget.hard_cap() == 600
    cfg("fb_daily_request_cap", 0)
    assert search_budget.hard_cap() == 0


def test_disabled(monkeypatch):
    cfg("fb_daily_request_cap", 0)
    assert search_budget.ensure_budget(FakeCache())


def test_budget_split_by_period(monkeypatch):
    from datetime import datetime

    cfg("fb_daily_request_cap", 500)
    cfg("fb_budget_periods", ["05:30", "12:00", "18:00"])
    cfg("fb_fresh_share", 0)  # chia theo buổi chỉ còn khi không chia theo phần
    at = lambda hm: datetime(2026, 10, 25, int(hm[:2]), int(hm[3:]), tzinfo=search_budget._VN)
    assert search_budget.allowed_now(at("06:00")) == 167
    assert search_budget.allowed_now(at("12:30")) == 334
    assert search_budget.allowed_now(at("19:00")) == 500
    assert search_budget.allowed_now(at("02:00")) == 500  # giờ yên lặng: không chia
    # hết phần buổi sáng -> nghỉ tới 12:00; hết cả ngày -> nghỉ tới 05:30 hôm sau
    assert search_budget._rest_end(at("09:00"), 170) == at("12:00")
    assert search_budget._rest_end(at("19:00"), 500).hour == 5


def test_stale_facebook_jobs_dropped():
    import time

    from social_crawler.consumer import loop

    old = int((time.time() - 9 * 3600) * 1000)
    fresh = int(time.time() * 1000)
    assert loop._stale_facebook_job("facebook", {"keyword": "x"}, old)
    assert loop._stale_facebook_job("facebook", {"type": "search"}, old)
    assert not loop._stale_facebook_job("facebook", {"keyword": "x"}, fresh)
    assert not loop._stale_facebook_job("facebook", {"type": "comments"}, old)
    assert not loop._stale_facebook_job("threads", {"keyword": "x"}, old)


class _SetCache(FakeCache):
    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value


def test_new_account_cap_when_global_cap_off(monkeypatch):
    """30/10/2026: hạn mức chung tắt nhưng tài khoản mới vẫn có trần riêng (NEW_ACCOUNT_DAILY_CAP x RAMP)."""
    cfg("fb_daily_request_cap", 0)
    cfg("fb_new_account_daily_cap", 10)
    cfg("fb_new_account_ramp", [0.2])
    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: {"new@x"})
    rested = []
    monkeypatch.setattr(search_budget, "rest_account_until", lambda platform, key, until: rested.append(key) or True)
    released = []
    monkeypatch.setattr("social_crawler.runtime.lanes.release_account", lambda account, cache: released.append(account))
    cache = _SetCache()
    assert search_budget.new_account_cap("old@x", cache) == 0  # tài khoản cũ không giới hạn
    assert search_budget.new_account_cap("new@x", cache) == 2  # ngày crawl đầu: 20% của 10
    for _ in range(2):
        assert search_budget.ensure_budget(cache, "new@x")
        search_budget.count_request("new@x", cache)
    assert not search_budget.ensure_budget(cache, "new@x")
    assert rested == ["new@x"] and released == ["new@x"]
    assert search_budget.ensure_budget(cache, "old@x")


def test_new_account_cap_off_when_global_cap_on(monkeypatch):
    cfg("fb_daily_request_cap", 500)
    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: {"new@x"})
    assert search_budget.new_account_cap("new@x", FakeCache()) == 0
