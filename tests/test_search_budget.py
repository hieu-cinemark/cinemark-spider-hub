"""services.search_budget: hạn mức request Facebook mỗi tài khoản mỗi ngày."""

from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.runtime import search_budget


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
    monkeypatch.setattr(search_budget, "DAILY_CAP", 2)
    monkeypatch.setattr(search_budget, "PERIODS", [])
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
    monkeypatch.setattr(search_budget, "DAILY_CAP", 1)
    monkeypatch.setattr(search_budget, "PERIODS", [])
    monkeypatch.setattr(search_budget, "rest_account_until", lambda *a: True)
    monkeypatch.setattr(search_budget, "get_accounts", lambda platform: [])
    assert not search_budget.ensure_budget(FakeCache())


def test_hard_cap(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 500)
    monkeypatch.setattr(search_budget, "PERIODS", [])
    assert search_budget.hard_cap() == 600
    monkeypatch.setattr(search_budget, "DAILY_CAP", 0)
    assert search_budget.hard_cap() == 0


def test_disabled(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 0)
    assert search_budget.ensure_budget(FakeCache())


def test_keep_user_agent_restores_marketplace_ua():
    from social_crawler.spiders.facebook.auth.cookies import keep_user_agent

    assert keep_user_agent("c_user=1; xs=a", "c_user=1; xs=old; useragent=TW96aWxsYQ%3D%3D") == (
        "c_user=1; xs=a; useragent=TW96aWxsYQ%3D%3D"
    )
    assert keep_user_agent("c_user=1; xs=a", "c_user=1; xs=old") == "c_user=1; xs=a"
    assert keep_user_agent("c_user=1; useragent=X", "useragent=Y") == "c_user=1; useragent=X"


def test_budget_split_by_period(monkeypatch):
    from datetime import datetime

    monkeypatch.setattr(search_budget, "DAILY_CAP", 500)
    monkeypatch.setattr(search_budget, "PERIODS", ["05:30", "12:00", "18:00"])
    monkeypatch.setattr(search_budget, "FRESH_SHARE", 0)  # chia theo buổi chỉ còn khi không chia theo phần
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
    assert loop._stale_facebook_job("facebook", {"type": "page_feed"}, old)
    assert not loop._stale_facebook_job("facebook", {"keyword": "x"}, fresh)
    assert not loop._stale_facebook_job("facebook", {"type": "comments"}, old)
    assert not loop._stale_facebook_job("threads", {"keyword": "x"}, old)
