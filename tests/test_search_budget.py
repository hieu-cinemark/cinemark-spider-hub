"""services.search_budget: hạn mức tìm kiếm Facebook mỗi tài khoản mỗi ngày."""

from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.services import search_budget


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
    rested = []
    monkeypatch.setattr(search_budget, "rest_account_until", lambda platform, key, until: rested.append(key) or True)
    monkeypatch.setattr(search_budget, "get_accounts", lambda platform: [{"id": "2", "email": "b@x"}])
    cache = FakeCache()
    cache.data[ACTIVE_ACCOUNT_REDIS_KEY] = "a@x"
    for _ in range(2):
        assert search_budget.ensure_budget(cache)
        search_budget.record_search(cache)
    # a@x đã đủ 2: nghỉ, gỡ con trỏ active; còn b@x chưa dùng -> vẫn chạy (bootstrap chọn b@x)
    assert search_budget.ensure_budget(cache)
    assert rested == ["a@x"] and cache.get(ACTIVE_ACCOUNT_REDIS_KEY) is None


def test_all_accounts_capped_skips(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 1)
    monkeypatch.setattr(search_budget, "rest_account_until", lambda *a: True)
    monkeypatch.setattr(search_budget, "get_accounts", lambda platform: [])
    assert not search_budget.ensure_budget(FakeCache())


def test_disabled(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 0)
    assert search_budget.ensure_budget(FakeCache())
