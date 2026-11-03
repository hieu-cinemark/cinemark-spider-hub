"""Hạn mức ngày tăng dần khi Facebook chạy lại sau một thời gian nghỉ."""

import importlib
from datetime import datetime, timedelta

import pytest

from social_crawler.runtime import search_budget as budget

DAY1 = datetime(2026, 11, 10, 9, 0, tzinfo=budget._VN)


class Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    monkeypatch.setattr(budget, "RESTART_DAILY_CAPS", [100, 150, 200, 300])
    monkeypatch.setattr(budget, "RESTART_IDLE_DAYS", 3)
    monkeypatch.setattr(budget, "DAILY_CAP", 400)
    monkeypatch.setattr(budget, "RAMP", [])


def _crawl(cache, day):
    budget._note_crawl_day("a@x", cache, day)


def test_hourly_cap_defaults_to_off(monkeypatch):
    monkeypatch.delenv("FACEBOOK_HOURLY_REQUEST_CAP", raising=False)
    fresh = importlib.reload(budget)
    try:
        assert fresh.hourly_cap("facebook") == 0
    finally:
        importlib.reload(budget)


def test_never_crawled_starts_at_first_step():
    assert budget.ramp_factor("a@x", Cache(), DAY1) == 100 / 400


def test_caps_rise_each_day_then_reach_the_full_cap():
    cache = Cache()
    for offset, expected in enumerate([100, 150, 200, 300, 400, 400]):
        day = DAY1 + timedelta(days=offset)
        _crawl(cache, day)
        assert budget.ramp_factor("a@x", cache, day) * 400 == expected


def test_short_gap_keeps_counting_and_long_gap_restarts():
    cache = Cache()
    _crawl(cache, DAY1)
    two_days_later = DAY1 + timedelta(days=2)
    assert budget.ramp_factor("a@x", cache, two_days_later) * 400 == 200
    _crawl(cache, two_days_later)
    after_idle = two_days_later + timedelta(days=3)
    assert budget.ramp_factor("a@x", cache, after_idle) * 400 == 100
    _crawl(cache, after_idle)
    assert budget.ramp_factor("a@x", cache, after_idle + timedelta(days=1)) * 400 == 150


def test_full_cap_account_restarts_after_schedule_was_off():
    cache = Cache()
    _crawl(cache, DAY1)
    for offset in range(1, 6):
        _crawl(cache, DAY1 + timedelta(days=offset))
    assert budget.ramp_factor("a@x", cache, DAY1 + timedelta(days=5)) == 1.0
    assert budget.ramp_factor("a@x", cache, DAY1 + timedelta(days=9)) * 400 == 100


def test_lower_of_new_account_and_restart_applies(monkeypatch):
    monkeypatch.setattr(budget, "RAMP", [0.2])
    monkeypatch.setattr(budget, "_new_account_keys", lambda: {"a@x"})
    assert budget.ramp_factor("a@x", Cache(), DAY1) == 0.2


def test_disabled_list_and_no_daily_cap_mean_no_restart_limit(monkeypatch):
    monkeypatch.setattr(budget, "RESTART_DAILY_CAPS", [])
    assert budget.ramp_factor("a@x", Cache(), DAY1) == 1.0
    monkeypatch.setattr(budget, "RESTART_DAILY_CAPS", [100])
    monkeypatch.setattr(budget, "DAILY_CAP", 0)
    assert budget.ramp_factor("a@x", Cache(), DAY1) == 1.0
