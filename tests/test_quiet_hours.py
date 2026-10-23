"""consumer.loop: giờ yên lặng 23:30-05:30 (giờ VN) cho Facebook / Threads."""

from datetime import datetime
from zoneinfo import ZoneInfo

from social_crawler.consumer import loop

VN = ZoneInfo("Asia/Ho_Chi_Minh")


def at(h, m):
    return datetime(2026, 10, 24, h, m, tzinfo=VN)


def test_inside_window_across_midnight(monkeypatch):
    monkeypatch.setattr(loop, "QUIET_HOURS", "23:30-05:30")
    assert loop._quiet_seconds_left("facebook", at(23, 45)) == (5 * 60 + 45) * 60
    assert loop._quiet_seconds_left("threads", at(3, 0)) == 150 * 60


def test_outside_window_and_other_platforms(monkeypatch):
    monkeypatch.setattr(loop, "QUIET_HOURS", "23:30-05:30")
    assert loop._quiet_seconds_left("facebook", at(5, 30)) == 0
    assert loop._quiet_seconds_left("facebook", at(12, 0)) == 0
    assert loop._quiet_seconds_left("tiktok", at(2, 0)) == 0


def test_disabled(monkeypatch):
    monkeypatch.setattr(loop, "QUIET_HOURS", "")
    assert loop._quiet_seconds_left("facebook", at(2, 0)) == 0
