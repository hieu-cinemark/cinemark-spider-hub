"""consumer.jobs.search._sweep_days_for: quét hằng ngày ngắn, đủ 60 ngày mỗi tuần một lần cho mỗi từ khoá."""

from __future__ import annotations

from datetime import date, timedelta

from social_crawler.consumer.jobs import search as crc


class _FixedDate(date):
    current = date(2026, 10, 19)

    @classmethod
    def today(cls):
        return cls.current


def test_full_sweep_once_a_week_per_keyword(monkeypatch) -> None:
    """Chế độ quét ngắn hằng ngày (FACEBOOK_DAILY_SWEEP_DAYS=4) - không còn là mặc định từ 28/10/2026."""
    monkeypatch.setattr(crc, "date", _FixedDate)
    monkeypatch.setattr(crc, "DAILY_SWEEP_DAYS", 4)
    for keyword_id in ("kw_a", "kw_b", "kw_c"):
        days = []
        for offset in range(crc.FULL_SWEEP_EVERY_DAYS):
            _FixedDate.current = date(2026, 10, 19) + timedelta(days=offset)
            days.append(crc._sweep_days_for({"keyword_id": keyword_id}))
        assert days.count(crc.DEFAULT_SWEEP_DAYS) == 1
        assert days.count(crc.DAILY_SWEEP_DAYS) == crc.FULL_SWEEP_EVERY_DAYS - 1


def test_explicit_dates_and_missing_keyword_unchanged(monkeypatch) -> None:
    assert crc._sweep_days_for({"keyword_id": "kw", "start_date": "2026-10-01", "end_date": "2026-10-10"}) == 10
    assert crc._sweep_days_for({}) == crc.DEFAULT_SWEEP_DAYS


def test_default_sweeps_the_full_window_every_run(monkeypatch) -> None:
    """28/10/2026: mặc định (DAILY_SWEEP_DAYS=0) mọi lượt đều quét đủ DEFAULT_SWEEP_DAYS ngày."""
    monkeypatch.setattr(crc, "DAILY_SWEEP_DAYS", 0)
    for keyword_id in ("kw_a", "kw_b", "kw_c"):
        assert crc._sweep_days_for({"keyword_id": keyword_id}) == crc.DEFAULT_SWEEP_DAYS
