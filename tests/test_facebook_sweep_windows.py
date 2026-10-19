"""facebook_search: khoảng ngày của lượt quét - các ngày gần nhất mỗi ngày một khoảng, phần cũ hơn gộp."""

from __future__ import annotations

from datetime import date

from social_crawler.spiders.facebook.features.search.search import _auto_window_days, _date_windows

ANCHOR = date(2026, 10, 19)


def test_recent_days_are_single_day_windows_then_grouped() -> None:
    windows = list(_date_windows(60, _auto_window_days(60), ANCHOR, daily_days=7))
    assert windows[:7] == [(date(2026, 10, 19 - i), date(2026, 10, 19 - i)) for i in range(7)]
    assert windows[7] == (date(2026, 10, 7), date(2026, 10, 12))  # 6 ngày, ngay trước các khoảng từng ngày
    assert windows[-1][0] == date(2026, 8, 21)  # phủ đúng 60 ngày
    covered = sum((end - start).days + 1 for start, end in windows)
    assert covered == 60 and len(windows) == 16


def test_without_daily_days_keeps_fixed_width() -> None:
    windows = list(_date_windows(30, 3, ANCHOR))
    assert len(windows) == 10 and windows[0] == (date(2026, 10, 17), date(2026, 10, 19))


def test_sweep_shorter_than_daily_days() -> None:
    assert list(_date_windows(3, 1, ANCHOR, daily_days=7)) == [
        (date(2026, 10, 19), date(2026, 10, 19)),
        (date(2026, 10, 18), date(2026, 10, 18)),
        (date(2026, 10, 17), date(2026, 10, 17)),
    ]
