"""Hàm dùng chung cho test: đặt giá trị một khoá `proxy_settings` trong một test (còn lại là bản chụp mặc định của cinemark-api)."""

from __future__ import annotations

from typing import Any

from social_crawler.db import proxy_settings


def cfg(key: str, value: Any) -> None:
    """`cfg("fb_daily_request_cap", 0)`. Danh sách (khoá dạng "a,b,c") truyền được dạng list. Tự reset sau mỗi test (fixture autouse trong conftest)."""
    if isinstance(value, (list, tuple)):
        value = ",".join(str(item) for item in value)
    proxy_settings._test_values[key] = value
    proxy_settings._settings_cache = None
