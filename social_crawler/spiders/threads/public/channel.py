"""Đọc bài của một kênh Threads không đăng nhập: hovercard (username -> pk), rồi lật tab Threads 25 bài/trang, mới nhất
trước, tới khi gặp bài cũ hơn `since_ts`, hết bài hoặc đủ `max_pages`.

Bài trả về đúng dạng extract_post của spider tìm kiếm, nên ingest không phải đổi gì. Trang nào cũng giao ngay cho
`on_batch` để job bị ngắt giữa chừng vẫn giữ phần đã đọc.
"""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from social_crawler.spiders.threads.features.search.extract import extract_response
from social_crawler.spiders.threads.public.client import (
    ThreadsPublicBlocked,
    ThreadsPublicClient,
    ThreadsPublicError,
)

_HANDLE_RE = re.compile(r"threads\.(?:com|net)/@([A-Za-z0-9._]+)", re.IGNORECASE)


def handle_from_url(value: str) -> str | None:
    """'https://www.threads.com/@abc', '@abc' hoặc 'abc' -> 'abc'."""
    value = (value or "").strip()
    match = _HANDLE_RE.search(value)
    if match:
        return match.group(1).lower()
    value = value.lstrip("@")
    return value.lower() if re.fullmatch(r"[A-Za-z0-9._]{1,30}", value) else None


@dataclass
class ChannelResult:
    handle: str
    status: str = "ok"  # ok | not_found | blocked | error
    error: str | None = None
    user_id: str | None = None
    followers: int | None = None
    pages: int = 0
    posts: int = 0
    stop_reason: str | None = None  # since | exhausted | max_pages | stopped | batch_failed
    oldest_ts: int | None = None
    newest_ts: int | None = None
    profile: dict[str, Any] = field(default_factory=dict)


def fetch_channel(
    handle: str,
    proxy: str | None = None,
    *,
    since_ts: int = 0,
    max_pages: int = 0,
    request_interval: float = 3.0,
    on_batch: Callable[[list[dict[str, Any]]], bool] | None = None,
    should_stop: Callable[[], bool] | None = None,
    client: ThreadsPublicClient | None = None,
) -> ChannelResult:
    """max_pages=0 là không giới hạn (dừng theo since_ts). Lỗi mạng mỗi trang thử lại một lần với phiên mới."""
    result = ChannelResult(handle=handle)
    own_client = client is None
    client = client or ThreadsPublicClient(proxy)
    try:
        try:
            user = client.user_by_username(handle)
        except ThreadsPublicBlocked as exc:
            result.status, result.error = "blocked", str(exc)
            return result
        except ThreadsPublicError as exc:
            result.status, result.error = "error", str(exc)
            return result
        if not user:
            result.status = "not_found"
            return result
        result.user_id = str(user.get("pk") or user.get("id"))
        result.followers = user.get("follower_count")
        result.profile = {
            "username": user.get("username") or handle,
            "full_name": user.get("full_name"),
            "follower_count": user.get("follower_count"),
            "is_verified": user.get("is_verified"),
            "profile_pic_url": user.get("profile_pic_url"),
        }

        cursor: str | None = None
        while True:
            if should_stop and should_stop():
                result.stop_reason = "stopped"
                break
            time.sleep(request_interval * random.uniform(0.7, 1.3))
            try:
                data, next_cursor = client.profile_threads_page(result.user_id, cursor)
            except ThreadsPublicBlocked as exc:
                result.status, result.error = "blocked", str(exc)
                break
            except ThreadsPublicError:
                # Một lần thử lại với phiên/token mới: lỗi mạng lẻ hoặc proxy xoay giữa chừng.
                if own_client:
                    client.close()
                    client = ThreadsPublicClient(proxy)
                time.sleep(request_interval * 2)
                try:
                    data, next_cursor = client.profile_threads_page(result.user_id, cursor)
                except ThreadsPublicBlocked as exc:
                    result.status, result.error = "blocked", str(exc)
                    break
                except ThreadsPublicError as exc:
                    result.status, result.error = "error", str(exc)
                    break
            result.pages += 1
            posts = [p for p in extract_response(data) if p.get("post_id")]
            # Tab gồm bài của chính kênh; bài trích dẫn của người khác lồng trong đó cũng khớp chữ ký nên lọc theo tác giả.
            own = [p for p in posts if str(p.get("author_id") or "") == result.user_id] or posts
            in_window = [p for p in own if not since_ts or (p.get("timestamp") or 0) >= since_ts]
            stamps = [p["timestamp"] for p in own if p.get("timestamp")]
            if stamps:
                result.oldest_ts = min(stamps + ([result.oldest_ts] if result.oldest_ts else []))
                result.newest_ts = max(stamps + ([result.newest_ts] if result.newest_ts else []))
            if in_window:
                if on_batch and not on_batch(in_window):
                    result.stop_reason = "batch_failed"
                    break
                result.posts += len(in_window)
            # Bài ghim có thể cũ hơn mốc nằm đầu trang 1, nên chỉ dừng khi cả trang đã cũ hơn mốc.
            if since_ts and stamps and max(stamps) < since_ts:
                result.stop_reason = "since"
                break
            if not next_cursor:
                result.stop_reason = "exhausted"
                break
            if max_pages and result.pages >= max_pages:
                result.stop_reason = "max_pages"
                break
            cursor = next_cursor
        return result
    finally:
        if own_client:
            client.close()
