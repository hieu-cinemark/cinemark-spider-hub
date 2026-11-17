"""Hàng đợi việc tin cậy trên Redis, dùng chung cho mọi kênh Redis của spider-hub (làn ưu tiên, hàng đợi rảnh, đọc page Facebook công khai,
kênh Threads, YouTube).

Mỗi hàng đợi gồm HAI khoá: `key` (việc chờ) và `processing` (việc đang chạy). Worker `claim` một việc (chuyển nguyên tử từ `key` sang
`processing`), chạy xong thì `ack`; chết giữa chừng thì việc vẫn nằm ở `processing` và lần khởi động sau `recover` trả nó về `key`.
Trước 17/11/2026 mỗi kênh tự viết lại bộ ba claim/ack/recover này (năm hàm `_recover_*` gần giống nhau trong consumer/loop.py).

    queue = JobQueue("crawl_idle:facebook")             # processing mặc định là "<key>:processing"
    raw, request = queue.claim(cache)
    ...
    queue.ack(cache, raw)
"""

from __future__ import annotations

from typing import Any

from social_crawler.clients.redis import RedisCache


class JobQueue:
    """Tên khoá của một hàng đợi + các thao tác tin cậy trên nó (xem docstring module). Không giữ kết nối: truyền `RedisCache`."""

    def __init__(self, key: str, processing: str | None = None) -> None:
        self.key = key
        self.processing = processing or f"{key}:processing"

    def claim(self, cache: RedisCache) -> tuple[str | None, Any]:
        """(chuỗi gốc, JSON) của việc lấy được; (None, None) khi hàng đợi rỗng."""
        return cache.claim_json(self.key, self.processing)

    def ack(self, cache: RedisCache, raw: str) -> None:
        """Việc đã xong: xoá khỏi `processing`."""
        cache.ack(self.processing, raw)

    def requeue(self, cache: RedisCache, raw: str, destination: str | None = None) -> bool:
        """Trả đúng việc vừa nhận về hàng đợi (mặc định `key`) vì chưa chạy được (hết proxy, làn bị chặn...)."""
        return cache.requeue_claim(self.processing, destination or self.key, raw)

    def recover(self, cache: RedisCache) -> int:
        """Trả mọi việc còn dở ở `processing` (tiến trình trước chết giữa chừng) về `key`; trả số việc đã trả."""
        return cache.requeue_processing(self.processing, self.key)
