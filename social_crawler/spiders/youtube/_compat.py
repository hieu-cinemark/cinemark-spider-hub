"""Thay cho app.config.* / app.exceptions của data-miner (nơi chép logic YouTube sang, 13/11/2026): logger, lỗi đổi cấu
trúc, proxy. Proxy do chỗ gọi truyền vào từng hàm (proxy=...); get_proxy()/registry giữ cho code chép nguyên văn chạy được."""

from __future__ import annotations

import logging
import os

HTTP_MAX_CONNECTIONS = 20
HTTP_MAX_KEEPALIVE = 10


class Logger:
    @staticmethod
    def get(name: str) -> logging.Logger:
        return logging.getLogger(name)


class CrawlerBaseError(Exception):
    pass


class YouTubeStructureChangedError(CrawlerBaseError):
    """YouTube đổi cấu trúc phản hồi - kèm đường dẫn khoá để sửa parser."""

    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} | context={self.context}" if self.context else base


async def get_proxy() -> str | None:
    """Proxy mặc định (YOUTUBE_PROXY_URL) cho code gọi không truyền proxy; để trống = IP máy."""
    return os.getenv("YOUTUBE_PROXY_URL") or None


class _Registry:
    def rotate(self) -> None:
        """data-miner đổi proxy trong pool khi lỗi kết nối; ở spider-hub proxy xoay phía nhà cung cấp."""


registry = _Registry()
