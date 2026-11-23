"""Client TikTok cho kênh: danh sách video của một kênh."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from curl_cffi import requests as curl_requests

from social_crawler.constants.tiktok import (
    CURL_CFFI_UA,
    POST_ITEM_LIST_URL,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokClient, TikTokNetworkError

logger = get_logger(__name__)

_SEC_UID_RE = re.compile(r'"secUid":"(MS4wLjAB[^"]+)"')


class TikTokChannelClient(TikTokClient):
    """Video đã đăng của một kênh qua /api/post/item_list/ - xem comment POST_ITEM_LIST_URL trong
    constants/tiktok.py. Gọi warm_session() trước."""

    def resolve_sec_uid(self, username: str) -> str | None:
        """secUid của @username, đọc từ HTML trang profile (một GET thường, không cần JS). None nếu
        trang không chứa nó (kênh không tồn tại, hoặc TikTok trả trang chặn)."""
        self._throttle()
        try:
            resp = self._session.get(
                f"https://www.tiktok.com/@{quote(username)}",
                headers={"user-agent": CURL_CFFI_UA, "accept-language": "en-US,en;q=0.9,vi;q=0.8"},
                cookies=self._cookies,
                timeout=20,
            )
        except curl_requests.RequestsError as exc:
            raise TikTokNetworkError(f"Failed to load @{username}'s profile page: {exc}") from exc
        match = _SEC_UID_RE.search(resp.text or "")
        return match.group(1) if match else None

    def list_posts(self, sec_uid: str, username: str, *, cursor: int = 0, count: int = 35) -> dict[str, Any]:
        """Một trang video. `cursor` là timestamp ms mà response trước trả về (0 = mới nhất)."""
        return self._request(
            POST_ITEM_LIST_URL,
            {
                "secUid": sec_uid,
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "user",
                "needPinnedItemIds": "true",
                "post_item_list_request_type": "0",
                "enable_cache": "false",
                "video_encoding": "dash",
                "referer": "",
                "root_referer": "",
            },
            referer=f"https://www.tiktok.com/@{quote(username)}",
        )
