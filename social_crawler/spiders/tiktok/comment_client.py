"""Client TikTok cho bình luận của một video."""

from __future__ import annotations

from typing import Any

from social_crawler.constants.tiktok import (
    COMMENT_ITEM_LIST_URL,
    COMMENT_REPLY_LIST_URL,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokClient

logger = get_logger(__name__)


class TikTokCommentClient(TikTokClient):
    """Client curl_cffi khách cho /api/comment/list/ — cùng đường tạo synthetic như
    TikTokHashtagClient. Gọi warm_session() trước để có msToken."""

    def list_comments(
        self,
        aweme_id: str,
        *,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """Một trang comment cấp một của `aweme_id` (id video TikTok). `video_url` nên là permalink
        công khai dùng làm Referer; quay về đường dẫn synthetic /video/<id> khi chỗ gọi chỉ có id.
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{aweme_id}"
        return self._request(
            COMMENT_ITEM_LIST_URL,
            {
                "aweme_id": str(aweme_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "enter_from": "tiktok_web",
                "is_non_personalized": "false",
                "current_region": "VN",
            },
            referer=referer,
        )

    def list_replies(
        self,
        *,
        comment_id: str,
        item_id: str,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """Một trang reply dưới một `comment_id` cấp một trên video `item_id`. Cùng cách ký như
        list_comments; đã xác nhận thực tế 2026-09-18 với /api/comment/list/reply/ (cursor bắt
        đầu từ 0).
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{item_id}"
        return self._request(
            COMMENT_REPLY_LIST_URL,
            {
                "comment_id": str(comment_id),
                "item_id": str(item_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "current_region": "VN",
            },
            referer=referer,
        )
