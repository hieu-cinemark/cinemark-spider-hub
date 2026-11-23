"""Client TikTok cho tìm kiếm từ khoá."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from social_crawler.constants.tiktok import (
    SEARCH_GENERAL_URL,
    SEARCH_WEB_SEARCH_CODE,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokClient

logger = get_logger(__name__)


class TikTokSearchClient(TikTokClient):
    """Tìm kiếm từ khoá tự do qua /api/search/general/full/ - xem comment SEARCH_GENERAL_URL trong
    constants/tiktok.py cho vì sao không phải search/item/full. Gọi warm_session() trước."""

    def search_keyword(
        self, keyword: str, *, cursor: int = 0, count: int = 12, search_id: str | None = None
    ) -> dict[str, Any]:
        """Một trang kết quả. Trang 2 trở đi phải mang `search_id` = log_pb.impr_id của trang 1,
        nếu không TikTok trả search_nil_info + 0 item (đã xác nhận thực tế 2026-10-13)."""
        extra = {
            "keyword": keyword,
            "count": str(count),
            "cursor": str(cursor),
            "offset": str(cursor),
            "from_page": "search",
            "search_source": "normal_search",
            "is_non_personalized_search": "0",
            "web_search_code": SEARCH_WEB_SEARCH_CODE,
            "device_type": "web_h265",
            # Trình duyệt gửi hai tham số này rỗng ở trang search.
            "referer": "",
            "root_referer": "",
        }
        if search_id:
            extra["search_id"] = search_id
        return self._request(SEARCH_GENERAL_URL, extra, referer=f"https://www.tiktok.com/search?q={quote(keyword)}")
