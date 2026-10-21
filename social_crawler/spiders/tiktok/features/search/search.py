"""
Spider tìm kiếm từ khoá tự do của TikTok - cùng kiến trúc với tiktok_hashtag_search (curl_cffi,
danh tính khách synthetic mới cho mỗi lần thử, ký ở local), chỉ khác endpoint:
/api/search/general/full/ thay vì /api/challenge/item_list/. Xem comment SEARCH_GENERAL_URL
trong constants/tiktok.py cho các điều kiện đã xác nhận thực tế (2026-10-13): phải warm
session để có msToken trước, chỉ general/full nhận synthetic (item/full thì không), và trang 2
trở đi cần search_id của trang 1.

Kế thừa TikTokHashtagSearchSpider để dùng lại nguyên phần thử lại theo danh tính mới, xử lý
lỗi/mã thoát, khử trùng SEEN_POSTS_KEY + publish RAW_POSTS_TOPIC, và chip hashtag liên quan
cho BFS trên dashboard (các #tag xuất hiện cùng trong kết quả tìm kiếm cũng có ích như trong
feed hashtag). self.hashtag giữ chính từ khoá - nó chỉ là nhãn nguồn trong log/item và gốc để
AI (Bee) chấm hashtag liên quan. Số lần thử là tiktok_hashtag_max_attempts (dùng chung).

Chạy:
    scrapy crawl tiktok_keyword_search -a keyword="trại buôn người"
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from social_crawler.constants.tiktok import MAX_CONSECUTIVE_EMPTY_NEW_PAGES
from social_crawler.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokSearchClient
from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response,
    update_related_hashtag_counts,
)
from social_crawler.spiders.tiktok.features.hashtag_search.search import TikTokHashtagSearchSpider
from social_crawler.spiders.tiktok.items import TikTokVideoItem

logger = get_logger(__name__)

# Số kết quả mỗi trang - đúng giá trị trình duyệt gửi; TikTok trả tối đa chừng này video/trang.
SEARCH_PAGE_SIZE = 50
# Mặc định thấp hơn 100 trang của hashtag: 12 kết quả/trang và độ liên quan giảm nhanh sau vài
# chục trang đầu. Consumer vẫn ghi đè được qua max_pages.
# 30 -> 60 (2026-10-16): lấy nhiều bài nhất có thể; vẫn dừng sớm sau MAX_CONSECUTIVE_EMPTY_NEW_PAGES trang không có bài mới.
DEFAULT_MAX_PAGES = 60


def _as_item_list(response: dict[str, Any]) -> dict[str, Any]:
    """data[] của general/full trộn video (type=1) với thẻ người dùng/live/gợi ý; item video mang
    đúng dạng item của challenge/item_list, nên bọc lại thành {"itemList": [...]} để dùng chung
    extractor của hashtag_search."""
    return {"itemList": [row["item"] for row in response.get("data") or [] if row.get("type") == 1 and row.get("item")]}


class TikTokKeywordSearchSpider(TikTokHashtagSearchSpider):
    name = "tiktok_keyword_search"

    def __init__(self, keyword: str = "", max_pages: int = DEFAULT_MAX_PAGES, *args, **kwargs):
        kwargs.pop("hashtag", None)
        # Lớp cha tự giới hạn max_pages ở BFS_MAX_PAGES khi bfs_depth > 0.
        super().__init__(*args, hashtag=keyword.strip(), max_pages=max_pages, **kwargs)

    async def _crawl_with_fresh_account(self) -> AsyncIterator[TikTokVideoItem]:
        """Một lần thử: danh tính synthetic mới, warm lấy msToken, rồi phân trang. TikTokBlockedError
        nổi lên start() của lớp cha để thử lại với danh tính khác."""
        if not self.hashtag:
            logger.error("missing_keyword", hint='scrapy crawl tiktok_keyword_search -a keyword="<từ khoá>"')
            return
        client = await asyncio.to_thread(TikTokSearchClient, redis_cache=self._cache, synthetic=True)
        await asyncio.to_thread(client.warm_session)

        cursor = 0
        search_id: str | None = None
        empty_new_streak = 0
        for page in range(1, self.max_pages + 1):
            response = await asyncio.to_thread(
                client.search_keyword, self.hashtag, cursor=cursor, count=SEARCH_PAGE_SIZE, search_id=search_id
            )
            wrapped = _as_item_list(response)
            videos = extract_response(wrapped)
            update_related_hashtag_counts(wrapped, self._related_hashtag_counts, exclude_ids=set())

            async for item in self._publish_page(videos):
                yield item
            new_posts = self._page_new_posts
            empty_new_streak = 0 if new_posts else empty_new_streak + 1
            logger.info(
                "page_crawled",
                page=page,
                new_posts=new_posts,
                fetched=len(videos),
                empty_new_streak=empty_new_streak,
                keyword=self.hashtag,
            )

            if empty_new_streak >= MAX_CONSECUTIVE_EMPTY_NEW_PAGES:
                logger.info(
                    "keyword_skipped_no_new_posts",
                    telegram=True,
                    keyword=self.hashtag,
                    keyword_id=self.keyword_id,
                    pages=page,
                    empty_new_streak=empty_new_streak,
                )
                break
            if not response.get("has_more"):
                break
            next_cursor = response.get("cursor")
            if next_cursor is None or int(next_cursor) == cursor:
                break
            cursor = int(next_cursor)
            search_id = search_id or (response.get("log_pb") or {}).get("impr_id")
