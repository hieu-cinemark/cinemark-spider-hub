"""
Spider danh sách video kênh/người dùng TikTok - gọi thẳng /api/post/item_list/ qua curl_cffi
với một danh tính khách synthetic mới, ký ở local, giống tiktok_hashtag_search. Trước
2026-10-13 spider này phải điều khiển một trình duyệt Patchright thật vì endpoint đòi X-Gnarly
5.3.2 + X-Dynosaur thật; signer 5.3.2 ở signature/ mở được nó (xem comment POST_ITEM_LIST_URL
trong constants/tiktok.py).

Luồng một lần thử: TikTokChannelClient(synthetic=True) -> warm_session() (msToken) ->
resolve_sec_uid() đọc secUid từ HTML trang profile -> phân trang list_posts() theo cursor
(timestamp ms) tới khi hasMore=false hoặc max_pages. TikTokBlockedError (body rỗng) thử lại với
một danh tính mới, tối đa tiktok_hashtag_max_attempts lần (dùng chung với hashtag/search).

Bản thân dạng response không cần logic trích xuất mới: item của /api/post/item_list/ mang đúng
các trường như item /api/challenge/item_list/, nên features/channel_videos/extract.py chỉ
re-export extractor của hashtag_search.

Chạy:
    scrapy crawl tiktok_channel_videos -a username="linzenguyen"

Chỉ ra output, không DB/Kafka (2026-09-25): khác mọi spider TikTok khác, spider này không
publish lên RAW_POSTS_TOPIC - video của một kênh được đọc thẳng từ
output/<spider_name>_<timestamp>.json (xem FEEDS trong settings.py), không lưu vào D1. Khử
trùng là một set thường trong lượt chạy, không phải SEEN_POSTS_KEY - key đó là dấu "đã
publish lên Kafka" dùng chung của hashtag_search/comments; sadd vào nó ở đây (trong khi
không bao giờ thực sự publish) sẽ âm thầm khiến hashtag_search bỏ qua một video nó chưa
publish, chỉ vì spider này tình cờ thấy nó trước.

Được crawl_request_consumer.py điều phối cho request type="channel_videos".
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator

import scrapy

from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.db.proxy_settings import get_setting
from social_crawler.core.logger import get_logger
from social_crawler.runtime import pool
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokChannelClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.channel_videos.extract import extract_response
from social_crawler.spiders.tiktok.items import TikTokChannelVideoItem

logger = get_logger(__name__)

# Trình duyệt xin 35 video/trang ở profile.
PAGE_SIZE = 35


class TikTokChannelVideosSpider(scrapy.Spider):
    name = "tiktok_channel_videos"

    # Không bao giờ đi qua downloader của Scrapy — curl_cffi ký trực tiếp.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        username: str = "",
        keyword_id: str | None = None,
        max_pages: int = 20,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.username = username.lstrip("@").strip()
        # Spider này không cần hiểu bên trong - chỉ truyền tiếp lên từng item được yield để cùng
        # dạng với mọi item nguồn video TikTok khác, dù ở đây không có gì publish nó đi đâu. Thường
        # là None với lượt crawl kênh (không có từ khoá nào điều khiển).
        self.keyword_id = keyword_id
        self.max_pages = int(max_pages)
        # Set thường trong lượt chạy, không phải SEEN_POSTS_KEY - xem docstring module để biết vì sao
        # đây không được là cache khử trùng Redis dùng chung.
        self._seen_video_ids: set[str] = set()
        self._post_count = 0

    async def start(self):
        if not self.username:
            logger.error("missing_username", hint='scrapy crawl tiktok_channel_videos -a username="<a channel handle>"')
            return

        try:
            max_attempts = int(get_setting("tiktok_hashtag_max_attempts"))
            for attempt in range(1, max_attempts + 1):
                try:
                    async for item in self._crawl_with_fresh_identity():
                        yield item
                    break
                except TikTokBlockedError as exc:
                    if attempt < max_attempts:
                        logger.warning(
                            "blocked_retrying_with_different_account",
                            attempt=attempt,
                            max_attempts=max_attempts,
                            username=self.username,
                            error=str(exc),
                        )
                        continue
                    logger.error("blocked", telegram=True, username=self.username, error=str(exc))
                    sys.exit(1)
        except pool.ProxyPoolExhaustedError as exc:
            logger.error("tiktok_proxy_pool_exhausted", telegram=True, error=str(exc))
            sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
        except TikTokRateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc), username=self.username)
            sys.exit(1)
        except TikTokNetworkError as exc:
            # Cùng lý do như tiktok_comments: lỗi proxy/kết nối phải được xếp hàng lại, không được
            # commit như một lượt chạy thành công với 0 video.
            logger.error("network_error", telegram=True, error=str(exc), username=self.username)
            sys.exit(PROXY_EXHAUSTED_EXIT_CODE)

        logger.info("crawl_finished", telegram=True, posts=self._post_count, username=self.username)

    async def _crawl_with_fresh_identity(self) -> AsyncIterator[TikTokChannelVideoItem]:
        client = await asyncio.to_thread(TikTokChannelClient, synthetic=True)
        await asyncio.to_thread(client.warm_session)

        sec_uid = await asyncio.to_thread(client.resolve_sec_uid, self.username)
        if not sec_uid:
            # Trang profile không mang secUid: kênh không tồn tại, hoặc IP/danh tính này nhận trang
            # chặn - thử lại với danh tính mới là cách xử lý chung cho cả hai.
            raise TikTokBlockedError(f"no secUid on @{self.username}'s profile page")

        cursor = 0
        for page in range(1, self.max_pages + 1):
            response = await asyncio.to_thread(
                client.list_posts, sec_uid, self.username, cursor=cursor, count=PAGE_SIZE
            )
            videos = extract_response(response)

            new_posts = 0
            for video in videos:
                video_id = video.get("video_id")
                if not video_id:
                    continue
                video_id = str(video_id)
                # Chỉ trong lượt chạy - xem docstring module để biết vì sao đây không phải SEEN_POSTS_KEY.
                if video_id in self._seen_video_ids:
                    continue
                self._seen_video_ids.add(video_id)
                new_posts += 1
                self._post_count += 1
                yield TikTokChannelVideoItem(username=self.username, **video)

            logger.info("page_crawled", page=page, new_posts=new_posts, fetched=len(videos), username=self.username)

            if not response.get("hasMore"):
                break
            next_cursor = response.get("cursor")
            if next_cursor in (None, "", "-1", -1) or str(next_cursor) == str(cursor):
                break
            cursor = int(next_cursor)
