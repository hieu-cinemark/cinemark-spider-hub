"""
Spider tìm kiếm Threads không bao giờ mở trình duyệt: gọi thẳng endpoint GraphQL qua
curl_cffi (giả dấu vân tay TLS của Chrome), dùng token mà
`social_crawler.spiders.threads.auth.bootstrap` đã cache. Giống
social_crawler.spiders.facebook.features.search.search - xem docstring của module đó để
biết đầy đủ lý do đằng sau max_pages/dedupe. Ở đây không cài đặt quét theo khoảng ngày (tìm
kiếm Threads không có bộ lọc "Ngày đăng" trong giao diện như Facebook), nên nó gần với vòng
crawl không lọc đơn giản của Facebook hơn.

Chạy:
    scrapy crawl threads_search -a query="keyword"

Truyền -a dedupe=false để tắt khử trùng giữa các lượt chạy - mặc định bật mỗi khi kết nối
được Redis, lặng lẽ quay về chỉ khử trùng trong lượt chạy nếu không.
"""

from __future__ import annotations

import asyncio
import sys
from typing import AsyncIterator

import scrapy

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.threads import (
    MAX_CONSECUTIVE_EMPTY_NEW_PAGES,
    SEEN_POSTS_KEY,
    SEEN_POSTS_TTL_SECONDS,
)
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.error_alerts import note_transient_error
from social_crawler.spiders.post_log import log_crawled_post
from social_crawler.spiders.search_query import build_search_query
from social_crawler.spiders.threads.auth.graphql_client import (
    CheckpointRequiredError,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    ThreadsGraphQLClient,
    find_page_info,
)
from social_crawler.spiders.threads.features.search.extract import extract_response
from social_crawler.spiders.threads.items import ThreadsPostItem

logger = get_logger(__name__)


class ThreadsSearchSpider(scrapy.Spider):
    name = "threads_search"

    # Spider này không bao giờ đi qua downloader của Scrapy (nó gọi thẳng curl_cffi để giả dấu
    # vân tay TLS của Chrome thật), nên robots.txt và downloader middleware không áp dụng ở đây.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        query: str = "test",
        keyword_id: str | None = None,
        count: int = 10,
        max_pages: int = 50,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        # Chỉ lời gọi tìm kiếm gửi đi thật mới dùng cái này - bản thân self.query vẫn là từ khoá
        # trơn ở mọi chỗ khác (item Kafka, log, keyword_match) để việc khớp phía sau với text từ
        # khoá lưu trong D1 không bị ảnh hưởng. Xem docstring của build_search_query để biết vì sao
        # có cái này.
        self.search_query = build_search_query(query)
        # Spider này không cần hiểu bên trong - chỉ truyền tiếp lên Kafka ở mỗi bài được publish,
        # giống keyword_id của facebook_search.
        self.keyword_id = keyword_id
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._post_count = 0
        self._kafka = KafkaPublisher()

    async def start(self):
        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            client = ThreadsGraphQLClient(redis_cache=self._cache)
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.threads.auth.bootstrap --query "{self.query}"',
            )
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("threads", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='threads' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("threads", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)

        try:
            async for item in self._crawl(client):
                yield item
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.threads.auth.bootstrap --query "{self.query}"',
            )
            sys.exit(1)
        except CheckpointRequiredError as exc:
            # _run() đã tắt tài khoản và gửi cảnh báo Telegram (xem comet_graphql_client.py) - chỉ cần
            # dừng lượt crawl ở đây.
            logger.error("checkpoint_required", error=str(exc))
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("threads", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='threads' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("threads", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, posts=self._post_count, query=self.query)

    async def _crawl(self, client: ThreadsGraphQLClient) -> AsyncIterator[ThreadsPostItem]:
        cursor: str | None = None
        page = 1
        empty_new_streak = 0

        while True:
            if cursor is None:
                response = await asyncio.to_thread(client.search, self.search_query, self.count)
            else:
                response = await asyncio.to_thread(client.search_next_page, self.search_query, cursor, self.count)

            posts = extract_response(response)

            new_posts = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id:
                    continue
                post_id = str(post_id)
                # add_if_new() (key TTL theo từng id, không phải set sadd() vĩnh viễn) coi lại cùng
                # post_id là mới sau SEEN_POSTS_TTL_SECONDS - like_count/reply_count/repost_count/
                # quote_count của một bài cứ thay đổi sau lần crawl đầu, nên khử trùng vĩnh viễn sẽ đóng
                # băng các số đó ở giá trị lần đầu thấy mãi mãi (cùng lý do như SEEN_POSTS_TTL_SECONDS của
                # TikTok).
                is_new = not self._cache or self._cache.add_if_new(
                    f"{SEEN_POSTS_KEY}:{post_id}", SEEN_POSTS_TTL_SECONDS
                )
                if not is_new:
                    continue
                published = await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"threads:{post_id}",
                    value={"platform": "threads", "keyword_id": self.keyword_id, **post},
                )
                if not published:
                    if self._cache:
                        # Chưa tới Kafka: bỏ dấu "đã thấy" để lượt crawl sau thử lại thay vì bỏ qua
                        # bài này suốt SEEN_POSTS_TTL_SECONDS.
                        self._cache.delete(f"{SEEN_POSTS_KEY}:{post_id}")
                    continue
                new_posts += 1
                self._post_count += 1
                log_crawled_post(
                    logger,
                    platform="threads",
                    post_id=post_id,
                    text=post.get("message"),
                    author=post.get("author_username") or post.get("author_name"),
                    url=post.get("url"),
                    likes=post.get("like_count"),
                    comments=post.get("reply_count"),
                    shares=post.get("repost_count"),
                )
                yield ThreadsPostItem(query=self.query, **post)

            if new_posts == 0:
                empty_new_streak += 1
            else:
                empty_new_streak = 0

            logger.info(
                "page_crawled",
                page=page,
                new_posts=new_posts,
                fetched=len(posts),
                empty_new_streak=empty_new_streak,
            )

            if empty_new_streak >= MAX_CONSECUTIVE_EMPTY_NEW_PAGES:
                logger.info(
                    "keyword_skipped_no_new_posts",
                    telegram=True,
                    query=self.query,
                    keyword_id=self.keyword_id,
                    pages=page,
                    empty_new_streak=empty_new_streak,
                    hint=f"{MAX_CONSECUTIVE_EMPTY_NEW_PAGES} consecutive pages with 0 new posts - stopping this keyword",
                )
                break

            page_info = find_page_info(response)
            if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1
