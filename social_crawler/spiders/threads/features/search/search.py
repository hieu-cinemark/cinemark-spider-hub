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
    DEFAULT_MAX_PAGES,
    MAX_CONSECUTIVE_EMPTY_NEW_PAGES,
    MAX_CONSECUTIVE_SEEN_ONLY_PAGES,
    SEEN_POSTS_KEY,
    SEEN_POSTS_TTL_SECONDS,
)
from social_crawler.core.logger import get_logger
from social_crawler.runtime import pool
from social_crawler.spiders.common.comet_graphql_client import RequestBudgetExceeded
from social_crawler.spiders.common.error_alerts import note_transient_error
from social_crawler.spiders.common.keyword_filter import OffTopicGuard, save_yield
from social_crawler.spiders.common.post_log import log_crawled_post
from social_crawler.spiders.common.search_query import build_search_query
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
        max_pages: int = DEFAULT_MAX_PAGES,  # 50 -> 100 (2026-10-16) -> 130 (2026-10-25), xem constants/threads.py
        dedupe: str = "true",
        recent: str = "false",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        # Bỏ bài không chứa từ khoá + dừng sớm khi tìm kiếm trả toàn bài lạc đề - xem spiders/keyword_filter.py.
        self._off_topic = OffTopicGuard(query, kwargs.pop("match_terms", None))
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
        # Tab "Gần đây" (mới nhất trước) thay vì "Hàng đầu" - lượt cào sau trong ngày dùng cái này (cinemark-api scheduler).
        self.recent = str(recent).lower() in ("true", "1", "yes")
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

        # Trần request mỗi giờ của tài khoản Threads (services/search_budget.py): chạm trần thì dừng gọn giữa job.
        client.enforce_hourly_cap = True
        try:
            async for item in self._crawl(client):
                yield item
        except RequestBudgetExceeded as exc:
            logger.info("request_budget_exhausted", error=str(exc))
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

        save_yield(self._cache, self.keyword_id, self._off_topic)

        logger.info(
            "crawl_finished",
            telegram=True,
            posts=self._post_count,
            query=self.query,
            off_topic_dropped=self._off_topic.dropped,
        )

    async def _crawl(self, client: ThreadsGraphQLClient) -> AsyncIterator[ThreadsPostItem]:
        cursor: str | None = None
        page = 1
        empty_new_streak = 0
        seen_only_streak = 0

        while True:
            if cursor is None:
                response = await asyncio.to_thread(client.search, self.search_query, self.count, None, self.recent)
            else:
                response = await asyncio.to_thread(
                    client.search_next_page, self.search_query, cursor, self.count, self.recent
                )

            posts = extract_response(response)

            new_posts = 0
            page_matched = 0
            page_total = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id:
                    continue
                post_id = str(post_id)
                page_total += 1
                if not self._off_topic.keep(post.get("message")):
                    continue
                page_matched += 1
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

            # Không bài mới: trang có bài đúng chủ đề (chỉ là đã thấy) -> đào tiếp, tính vào seen_only_streak (mốc cao);
            # trang không có bài đúng chủ đề nào -> empty_new_streak (mốc 20) như trước.
            if new_posts:
                empty_new_streak = seen_only_streak = 0
            # Xếp mới nhất trước: gặp bài đã thấy là đã tới phần cũ -> dừng như cũ.
            elif page_matched and not self.recent:
                seen_only_streak += 1
            else:
                empty_new_streak += 1

            logger.info(
                "page_crawled",
                page=page,
                new_posts=new_posts,
                fetched=len(posts),
                empty_new_streak=empty_new_streak,
                seen_only_streak=seen_only_streak,
            )

            if self._off_topic.end_page(page_matched, page_total):
                logger.info(
                    "keyword_stopped_off_topic",
                    query=self.query,
                    keyword_id=self.keyword_id,
                    pages=page,
                    matched=self._off_topic.matched,
                    dropped=self._off_topic.dropped,
                    hint="search keeps returning posts that don't mention the keyword - stopping early",
                )
                break

            if (
                empty_new_streak >= MAX_CONSECUTIVE_EMPTY_NEW_PAGES
                or seen_only_streak >= MAX_CONSECUTIVE_SEEN_ONLY_PAGES
            ):
                logger.info(
                    "keyword_skipped_no_new_posts",
                    query=self.query,
                    keyword_id=self.keyword_id,
                    pages=page,
                    empty_new_streak=empty_new_streak,
                    seen_only_streak=seen_only_streak,
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
