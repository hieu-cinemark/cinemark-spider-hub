"""
Spider bài của một kênh Threads (@username) - từ 20/10/2026, cho kênh trong danh sách booking mà D1 chưa có bài nào
("Nghiệm thu booking" của cinemark: cinemark-api app/services/priority_channels.py xếp type=channel_posts lên
crawl_requests, crawl_request_consumer.py chạy spider này trong vòng lặp từng-cái-một của Threads).

Luồng: fetch_profile(username) lấy pk (userID - query tab hồ sơ không nhận username) -> phân trang fetch_profile_posts
(tab "Threads" của trang hồ sơ, mới nhất trước) tới khi hết max_pages, hết trang, hoặc cả trang đều cũ hơn `days` ngày.
Chỉ giữ bài nhắc tới phim đang nghiệm thu (ChannelMovieMatcher - hashtag trùng hashtag của phim / chứa tên phim / chứa
từ khoá; cinemark-api gửi kèm keyword_id + các điều kiện này trong request). Bài được publish lên RAW_POSTS_TOPIC ĐÚNG
dạng item của threads_search ({"platform": "threads", "keyword_id": ..., **extract_post(...)}, thêm source) nên ingest,
khử trùng, chấm liên quan và lượt comment phía sau chạy y như bài tìm kiếm; khử trùng dùng chung SEEN_POSTS_KEY.

Lỗi: session hết hạn/checkpoint/rate limit/mạng/response lạ (doc_id hết hạn) -> thoát mã khác 0 để consumer ghi failed
(lỗi proxy: PROXY_EXHAUSTED_EXIT_CODE để consumer xếp lại request) - cùng quy ước với threads_search.

Chạy:
    scrapy crawl threads_channel_posts -a username="minnnsb" -a keyword_id=<id> -a keyword="..." \
        -a match_terms='["Tên Phim"]' -a match_hashtags='["tenphim"]' -a max_pages=3 -a days=60
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator
from typing import ClassVar

import scrapy

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.threads import (
    CHANNEL_POSTS_DAYS,
    CHANNEL_POSTS_MAX_PAGES,
    SEEN_POSTS_KEY,
    SEEN_POSTS_TTL_SECONDS,
)
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.error_alerts import note_transient_error
from social_crawler.spiders.keyword_filter import ChannelMovieMatcher
from social_crawler.spiders.post_log import log_crawled_post
from social_crawler.spiders.threads.auth.graphql_client import (
    CheckpointRequiredError,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    ThreadsGraphQLClient,
)
from social_crawler.spiders.threads.features.channel_posts.extract import ChannelPostsQueryError, extract_channel_page
from social_crawler.spiders.threads.features.profiles.extract import ProfileQueryError
from social_crawler.spiders.threads.items import ThreadsPostItem

logger = get_logger(__name__)

_NETWORK_HINT = (
    "check connectivity to the platform_proxies row for platform='threads' - "
    "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help."
)
# Giá trị trường source trong message Kafka (đi vào raw_json của bài) - để biết bài đến từ lượt cào kênh, không phải tìm
# kiếm. Ingest chỉ xử lý riêng source="page", giá trị này đi đường thường.
SOURCE = "threads_channel"


class ThreadsChannelPostsSpider(scrapy.Spider):
    name = "threads_channel_posts"

    # Như threads_search: không đi qua downloader của Scrapy (curl_cffi gọi thẳng).
    custom_settings: ClassVar[dict] = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        username: str = "",
        keyword_id: str | None = None,
        keyword: str | None = None,
        match_terms: str | None = None,
        match_hashtags: str | None = None,
        max_pages: int = CHANNEL_POSTS_MAX_PAGES,
        days: int = CHANNEL_POSTS_DAYS,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.username = username.strip().lstrip("@").split("/")[0]
        # Từ khoá Threads của phim (cinemark-api chọn) - ingest tra phim/từ khoá theo id này, như bài tìm kiếm.
        self.keyword_id = keyword_id
        self._matcher = ChannelMovieMatcher(keyword, match_terms, match_hashtags)
        self.max_pages = max(1, int(max_pages))
        self.days = max(1, int(days))
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._post_count = 0
        self._kafka = KafkaPublisher()

    async def start(self):
        if not self.username:
            logger.error("missing_username", hint='scrapy crawl threads_channel_posts -a username="<handle>"')
            sys.exit(1)

        await self._kafka.start()
        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            client = ThreadsGraphQLClient(redis_cache=self._cache)
            async for item in self._crawl(client):
                yield item
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                username=self.username,
                hint='python -m social_crawler.spiders.threads.auth.bootstrap --query "test"',
            )
            sys.exit(1)
        except CheckpointRequiredError as exc:
            # _run() đã tắt tài khoản và gửi cảnh báo Telegram (comet_graphql_client.py).
            logger.error("checkpoint_required", error=str(exc), username=self.username)
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc), username=self.username)
            note_transient_error("threads", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error("network_error", telegram=True, error=str(exc), username=self.username, hint=_NETWORK_HINT)
            note_transient_error("threads", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)
        except (ProfileQueryError, ChannelPostsQueryError) as exc:
            # Thường là PROFILE_DOC_ID / CHANNEL_POSTS_DOC_ID đã hết hạn - bắt lại từ DevTools (constants/threads.py).
            logger.error(
                "threads_channel_posts_unexpected_response", telegram=True, error=str(exc), username=self.username
            )
            sys.exit(1)
        finally:
            await self._kafka.stop()

        logger.info(
            "crawl_finished",
            telegram=True,
            posts=self._post_count,
            username=self.username,
            keyword_id=self.keyword_id,
            matched=self._matcher.matched,
            off_topic_dropped=self._matcher.dropped,
        )

    async def _crawl(self, client: ThreadsGraphQLClient) -> AsyncIterator[ThreadsPostItem]:
        profile = await asyncio.to_thread(client.fetch_profile, self.username)
        user_id = (profile or {}).get("pk")
        if not user_id:
            # Kênh không tồn tại / đổi tên: không phải lỗi của lượt cào - xong với 0 bài.
            logger.warning("threads_channel_not_found", username=self.username, found=profile is not None)
            return

        cutoff = time.time() - self.days * 86_400
        cursor: str | None = None
        for page in range(1, self.max_pages + 1):
            response = await asyncio.to_thread(client.fetch_profile_posts, user_id, cursor)
            posts, page_info = extract_channel_page(response, user_id)

            new_posts = in_window = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id:
                    continue
                post_id = str(post_id)
                timestamp = post.get("timestamp")
                if isinstance(timestamp, (int, float)) and timestamp < cutoff:
                    continue
                in_window += 1
                if not self._matcher.keep(post.get("message"), post.get("topic")):
                    continue
                # Cùng khử trùng với threads_search (key TTL theo id - xem comment ở đó).
                is_new = not self._cache or self._cache.add_if_new(
                    f"{SEEN_POSTS_KEY}:{post_id}", SEEN_POSTS_TTL_SECONDS
                )
                if not is_new:
                    continue
                published = await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"threads:{post_id}",
                    value={"platform": "threads", "keyword_id": self.keyword_id, "source": SOURCE, **post},
                )
                if not published:
                    if self._cache:
                        # Chưa tới Kafka: bỏ dấu "đã thấy" để lượt sau thử lại.
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
                yield ThreadsPostItem(query=f"@{self.username}", **post)

            logger.info(
                "page_crawled",
                page=page,
                username=self.username,
                fetched=len(posts),
                in_window=in_window,
                new_posts=new_posts,
            )
            # Mới nhất trước: cả trang đã cũ hơn mốc ngày thì các trang sau còn cũ hơn. Không dừng ở bài cũ ĐẦU TIÊN vì bài
            # ghim (có thể rất cũ) đứng đầu trang 1.
            if posts and not in_window:
                logger.info("threads_channel_reached_cutoff", username=self.username, page=page, days=self.days)
                break
            cursor = (page_info or {}).get("end_cursor")
            if not (page_info or {}).get("has_next_page") or not cursor:
                break
