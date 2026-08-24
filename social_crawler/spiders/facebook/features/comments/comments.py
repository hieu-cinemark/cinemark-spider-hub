"""
Facebook comments spider that never opens a browser: fetches comments for a
post via curl_cffi, using the query cached by `bootstrap_comments()`
(social_crawler.spiders.facebook.auth.bootstrap).

Pass -a dedupe=false to disable cross-run dedupe (e.g. to re-fetch comments
already seen in a previous run) - it's on by default whenever Redis is
reachable, and silently falls back to in-run-only dedupe otherwise.

Run:
    scrapy crawl facebook_comments -a post_id="122197539992842674" -a max_pages=3
"""

from __future__ import annotations

import asyncio

import scrapy

from social_crawler.constants.facebook import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.facebook.auth.graphql_client import (
    FacebookGraphQLClient,
    RateLimitedError,
    SessionExpiredError,
)
from social_crawler.spiders.facebook.features.comments.extract import extract_comments, find_comments_page_info
from social_crawler.spiders.facebook.items import FacebookCommentItem

logger = get_logger(__name__)


class FacebookCommentsSpider(scrapy.Spider):
    name = "facebook_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        post_id: str | None = None,
        count: int = 10,
        max_pages: int = 1,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.post_id = post_id
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

    async def start(self):
        if not self.post_id:
            logger.error("missing_post_id", hint='scrapy crawl facebook_comments -a post_id="<a post id>"')
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            # Reuse this spider's own RedisCache/connection instead of
            # letting the client open a second, independent one internally.
            client = FacebookGraphQLClient(redis_cache=self._cache)
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc))
            return
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            return

        cursor: str | None = None
        page = 1
        total_count = 0

        try:
            while True:
                try:
                    if cursor is None:
                        response = await asyncio.to_thread(client.get_comments, self.post_id)
                    else:
                        response = await asyncio.to_thread(
                            client.get_comments_next_page, self.post_id, cursor, self.count
                        )
                except SessionExpiredError as exc:
                    logger.error(
                        "session_expired",
                        error=str(exc),
                        hint='python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url>"',
                    )
                    return
                except RateLimitedError as exc:
                    logger.error("rate_limited", telegram=True, error=str(exc))
                    return

                comments = extract_comments(response)
                new_count = 0
                for comment in comments:
                    comment_id = comment.get("comment_id")
                    if not comment_id:
                        logger.warning("comment_missing_id", post_id=self.post_id)
                        continue
                    # sadd()'s return value already answers "was this new" in one
                    # atomic round trip - no separate sismember check needed (and
                    # no race between a check and a later add).
                    if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
                        continue
                    new_count += 1
                    total_count += 1
                    await self._kafka.publish(
                        topic=RAW_COMMENTS_TOPIC,
                        key=f"facebook:{comment_id}",
                        value={"platform": "facebook", **comment},
                    )
                    yield FacebookCommentItem(post_id=self.post_id, **comment)

                logger.info("page_crawled", page=page, new_comments=new_count, fetched=len(comments))

                page_info = find_comments_page_info(response)
                if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                    break
                cursor = page_info.get("end_cursor")
                if not cursor:
                    break
                page += 1
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, post_id=self.post_id, comments=total_count, pages=page)
