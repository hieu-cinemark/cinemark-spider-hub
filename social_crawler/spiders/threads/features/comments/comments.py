"""
Threads replies spider that never opens a browser: fetches a post's
permalink HTML directly via curl_cffi and extracts the SSR-embedded first
page of replies - see features/comments/extract.py's module docstring for
why there's no pagination beyond that page yet.

Pass -a dedupe=false to disable cross-run dedupe (e.g. to re-fetch replies
already seen in a previous run) - it's on by default whenever Redis is
reachable, and silently falls back to in-run-only dedupe otherwise.

Run:
    scrapy crawl threads_comments -a post_id="3947461584399427661" \
        -a post_url="https://www.threads.com/@x/post/CODE"
"""

from __future__ import annotations

import asyncio

import scrapy

from social_crawler.constants.threads import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.threads.auth.graphql_client import SessionExpiredError, ThreadsGraphQLClient
from social_crawler.spiders.threads.features.comments.extract import extract_first_page_replies
from social_crawler.spiders.threads.items import ThreadsCommentItem

logger = get_logger(__name__)


class ThreadsCommentsSpider(scrapy.Spider):
    name = "threads_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(self, post_id: str | None = None, post_url: str | None = None, dedupe: str = "true", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_id = post_id
        self.post_url = post_url
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

    async def start(self):
        if not self.post_id or not self.post_url:
            logger.error(
                "missing_post_id_or_url",
                hint='scrapy crawl threads_comments -a post_id="<a post id>" -a post_url="<its permalink>"',
            )
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            client = ThreadsGraphQLClient(redis_cache=self._cache)
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc))
            return

        try:
            html = await asyncio.to_thread(client.get_comments_page_html, self.post_url)
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc))
            return

        replies = extract_first_page_replies(html)
        new_count = 0
        for reply in replies:
            reply_id = reply["reply_id"]
            # sadd()'s return value already answers "was this new" in one
            # atomic round trip - no separate sismember check needed (and
            # no race between a check and a later add).
            if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, reply_id) == 0:
                continue
            new_count += 1
            await self._kafka.publish(
                topic=RAW_COMMENTS_TOPIC,
                key=f"threads:{reply_id}",
                value={"platform": "threads", "post_id": self.post_id, **reply},
            )
            yield ThreadsCommentItem(post_id=self.post_id, **reply)

        await self._kafka.stop()
        logger.info(
            "crawl_finished",
            telegram=True,
            post_id=self.post_id,
            fetched=len(replies),
            new_replies=new_count,
            note="first page only (see extract.py docstring - Threads' own pagination query doesn't work via replay)",
        )
