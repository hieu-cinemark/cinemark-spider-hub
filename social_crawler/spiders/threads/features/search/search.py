"""
Threads search spider that never opens a browser: calls the GraphQL
endpoint directly through curl_cffi (impersonating a Chrome TLS
fingerprint), using the token cached by
`social_crawler.spiders.threads.auth.bootstrap`. Mirrors
social_crawler.spiders.facebook.features.search.search - see that module's
docstring for the full rationale behind max_pages/dedupe. Date-range
sweeping isn't implemented here (Threads search has no "Date posted" filter
in its UI the way Facebook's does), so this is closer to Facebook's plain
unfiltered crawl loop.

Run:
    scrapy crawl threads_search -a query="keyword"

Pass -a dedupe=false to disable cross-run dedupe - on by default whenever
Redis is reachable, silently falls back to in-run-only dedupe otherwise.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import scrapy

from social_crawler.constants.threads import SEEN_POSTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.error_alerts import note_transient_error
from social_crawler.services.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
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

    # This spider never goes through Scrapy's downloader (it calls curl_cffi
    # directly to impersonate a real Chrome TLS fingerprint), so robots.txt
    # and downloader middlewares don't apply here.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        query: str = "test",
        keyword_id: str | None = None,
        count: int = 10,
        max_pages: int = 100,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        # Opaque to this spider - just threaded through to Kafka on every
        # published post, same as facebook_search's keyword_id.
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
            return
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("threads", "rate_limited", self._cache)
            return
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='threads' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("threads", "network_error", self._cache)
            return

        try:
            async for item in self._crawl(client):
                yield item
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.threads.auth.bootstrap --query "{self.query}"',
            )
            return
        except CheckpointRequiredError as exc:
            # _run() already disabled the account and sent the Telegram
            # alert (see comet_graphql_client.py) - just stop the crawl here.
            logger.error("checkpoint_required", error=str(exc))
            return
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("threads", "rate_limited", self._cache)
            return
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='threads' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("threads", "network_error", self._cache)
            return
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, posts=self._post_count, query=self.query)

    async def _crawl(self, client: ThreadsGraphQLClient) -> AsyncIterator[ThreadsPostItem]:
        cursor: str | None = None
        page = 1

        while True:
            if cursor is None:
                response = await asyncio.to_thread(client.search, self.query, self.count)
            else:
                response = await asyncio.to_thread(client.search_next_page, self.query, cursor, self.count)

            posts = extract_response(response)

            new_posts = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id:
                    continue
                post_id = str(post_id)
                # sadd()'s return value already answers "was this new" in one
                # atomic round trip - no separate sismember check needed (and
                # no race between a check and a later add).
                if self._cache and self._cache.sadd(SEEN_POSTS_KEY, post_id) == 0:
                    continue
                new_posts += 1
                self._post_count += 1
                await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"threads:{post_id}",
                    value={"platform": "threads", "keyword_id": self.keyword_id, **post},
                )
                yield ThreadsPostItem(query=self.query, **post)

            logger.info("page_crawled", page=page, new_posts=new_posts, fetched=len(posts))

            page_info = find_page_info(response)
            if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1
