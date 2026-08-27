"""
TikTok hashtag-search spider that never opens a browser: calls
/api/challenge/item_list/ directly through curl_cffi (impersonating a
Chrome TLS fingerprint), signing every request locally with a freshly
computed X-Gnarly (see signature/gnarly.py). Mirrors
social_crawler.spiders.threads.features.search.search - see that module's
docstring and client.py's module docstring for the full rationale.

Unlike Facebook/Threads, pagination here is TikTok's own cursor/hasMore
pair (not GraphQL page_info), and there's no query string - a hashtag name
resolves once to a numeric challenge_id via resolve_hashtag(), then every
page after that is fetched by that id.

Run:
    scrapy crawl tiktok_hashtag_search -a hashtag="holinhtrangsi"

Pass -a dedupe=false to disable cross-run dedupe - on by default whenever
Redis is reachable, silently falls back to in-run-only dedupe otherwise.
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

import scrapy

from social_crawler.constants.tiktok import SEEN_POSTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokHashtagClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.hashtag_search.extract import extract_response
from social_crawler.spiders.tiktok.items import TikTokVideoItem

logger = get_logger(__name__)


class TikTokHashtagSearchSpider(scrapy.Spider):
    name = "tiktok_hashtag_search"

    # This spider never goes through Scrapy's downloader (it calls
    # curl_cffi directly to impersonate a real Chrome TLS fingerprint), so
    # robots.txt and downloader middlewares don't apply here.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        hashtag: str = "test",
        keyword_id: str | None = None,
        count: int = 30,
        max_pages: int = 100,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.hashtag = hashtag
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
            client = TikTokHashtagClient(redis_cache=self._cache)
            challenge_id = await asyncio.to_thread(client.resolve_hashtag, self.hashtag)
        except TikTokBlockedError as exc:
            logger.error("blocked", telegram=True, error=str(exc))
            return
        except TikTokRateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            return
        except TikTokNetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='tiktok' - "
                "this is a proxy/network problem, not a stale identity, re-capturing cookie/device_id/odin_id won't help.",
            )
            return

        if not challenge_id:
            logger.error("hashtag_not_found", telegram=True, hashtag=self.hashtag)
            return

        try:
            async for item in self._crawl(client, challenge_id):
                yield item
        except TikTokBlockedError as exc:
            logger.error("blocked", telegram=True, error=str(exc))
            return
        except TikTokRateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            return
        except TikTokNetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='tiktok' - "
                "this is a proxy/network problem, not a stale identity, re-capturing cookie/device_id/odin_id won't help.",
            )
            return
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, posts=self._post_count, hashtag=self.hashtag)

    async def _crawl(self, client: TikTokHashtagClient, challenge_id: str) -> AsyncIterator[TikTokVideoItem]:
        cursor = 0
        page = 1

        while True:
            response = await asyncio.to_thread(client.search_hashtag, challenge_id, cursor, self.count)
            videos = extract_response(response)

            new_posts = 0
            for video in videos:
                video_id = video.get("video_id")
                if not video_id:
                    continue
                video_id = str(video_id)
                # sadd()'s return value already answers "was this new" in
                # one atomic round trip - no separate sismember check
                # needed (and no race between a check and a later add).
                if self._cache and self._cache.sadd(SEEN_POSTS_KEY, video_id) == 0:
                    continue
                new_posts += 1
                self._post_count += 1
                await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"tiktok:{video_id}",
                    value={"platform": "tiktok", "keyword_id": self.keyword_id, **video},
                )
                yield TikTokVideoItem(hashtag=self.hashtag, **video)

            logger.info("page_crawled", page=page, new_posts=new_posts, fetched=len(videos))

            if page >= self.max_pages or not response.get("hasMore"):
                break
            next_cursor = response.get("cursor")
            if next_cursor is None or int(next_cursor) == cursor:
                break
            cursor = int(next_cursor)
            page += 1
