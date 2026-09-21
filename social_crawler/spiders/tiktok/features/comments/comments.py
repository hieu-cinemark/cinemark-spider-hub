"""
TikTok comments spider via curl_cffi — same architecture as
tiktok_hashtag_search: mint a fresh synthetic guest identity, sign each
request locally (X-Gnarly + X-Dynosaur), paginate /api/comment/list/.

STATUS (2026-09-18): the Patchright browser path was replaced after a live
A/B showed:
  - Gnarly-only comment/list → HTTP 200 + empty body (even on identities
    that successfully fetch hashtag item_list).
  - Same request + local get_X_Dynosaur (signature/dynosaur.py) → real
    comments, stable across 5 fresh identities / 2 videos / pagination.

Hashtag item_list stays Dynosaur-free (sign_dynosaur=False). Comments
always pass sign_dynosaur=True via TikTokCommentClient.

No platform_accounts rotation and no sticky proxy pin — each attempt is a
new TikTokCommentClient(synthetic=True), matching hashtag_search. Empty
HTTP bodies raise TikTokBlockedError and retry with a fresh identity;
proxy-pool exhaustion exits PROXY_EXHAUSTED_EXIT_CODE so the consumer
can requeue.

Run:
    scrapy crawl tiktok_comments -a video_id="7670822924022074645" \
        -a video_url="https://www.tiktok.com/@user/video/7670822924022074645"
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator

import scrapy

from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE, SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.error_alerts import note_transient_error
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokCommentClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.comments.extract import extract_comments
from social_crawler.spiders.tiktok.items import TikTokCommentItem

logger = get_logger(__name__)

# Same rationale as hashtag_search: each attempt is an independent synthetic
# draw (fresh identity + proxy lease), not a platform_accounts rotation.
MAX_ACCOUNT_ATTEMPTS = 8
# Comments per page — matches the live Dynosaur probe that returned 20.
DEFAULT_COUNT = 20


class TikTokCommentsSpider(scrapy.Spider):
    name = "tiktok_comments"

    # Never goes through Scrapy's downloader — curl_cffi signs directly.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        video_id: str | None = None,
        video_url: str | None = None,
        max_pages: int = 5,
        count: int = DEFAULT_COUNT,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.video_id = video_id
        self.video_url = video_url
        self.max_pages = int(max_pages)
        self.count = int(count)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()
        self._new_count = 0
        self._pages_fetched = 0

    async def start(self):
        if not self.video_id or not self.video_url:
            logger.error(
                "missing_video_id_or_url",
                hint='scrapy crawl tiktok_comments -a video_id="<a video id>" -a video_url="<its permalink>"',
            )
            return

        await self._kafka.start()
        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            try:
                for attempt in range(1, MAX_ACCOUNT_ATTEMPTS + 1):
                    try:
                        async for item in self._crawl_with_fresh_identity():
                            yield item
                        break
                    except TikTokBlockedError as exc:
                        if attempt < MAX_ACCOUNT_ATTEMPTS:
                            logger.warning(
                                "blocked_retrying_with_different_account",
                                attempt=attempt,
                                max_attempts=MAX_ACCOUNT_ATTEMPTS,
                                video_id=self.video_id,
                                error=str(exc),
                            )
                            continue
                        logger.error(
                            "blocked",
                            telegram=True,
                            video_id=self.video_id,
                            error=str(exc),
                            hint="every synthetic identity got empty comment/list",
                        )
                        sys.exit(1)
                    except pool.ProxyPoolExhaustedError as exc:
                        logger.error("tiktok_proxy_pool_exhausted", telegram=True, error=str(exc))
                        await self._kafka.stop()
                        sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
            except TikTokRateLimitedError as exc:
                logger.error("rate_limited", telegram=True, error=str(exc), video_id=self.video_id)
                note_transient_error("tiktok", "rate_limited", self._cache)
            except TikTokNetworkError as exc:
                logger.error(
                    "network_error",
                    telegram=True,
                    error=str(exc),
                    video_id=self.video_id,
                    hint="proxy/network problem on synthetic comment client - not a Dynosaur issue",
                )
                note_transient_error("tiktok", "network_error", self._cache)
        finally:
            await self._kafka.stop()

        logger.info(
            "crawl_finished",
            telegram=True,
            video_id=self.video_id,
            pages=self._pages_fetched,
            new_comments=self._new_count,
        )

    async def _crawl_with_fresh_identity(self) -> AsyncIterator[TikTokCommentItem]:
        """One attempt: mint synthetic guest, paginate comment/list until
        max_pages or has_more=false. Raises TikTokBlockedError on empty
        body so start() can remint."""
        client = await asyncio.to_thread(TikTokCommentClient, redis_cache=self._cache, synthetic=True)
        logger.info(
            "tiktok_comments_attempt",
            device_id=client._device_id,
            video_id=self.video_id,
            synthetic=True,
        )
        await asyncio.to_thread(client.warm_session)

        cursor = 0
        self._pages_fetched = 0
        for page_idx in range(1, self.max_pages + 1):
            data = await asyncio.to_thread(
                client.list_comments,
                self.video_id,
                cursor=cursor,
                count=self.count,
                video_url=self.video_url,
            )
            self._pages_fetched = page_idx
            comments = extract_comments(data)
            status_code = data.get("status_code")
            logger.info(
                "comment_page_fetched",
                page=page_idx,
                cursor=cursor,
                comments=len(comments),
                has_more=data.get("has_more"),
                status_code=status_code,
            )

            # Non-zero status with no comments on the first page usually
            # means a soft block / region filter, not a truly empty video.
            if page_idx == 1 and not comments and status_code not in (0, None):
                raise TikTokBlockedError(
                    f"comment/list status_code={status_code} with zero comments on page 1"
                )

            for comment in comments:
                comment_id = comment["comment_id"]
                if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
                    continue
                self._new_count += 1
                await self._kafka.publish(
                    topic=RAW_COMMENTS_TOPIC,
                    key=f"tiktok:{comment_id}",
                    value={
                        "platform": "tiktok",
                        "post_id": self.video_id,
                        "video_id": self.video_id,
                        **comment,
                    },
                )
                yield TikTokCommentItem(video_id=self.video_id, **comment)

            # status_code 0 + empty comments on page 1 is a real zero-comment
            # video (or filtered), not a block — stop cleanly.
            if page_idx == 1 and not comments and not data.get("has_more"):
                break

            if not data.get("has_more"):
                break
            next_cursor = data.get("cursor")
            if next_cursor is None or str(next_cursor) == str(cursor):
                break
            try:
                cursor = int(next_cursor)
            except (TypeError, ValueError):
                break
