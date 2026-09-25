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

Two completeness gaps closed 2026-09-25 (user-reported "not enough
comments"):
  - max_pages default was 5 * count=20 = a hard 100-comment ceiling, hit
    regardless of has_more, and crawl_request_consumer.py's subprocess
    call never overrides either - every production run used to hit
    exactly this ceiling on any video with >100 top-level comments.
    Raised to MAX_TOP_LEVEL_PAGES; still bounded (not "until has_more is
    false" unconditionally) so one viral video can't dominate the shared
    proxy/account pool.
  - Replies were never fetched at all - client.py's list_replies() existed
    and was already confirmed working live (2026-09-18, see its own
    docstring) but nothing here ever called it. Every top-level comment
    extract_comment finds carries reply_count (raw reply_comment_total);
    now paginated per comment via _fetch_replies below, same has_more/
    cursor contract as top-level, capped at MAX_REPLY_PAGES_PER_COMMENT.

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
# Raised from 5 (2026-09-25) - that was a silent 100-comment ceiling hit on
# every video with more top-level comments than that, regardless of
# has_more, since crawl_request_consumer.py never overrides this default.
# Still bounded, not "loop until has_more is false" unconditionally - a
# viral video with tens of thousands of comments must not tie up this
# platform's small shared proxy/account pool indefinitely.
MAX_TOP_LEVEL_PAGES = 50
# Replies per commented-on top-level comment - same reasoning as above,
# scoped smaller since this multiplies by however many top-level comments
# actually have replies (reply_count > 0), not a flat per-video cost.
MAX_REPLY_PAGES_PER_COMMENT = 10


class TikTokCommentsSpider(scrapy.Spider):
    name = "tiktok_comments"

    # Never goes through Scrapy's downloader — curl_cffi signs directly.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        video_id: str | None = None,
        video_url: str | None = None,
        max_pages: int = MAX_TOP_LEVEL_PAGES,
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
                # Every retry failed to even get an HTTP response back (see
                # TikTokNetworkError's own docstring) - a proxy/connectivity
                # problem, not a dead identity, so this must exit the same
                # way pool.ProxyPoolExhaustedError does (PROXY_EXHAUSTED_
                # EXIT_CODE) rather than falling through to the normal
                # "crawl_finished" completion below. Confirmed happening for
                # real (2026-09-21): before this, a bad proxy mid-mint made
                # crawl_request_consumer.py commit the job as "done" with
                # zero comments fetched instead of requeuing it - the exact
                # "quiet success" this exit code exists to prevent (see
                # pool.ProxyPoolExhaustedError's own docstring).
                await self._kafka.stop()
                sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
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
                async for item in self._publish_comment(comment):
                    yield item
                if comment.get("reply_count"):
                    async for item in self._fetch_replies(client, comment):
                        yield item

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

    async def _publish_comment(self, comment: dict) -> AsyncIterator[TikTokCommentItem]:
        comment_id = comment["comment_id"]
        if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
            return
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

    async def _fetch_replies(self, client: TikTokCommentClient, parent: dict) -> AsyncIterator[TikTokCommentItem]:
        """Paginates every reply under one top-level comment - same
        cursor/has_more contract as the top-level loop above, just scoped
        to MAX_REPLY_PAGES_PER_COMMENT instead of MAX_TOP_LEVEL_PAGES.
        Left uncaught on a TikTokBlockedError, same as a top-level page -
        _crawl_with_fresh_identity's caller already remints a fresh
        identity and retries the whole video on that."""
        parent_id = parent["comment_id"]
        cursor = 0
        for _ in range(MAX_REPLY_PAGES_PER_COMMENT):
            data = await asyncio.to_thread(
                client.list_replies,
                comment_id=parent_id,
                item_id=self.video_id,
                cursor=cursor,
                count=self.count,
                video_url=self.video_url,
            )
            replies = extract_comments(data, parent_comment_id=parent_id)
            logger.info(
                "reply_page_fetched",
                parent_comment_id=parent_id,
                cursor=cursor,
                replies=len(replies),
                has_more=data.get("has_more"),
            )
            for reply in replies:
                async for item in self._publish_comment(reply):
                    yield item

            if not data.get("has_more"):
                break
            next_cursor = data.get("cursor")
            if next_cursor is None or str(next_cursor) == str(cursor):
                break
            try:
                cursor = int(next_cursor)
            except (TypeError, ValueError):
                break
