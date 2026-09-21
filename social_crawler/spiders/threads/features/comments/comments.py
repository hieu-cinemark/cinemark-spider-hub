"""
Threads replies spider that never opens a browser: GET
/api/v1/text_feed/<post_id>/replies/ through curl_cffi with the same
cookie session search already bootstraps (ds_user_id/sessionid/csrftoken).

The GraphQL query a real logged-in SPA fires for this
(BarcelonaPostPageDirectQuery /
xdt_api__v1__text_feed__media_id__replies__connection) is a Relay wrapper
around that REST path. Replaying the GraphQL doc itself comes back
direct_replies: null; a cold permalink browser load never even fires it
(see browser_capture.py). The REST GET is the surface that actually
returns reply_threads + paging_tokens.downwards, same as Facebook comments
using GraphQL replay rather than a live page.

Guest (no cookies) 403s with login_required - probed live. Session cookies
from `python -m social_crawler.spiders.threads.auth.bootstrap` are enough;
no separate comments-query cache.

Run:
    scrapy crawl threads_comments -a post_id="3947461584399427661"

Pass -a dedupe=false to disable cross-run dedupe (e.g. to re-fetch replies
already seen in a previous run) - it's on by default whenever Redis is
reachable, and silently falls back to in-run-only dedupe otherwise.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import scrapy

from social_crawler.constants.threads import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.error_alerts import note_transient_error
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.threads.auth.graphql_client import (
    CheckpointRequiredError,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    ThreadsGraphQLClient,
)
from social_crawler.spiders.threads.features.comments.extract import (
    apply_feed_parent,
    extract_replies_from_text_feed,
    find_text_feed_page_info,
    replies_needing_expand,
)
from social_crawler.spiders.threads.items import ThreadsCommentItem

logger = get_logger(__name__)

# Root feed uses spider max_pages. Each nested reply we expand is its own
# text_feed/{reply_id}/replies/ walk - live 200-direct-reply posts still only
# expose ~66 ranked top-level threads on the root feed (count=100, no
# cursor); expanding truncated parents recovered the inlined gap, not the
# ranked-out remainder. Nested caps are a safety ceiling (same idea as
# Facebook's MAX_REPLY_PAGES), not the expected yield - raise when a
# viral thread still has declared reply_count >> collected children.
MAX_NESTED_FEEDS = 200
MAX_NESTED_PAGES = 20


class ThreadsCommentsSpider(scrapy.Spider):
    name = "threads_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        post_id: str | None = None,
        post_url: str | None = None,
        count: int = 500,
        max_pages: int = 80,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.post_id = post_id
        self.post_url = post_url
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

    async def start(self):
        if not self.post_id:
            logger.error(
                "missing_post_id",
                hint='scrapy crawl threads_comments -a post_id="<a post id>"',
            )
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            client = ThreadsGraphQLClient(redis_cache=self._cache)
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint='python -m social_crawler.spiders.threads.auth.bootstrap --query "test"',
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

        total_count = 0
        root_pages = 0
        nested_feeds = 0
        child_counts: dict[str, int] = {}
        seen_this_run: set[str] = set()
        feeds: deque[tuple[str, str | None]] = deque([(self.post_id, None)])
        seen_feeds = {self.post_id}

        try:
            while feeds:
                feed_id, default_parent = feeds.popleft()
                is_root = feed_id == self.post_id
                max_pages = self.max_pages if is_root else MAX_NESTED_PAGES
                cursor: str | None = None
                page = 1
                if not is_root:
                    nested_feeds += 1
                    logger.info("nested_feed_started", feed_id=feed_id, parent_reply_id=default_parent)

                while True:
                    try:
                        response: dict[str, Any] = await asyncio.to_thread(
                            client.get_text_feed_replies, feed_id, self.count, cursor
                        )
                    except SessionExpiredError as exc:
                        logger.error(
                            "session_expired",
                            error=str(exc),
                            hint='python -m social_crawler.spiders.threads.auth.bootstrap --query "test"',
                        )
                        return
                    except CheckpointRequiredError as exc:
                        logger.error("checkpoint_required", error=str(exc))
                        return
                    except RateLimitedError as exc:
                        logger.error("rate_limited", telegram=True, error=str(exc))
                        note_transient_error("threads", "rate_limited", self._cache)
                        # Root abort: no further pages help without a live session
                        # budget. Nested soft-fail: keep walking other parents so
                        # one throttled expand doesn't discard the whole queue.
                        if is_root:
                            return
                        logger.warning("nested_feed_aborted_rate_limited", feed_id=feed_id)
                        break
                    except NetworkError as exc:
                        logger.error(
                            "network_error",
                            telegram=True,
                            error=str(exc),
                            hint="check connectivity to the platform_proxies row for platform='threads' - "
                            "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
                        )
                        note_transient_error("threads", "network_error", self._cache)
                        if is_root:
                            return
                        logger.warning("nested_feed_aborted_network_error", feed_id=feed_id)
                        break

                    replies = apply_feed_parent(
                        extract_replies_from_text_feed(response),
                        feed_id=feed_id,
                        root_post_id=self.post_id,
                    )
                    new_count = 0
                    for reply in replies:
                        reply_id = reply.get("reply_id")
                        if not reply_id or reply_id in seen_this_run:
                            continue
                        seen_this_run.add(reply_id)
                        parent_id = reply.get("parent_reply_id")
                        if parent_id:
                            child_counts[parent_id] = child_counts.get(parent_id, 0) + 1
                        if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, reply_id) == 0:
                            continue
                        new_count += 1
                        total_count += 1
                        await self._kafka.publish(
                            topic=RAW_COMMENTS_TOPIC,
                            key=f"threads:{reply_id}",
                            value={"platform": "threads", "post_id": self.post_id, **reply},
                        )
                        yield ThreadsCommentItem(post_id=self.post_id, **reply)

                    for expand_id in replies_needing_expand(replies, child_counts):
                        if expand_id in seen_feeds or nested_feeds + len(feeds) >= MAX_NESTED_FEEDS:
                            continue
                        seen_feeds.add(expand_id)
                        feeds.append((expand_id, expand_id))

                    page_info = find_text_feed_page_info(response)
                    logger.info(
                        "page_crawled",
                        page=page,
                        new_replies=new_count,
                        fetched=len(replies),
                        post_id=self.post_id,
                        feed_id=feed_id,
                        has_next_page=page_info.get("has_next_page"),
                        has_cursor=bool(page_info.get("end_cursor")),
                    )
                    if is_root:
                        root_pages = page
                    next_cursor = page_info.get("end_cursor")
                    if page >= max_pages or not page_info.get("has_next_page"):
                        break
                    if not next_cursor or next_cursor == cursor:
                        break
                    cursor = next_cursor
                    page += 1
        finally:
            await self._kafka.stop()

        logger.info(
            "crawl_finished",
            telegram=True,
            post_id=self.post_id,
            pages=root_pages,
            nested_feeds=nested_feeds,
            new_replies=total_count,
        )
