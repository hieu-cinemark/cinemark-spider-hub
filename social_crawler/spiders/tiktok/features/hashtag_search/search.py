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

Two things this spider does on its own, beyond just crawling the one
hashtag it was asked for:

  - Account-retry on TikTokBlockedError: a stale/lost-trust identity is a
    property of the *account* this run happened to rotate to (see
    client.py's own docstring), not of the hashtag or this crawl in
    general - so instead of giving up outright, it re-resolves and re-runs
    once against whatever account rotation picks next. Already-published
    videos aren't re-published on the retry (SEEN_POSTS_KEY dedupe blocks
    them same as any other repeat), so the only cost of a spurious retry is
    one extra resolve_hashtag round trip. Rate-limit/network errors don't
    get this treatment - both are explicitly documented as not being an
    identity problem, so rotating accounts wouldn't help either.

  - BFS hashtag expansion: every related hashtag already tallied for the
    "related_hashtags_found" log (see extract.top_related_hashtags) also
    gets queued as its own follow-up crawl_request over Kafka - see
    constants/tiktok.py's BFS_MAX_* for the depth/fanout/page caps, and
    SEEN_HASHTAGS_KEY for the cross-run dedupe that stops it from ever
    re-queuing the same hashtag twice. Deliberately queued with no
    keyword_id (see that publish call's own comment for why).
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import AsyncIterator

import scrapy

from social_crawler.constants.tiktok import (
    BFS_MAX_DEPTH,
    BFS_MAX_HASHTAGS_PER_RUN,
    BFS_MAX_PAGES,
    SEEN_HASHTAGS_KEY,
    SEEN_POSTS_KEY,
)
from social_crawler.logger import get_logger
from social_crawler.services.error_alerts import note_transient_error
from social_crawler.services.kafka import CRAWL_REQUESTS_TOPIC, RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokHashtagClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response,
    top_related_hashtags,
    update_related_hashtag_counts,
)
from social_crawler.spiders.tiktok.items import TikTokVideoItem

logger = get_logger(__name__)

# Total attempts across every rotated account for one crawl_request - not
# "how many accounts exist", just a small ceiling on how many times a
# single TikTokBlockedError is worth retrying before accepting this run is
# failing for a reason a different account won't fix either.
MAX_ACCOUNT_ATTEMPTS = 2


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
        bfs_depth: int = 0,
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
        # 0 for a manually-queued hashtag; > 0 only ever set by a BFS-
        # discovered crawl_request (see crawl_request_consumer.py), one
        # more than whatever hashtag discovered this one - see
        # BFS_MAX_DEPTH's own comment for why this needs a ceiling at all.
        self.bfs_depth = int(bfs_depth)
        self._cache: RedisCache | None = None
        self._post_count = 0
        self._related_hashtag_counts: Counter[tuple[str, str]] = Counter()
        self._kafka = KafkaPublisher()

    async def start(self):
        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        # Everything that needs the Kafka producer still running lives in
        # this one try - _queue_bfs_hashtags() publishes new crawl_requests
        # near the very end, so stopping the producer has to wait until
        # after that (a stopped KafkaPublisher's publish() call doesn't
        # fail fast, it hangs - confirmed by direct test - so this ordering
        # isn't just tidiness, getting it wrong wedges the whole process).
        try:
            for attempt in range(1, MAX_ACCOUNT_ATTEMPTS + 1):
                try:
                    async for item in self._crawl_with_fresh_account():
                        yield item
                    break
                except TikTokBlockedError as exc:
                    if attempt < MAX_ACCOUNT_ATTEMPTS:
                        logger.warning(
                            "blocked_retrying_with_different_account",
                            attempt=attempt,
                            max_attempts=MAX_ACCOUNT_ATTEMPTS,
                            error=str(exc),
                        )
                        continue
                    logger.error("blocked", telegram=True, error=str(exc))
                    return

            logger.info("crawl_finished", telegram=True, posts=self._post_count, hashtag=self.hashtag)

            # Surfaced for a human to review, not auto-added to D1's
            # keywords - a generic co-occurring tag (e.g. "#reviewphim")
            # would otherwise start pulling in unrelated movies' videos
            # under this one's keyword_id. Silent (no telegram) when
            # nothing crosses the min_occurrences bar, so an ordinary run
            # doesn't ping the channel.
            related = top_related_hashtags(self._related_hashtag_counts)
            if related:
                logger.info(
                    "related_hashtags_found",
                    telegram=True,
                    hashtag=self.hashtag,
                    related=[f"#{tag['title']} (id={tag['id']}, seen {tag['count']}x)" for tag in related],
                )
                await self._queue_bfs_hashtags(related)
        except TikTokRateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("tiktok", "rate_limited", self._cache)
        except TikTokNetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='tiktok' - "
                "this is a proxy/network problem, not a stale identity, re-capturing cookie/device_id/odin_id won't help.",
            )
            note_transient_error("tiktok", "network_error", self._cache)
        finally:
            await self._kafka.stop()

    async def _crawl_with_fresh_account(self) -> AsyncIterator[TikTokVideoItem]:
        """One full attempt: rotate to whatever account next_account() picks
        next, resolve self.hashtag against it, then crawl every page. Split
        out from start() so a TikTokBlockedError retry re-runs this whole
        thing (fresh account, fresh resolve_hashtag call) rather than
        reusing a client tied to the account that just got blocked."""
        client = TikTokHashtagClient(redis_cache=self._cache)
        challenge_id = await asyncio.to_thread(client.resolve_hashtag, self.hashtag)

        if not challenge_id:
            logger.error("hashtag_not_found", telegram=True, hashtag=self.hashtag)
            return

        if self._cache:
            self._cache.sadd(SEEN_HASHTAGS_KEY, str(challenge_id))

        async for item in self._crawl(client, challenge_id):
            yield item

    async def _queue_bfs_hashtags(self, related: list[dict]) -> None:
        """Publishes up to BFS_MAX_HASHTAGS_PER_RUN of this run's related
        hashtags as their own crawl_requests - see this module's docstring
        for the full rationale. No-ops past BFS_MAX_DEPTH or without Redis
        (SEEN_HASHTAGS_KEY dedupe needs it to avoid runaway re-queuing, so
        skipping BFS entirely is safer than queuing unbounded duplicates)."""
        if self._cache is None or self.bfs_depth >= BFS_MAX_DEPTH:
            return

        queued = []
        for tag in related[:BFS_MAX_HASHTAGS_PER_RUN]:
            if self._cache.sadd(SEEN_HASHTAGS_KEY, str(tag["id"])) == 0:
                continue  # already crawled or already queued by another branch
            await self._kafka.publish(
                topic=CRAWL_REQUESTS_TOPIC,
                key=f"tiktok-bfs:{tag['id']}",
                value={
                    "platform": "tiktok",
                    "keyword": tag["title"],
                    # Deliberately no keyword_id: a hashtag discovered by
                    # co-occurrence isn't reliably about whatever
                    # movie/keyword started this chain (see
                    # "related_hashtags_found"'s own comment) - this grows
                    # total ingested volume without attributing it to that
                    # keyword's stats.
                    "keyword_id": None,
                    "max_pages": BFS_MAX_PAGES,
                    "bfs_depth": self.bfs_depth + 1,
                },
            )
            queued.append(tag["title"])

        if queued:
            logger.info("bfs_hashtags_queued", hashtag=self.hashtag, depth=self.bfs_depth, queued=queued)

    async def _crawl(self, client: TikTokHashtagClient, challenge_id: str) -> AsyncIterator[TikTokVideoItem]:
        cursor = 0
        page = 1

        while True:
            response = await asyncio.to_thread(client.search_hashtag, challenge_id, cursor, self.count)
            videos = extract_response(response)
            update_related_hashtag_counts(response, self._related_hashtag_counts, exclude_ids={challenge_id})

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
