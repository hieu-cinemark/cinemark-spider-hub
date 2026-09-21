"""
TikTok hashtag-search spider that never opens a browser: calls
/api/challenge/item_list/ directly through curl_cffi (impersonating a
Chrome TLS fingerprint), signing every request locally with a freshly
computed X-Gnarly (see signature/gnarly.py). Mirrors
social_crawler.spiders.threads.features.search.search - see that module's
docstring and client.py's module docstring for the full rationale.

Runs as a fresh, synthetic guest identity (TikTokHashtagClient(synthetic=
True)) - no platform_accounts row at all, see client.py's own docstring
for the mechanism. This replaced a platform_accounts-rotation design that
turned out to be actively harmful here: TikTok tracks abuse signal per
device_id, and a small pool of accounts reused across many crawls all day
eventually needed a real X-Dynosaur header (browser-JS-only, no working
local implementation) even though nothing about the request itself was
wrong. Confirmed by direct live A/B testing (2026-09-17): three different
long-lived accounts, three different proxies (including two never used for
TikTok before that day), all got an empty response from a perfectly valid,
freshly-issued X-Gnarly - while a synthetic device_id/odinId/cookie set
generated in the same few minutes, on the very same proxies, worked on the
first try, every time, no X-Dynosaur needed. So "guest mode is dead" (the
previous conclusion here) was really "this specific handful of reused
identities is dead" - minting a new one per crawl sidesteps the whole
class of failure instead of managing it.

Earlier investigation trail, superseded by the above but kept for context:
a real browser variant was tried for the logged-in path - it does get
meaningfully more results per hashtag when TikTok's own JS signs the
request, but pagination past the first couple of pages depends on TikTok's
own internal SPA state advancing, which scrolling/re-fetching from outside
can't reliably drive - results were 2-5x noisier and often no better than
the guest path. An independent third-party project (github.com/caixax/
opentok's TIKTOK-API.md) hit what looked like the same "browser-only" wall
and concluded X-Dynosaur is categorically required - true for a *reused*
identity (matches this project's own finding above), not for a fresh one.

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

  - Retry-with-a-fresh-identity on TikTokBlockedError: since every attempt
    already mints a brand-new synthetic identity (see client.py's own
    docstring), a TikTokBlockedError here means this one draw was
    unlucky (a proxy blip, a race with something else on the same IP),
    not a systemic problem - so instead of giving up outright, it
    re-resolves and re-runs once with a fresh client. Already-published
    videos aren't re-published on the retry (SEEN_POSTS_KEY dedupe blocks
    them same as any other repeat), so the only cost of a spurious retry
    is one extra resolve_hashtag round trip. Rate-limit/network errors
    don't get this treatment - both are explicitly documented as not
    being an identity problem, so a fresh identity wouldn't help either.

  - Related hashtags for dashboard review: co-occurring tags (see
    extract.top_related_hashtags) are stored in Redis under
    RELATED_HASHTAGS_KEY_TMPL when the crawl has a keyword_id, so an
    operator can approve a BFS hop from the keyword table. This spider
    never auto-queues follow-up crawls.
"""

from __future__ import annotations

import asyncio
import sys
from collections import Counter
from collections.abc import AsyncIterator

import scrapy

from social_crawler.constants.tiktok import (
    BFS_MAX_DEPTH,
    BFS_MAX_HASHTAGS_PER_RUN,
    BFS_MAX_PAGES,
    MAX_CONSECUTIVE_EMPTY_NEW_PAGES,
    PROXY_EXHAUSTED_EXIT_CODE,
    RELATED_HASHTAGS_KEY_TMPL,
    RELATED_HASHTAGS_TTL_SECONDS,
    SEEN_HASHTAGS_KEY,
    SEEN_POSTS_KEY,
)
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.error_alerts import note_transient_error
from social_crawler.services.kira import classify_hashtag_relevance
from social_crawler.services.kafka import RAW_POSTS_TOPIC, KafkaPublisher
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

# Total attempts across every fresh synthetic identity for one
# crawl_request - not "how many accounts exist" (there's no account pool
# involved here any more, see client.py's own docstring), just a ceiling on
# how many times a single TikTokBlockedError is worth retrying before
# accepting this run is failing for a reason a fresh identity won't fix
# either.
#
# Raised twice on 2026-09-17: first 2->4, then 4->8 once live testing
# showed the proxiestrust US pool's real clean-IP rate is closer to 1-in-4
# or 1-in-5 than the original 1-in-3 estimate (get_new_proxy's own
# _MAX_COOLDOWN_WAIT_SECONDS already makes each attempt wait out the
# vendor's ~90s rotation cooldown rather than silently settling for an
# already-known-bad DB proxy, so every attempt here is a genuinely
# independent draw, not a wasted one) - at the user's own explicit
# direction, prioritizing "the crawl actually gets data" over wall-clock
# speed (a bigger gap between keywords is fine; a keyword that silently
# yields nothing is not). 8 independent draws at a conservative 20% clean
# rate clears ~83% odds of at least one success per crawl_request; each
# failed draw costs roughly one cooldown wait (~45-90s), so a fully-unlucky
# run can take several minutes - acceptable given the above.
MAX_ACCOUNT_ATTEMPTS = 8


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
        # 0 for a manually-queued hashtag; > 0 only on an operator-approved
        # BFS hop (see crawl_request_consumer.py). Capped at BFS_MAX_PAGES.
        self.bfs_depth = int(bfs_depth)
        if self.bfs_depth > 0:
            self.max_pages = min(self.max_pages, BFS_MAX_PAGES)
        self._cache: RedisCache | None = None
        self._post_count = 0
        self._related_hashtag_counts: Counter[tuple[str, str]] = Counter()
        self._kafka = KafkaPublisher()

    async def start(self):
        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        # Everything that needs the Kafka producer still running lives in
        # this one try. Related-hashtag chips are written near the end;
        # stopping the producer has to wait until after that (a stopped
        # KafkaPublisher's publish() call hangs).
        try:
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
                        sys.exit(1)
                    except pool.ProxyPoolExhaustedError as exc:
                        logger.error("tiktok_proxy_pool_exhausted", telegram=True, error=str(exc))
                        sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
                    except RuntimeError as exc:
                        logger.error("tiktok_account_unusable", telegram=True, error=str(exc))
                        sys.exit(1)
            except TikTokRateLimitedError as exc:
                logger.error("rate_limited", telegram=True, error=str(exc))
                note_transient_error("tiktok", "rate_limited", self._cache)
                sys.exit(1)
            except TikTokNetworkError as exc:
                if isinstance(exc.__cause__, pool.ProxyPoolExhaustedError):
                    logger.error("tiktok_proxy_pool_exhausted", telegram=True, error=str(exc))
                    sys.exit(PROXY_EXHAUSTED_EXIT_CODE)
                logger.error(
                    "network_error",
                    telegram=True,
                    error=str(exc),
                    hint="check connectivity to the platform_proxies row for platform='tiktok' - "
                    "this is a proxy/network problem, not a stale identity, re-capturing cookie/device_id/odin_id won't help.",
                )
                note_transient_error("tiktok", "network_error", self._cache)
                sys.exit(1)

            logger.info("crawl_finished", telegram=True, posts=self._post_count, hashtag=self.hashtag)

            related = top_related_hashtags(self._related_hashtag_counts)
            if related:
                logger.info(
                    "related_hashtags_found",
                    telegram=True,
                    hashtag=self.hashtag,
                    related=[f"#{tag['title']} (id={tag['id']}, seen {tag['count']}x)" for tag in related],
                )
                await self._queue_bfs_hashtags(related)
        finally:
            await self._kafka.stop()

    async def _crawl_with_fresh_account(self) -> AsyncIterator[TikTokVideoItem]:
        """One full attempt: mint a brand-new synthetic guest identity,
        resolve self.hashtag against it, then crawl every page. Split out
        from start() so a TikTokBlockedError retry re-runs this whole thing
        (fresh identity, fresh resolve_hashtag call) rather than reusing a
        client tied to the identity that just got blocked - see client.py's
        own docstring for why a fresh identity is the actual fix here."""
        # to_thread: __init__ does blocking network I/O itself now (mint a
        # fresh proxy lease, mint guest cookies) and can block for up to
        # ~100s if it has to wait out proxiestrust's own rotation cooldown
        # (see proxy_provider.get_new_proxy's own docstring) - must not
        # block the event loop the Kafka producer/other spiders share.
        client = await asyncio.to_thread(TikTokHashtagClient, redis_cache=self._cache, synthetic=True)
        challenge_id = await asyncio.to_thread(client.resolve_hashtag, self.hashtag)

        if not challenge_id:
            logger.error("hashtag_not_found", telegram=True, hashtag=self.hashtag)
            return

        if self._cache:
            self._cache.sadd(SEEN_HASHTAGS_KEY, str(challenge_id))

        async for item in self._crawl(client, challenge_id):
            yield item

    async def _queue_bfs_hashtags(self, related: list[dict]) -> None:
        """Store related tags for dashboard approval. Never publishes
        crawl_requests - an operator click creates the keyword and queues
        the hop with bfs_depth. Stops suggesting tags at BFS_MAX_DEPTH.
        When AI settings are enabled, generic co-occurring tags are dropped."""
        if self.bfs_depth >= BFS_MAX_DEPTH:
            logger.info(
                "bfs_max_depth_reached",
                hashtag=self.hashtag,
                bfs_depth=self.bfs_depth,
            )
            return
        if not self.keyword_id:
            logger.info("related_hashtags_not_stored", hashtag=self.hashtag, reason="no_keyword_id")
            return
        if self._cache is None:
            logger.info("related_hashtags_not_stored", hashtag=self.hashtag, reason="no_cache")
            return
        next_depth = self.bfs_depth + 1
        candidates: list[dict] = []
        for tag in related:
            cid = str(tag.get("id") or "")
            if cid and self._cache.sismember(SEEN_HASHTAGS_KEY, cid):
                continue
            title = str(tag.get("title") or "")
            relevant = await classify_hashtag_relevance(self.hashtag, title)
            if relevant is False:
                logger.info("bfs_hashtag_rejected_generic", root=self.hashtag, candidate=title)
                continue
            candidates.append({**tag, "bfs_depth": next_depth})
            if len(candidates) >= BFS_MAX_HASHTAGS_PER_RUN:
                break
        if not candidates:
            logger.info("bfs_no_new_hashtags", hashtag=self.hashtag)
            return
        self._cache.set(
            RELATED_HASHTAGS_KEY_TMPL.format(keyword_id=self.keyword_id),
            candidates,
            ttl_seconds=RELATED_HASHTAGS_TTL_SECONDS,
        )
        logger.info(
            "related_hashtags_stored",
            hashtag=self.hashtag,
            keyword_id=self.keyword_id,
            count=len(candidates),
            bfs_depth=next_depth,
        )

    async def _crawl(self, client: TikTokHashtagClient, challenge_id: str) -> AsyncIterator[TikTokVideoItem]:
        cursor = 0
        page = 1
        empty_new_streak = 0

        while True:
            response = await asyncio.to_thread(
                client.search_hashtag, challenge_id, cursor, self.count, self.hashtag
            )
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

            if new_posts == 0:
                empty_new_streak += 1
            else:
                empty_new_streak = 0

            logger.info(
                "page_crawled",
                page=page,
                new_posts=new_posts,
                fetched=len(videos),
                empty_new_streak=empty_new_streak,
            )

            if empty_new_streak >= MAX_CONSECUTIVE_EMPTY_NEW_PAGES:
                logger.info(
                    "keyword_skipped_no_new_posts",
                    telegram=True,
                    hashtag=self.hashtag,
                    keyword_id=self.keyword_id,
                    pages=page,
                    empty_new_streak=empty_new_streak,
                    hint=f"{MAX_CONSECUTIVE_EMPTY_NEW_PAGES} consecutive pages with 0 new posts - stopping this keyword",
                )
                break

            if page >= self.max_pages or not response.get("hasMore"):
                break
            next_cursor = response.get("cursor")
            if next_cursor is None or int(next_cursor) == cursor:
                break
            cursor = int(next_cursor)
            page += 1
