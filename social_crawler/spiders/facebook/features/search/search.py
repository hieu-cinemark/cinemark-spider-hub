"""
Facebook search spider that never opens a browser: calls the GraphQL
endpoint directly through curl_cffi (impersonating a Chrome TLS
fingerprint), using the token cached by `social_crawler.spiders.facebook.auth.bootstrap`.

Run:
    scrapy crawl facebook_search -a query="keyword"

max_pages defaults to 100 as a safety ceiling, not a target - the loop
already stops on its own once Facebook reports no more pages (has_next_page
is False), so this rarely gets hit in practice. Pass -a max_pages=N to cap
it lower.

Pass -a dedupe=false to disable cross-run dedupe (e.g. to re-fetch posts
already seen in a previous run) - it's on by default whenever Redis is
reachable, and silently falls back to in-run-only dedupe otherwise.

Pass -a start_date=YYYY-MM-DD -a end_date=YYYY-MM-DD (both required
together) to only get posts created in that range, using Facebook's own
"Date posted" search filter - e.g.:
    scrapy crawl facebook_search -a query="keyword" -a start_date=2026-08-01 -a end_date=2026-08-15

Facebook caps how many results a single search query returns, no matter how
far you paginate - each `start_date`/`end_date`-filtered search is evaluated
independently though, so sweeping the same query across many small date
windows gets past that cap instead of hitting it once and stopping. Pass
-a sweep_days=N to sweep the last N days (most recent first) in windows of
-a sweep_window_days=M days each (default 1 = one query per day) - e.g.:
    scrapy crawl facebook_search -a query="keyword" -a sweep_days=30 -a sweep_window_days=3
This overrides start_date/end_date when set. Between windows (not between
pages within one window - that's graphql_client's own throttle), the spider
pauses a random amount of time - default 5-20s, override with
-a sweep_pause_min=N -a sweep_pause_max=N - since firing 30 separate
searches back to back with no gap is itself a bot-like pattern.

Entities (Group/User/Hashtag/Photo/Video/... bundled in the same search
response) are yielded alongside posts by default - pass
-a include_entities=false to only get posts.
"""

from __future__ import annotations

import asyncio
import random
from datetime import date, timedelta
from typing import AsyncIterator, Iterator

import scrapy

from social_crawler.constants.facebook import SEEN_ENTITIES_KEY, SEEN_POSTS_KEY
from social_crawler.items import FacebookEntityItem, FacebookPostItem
from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.facebook.auth.graphql_client import (
    FacebookGraphQLClient,
    SessionExpiredError,
    find_page_info,
)
from social_crawler.spiders.facebook.features.search.extract import extract_response

logger = get_logger(__name__)


def _date_windows(sweep_days: int, window_days: int) -> Iterator[tuple[date, date]]:
    """Yield (start, end) day ranges covering the last `sweep_days` days
    (today included), most recent window first, each up to `window_days`
    wide. Used to sweep past Facebook's per-query results cap - see the
    module docstring."""
    today = date.today()
    offset = 0
    while offset < sweep_days:
        window_end = today - timedelta(days=offset)
        window_start = today - timedelta(days=min(offset + window_days - 1, sweep_days - 1))
        yield window_start, window_end
        offset += window_days


class FacebookSearchSpider(scrapy.Spider):
    name = "facebook_search"

    # This spider never goes through Scrapy's downloader (it calls curl_cffi
    # directly to impersonate a real Chrome TLS fingerprint), so robots.txt
    # and downloader middlewares don't apply here.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        query: str = "test",
        count: int = 5,
        max_pages: int = 100,
        dedupe: str = "true",
        start_date: str | None = None,
        end_date: str | None = None,
        include_entities: str = "true",
        sweep_days: int = 0,
        sweep_window_days: int = 1,
        sweep_pause_min: float = 5.0,
        sweep_pause_max: float = 20.0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self.start_date = date.fromisoformat(start_date) if start_date else None
        self.end_date = date.fromisoformat(end_date) if end_date else None
        self.include_entities = str(include_entities).lower() not in ("false", "0", "no")
        self.sweep_days = int(sweep_days)
        self.sweep_window_days = max(1, int(sweep_window_days))
        # Gap between sweep windows (not between pages within one window -
        # graphql_client's own throttle already handles that): a real person
        # pauses between separate searches instead of firing them back to
        # back, and it also spreads a long sweep out over more of the
        # proxy's IP rotation window instead of hammering through it as one
        # burst.
        self.sweep_pause_min = float(sweep_pause_min)
        self.sweep_pause_max = float(sweep_pause_max)
        self._cache: RedisCache | None = None
        # Shared across windows so the same post/entity surfaced by two
        # overlapping windows in the same run is only yielded once.
        self._seen_ids: set[str] = set()
        self._post_count = 0
        self._entity_count = 0

    async def start(self):
        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            client = FacebookGraphQLClient()
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.facebook.auth.bootstrap --query "{self.query}"',
            )
            return

        if self.sweep_days > 0:
            windows = list(_date_windows(self.sweep_days, self.sweep_window_days))
            logger.info("sweep_enabled", windows=len(windows), sweep_days=self.sweep_days, window_days=self.sweep_window_days)
            for i, (window_start, window_end) in enumerate(windows):
                async for item in self._crawl_window(client, window_start, window_end):
                    yield item
                if i < len(windows) - 1:
                    pause = random.uniform(self.sweep_pause_min, self.sweep_pause_max)
                    logger.info("sweep_pause", seconds=round(pause, 1))
                    await asyncio.sleep(pause)
        else:
            async for item in self._crawl_window(client, self.start_date, self.end_date):
                yield item

        logger.info(
            "crawl_finished", telegram=True, posts=self._post_count, entities=self._entity_count, query=self.query
        )

    async def _crawl_window(
        self, client: FacebookGraphQLClient, start_date: date | None, end_date: date | None
    ) -> AsyncIterator[FacebookPostItem | FacebookEntityItem]:
        """Run the paginated search loop once for a single start_date/end_date
        window (or the unfiltered whole-history search if both are None),
        stopping once Facebook reports no more pages or max_pages is hit."""
        window_label = f"{start_date}:{end_date}" if start_date else "unfiltered"
        cursor: str | None = None
        page = 1

        while True:
            try:
                if cursor is None:
                    response = await asyncio.to_thread(client.search, self.query, self.count, start_date, end_date)
                else:
                    response = await asyncio.to_thread(
                        client.search_next_page, self.query, cursor, self.count, start_date, end_date
                    )
            except SessionExpiredError as exc:
                logger.error("session_expired", error=str(exc))
                return

            posts, others = extract_response(response)

            new_posts = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id or post_id in self._seen_ids:
                    continue
                self._seen_ids.add(post_id)
                if self._cache and self._cache.sismember(SEEN_POSTS_KEY, post_id):
                    continue
                if self._cache:
                    self._cache.sadd(SEEN_POSTS_KEY, post_id)
                new_posts += 1
                self._post_count += 1
                yield FacebookPostItem(query=self.query, **post)

            new_entities = 0
            if self.include_entities:
                for entity in others:
                    entity_id = entity.get("id")
                    if not entity_id or entity_id in self._seen_ids:
                        continue
                    self._seen_ids.add(entity_id)
                    if self._cache and self._cache.sismember(SEEN_ENTITIES_KEY, entity_id):
                        continue
                    if self._cache:
                        self._cache.sadd(SEEN_ENTITIES_KEY, entity_id)
                    new_entities += 1
                    self._entity_count += 1
                    yield FacebookEntityItem(query=self.query, **entity)

            logger.info("page_crawled", window=window_label, page=page, new_posts=new_posts, new_entities=new_entities)

            page_info = find_page_info(response)
            if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1
