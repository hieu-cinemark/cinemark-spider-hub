"""
TikTok channel/user video-list spider - drives a real, headless* Patchright
browser to a creator's own profile page (https://www.tiktok.com/@<username>)
and reads its own /api/post/item_list/ network responses directly.

This is the "get all videos posted by one channel" endpoint - distinct from
tiktok_hashtag_search (a #hashtag's own video feed, curl_cffi-only, no
browser). Do
NOT assume a browser is needed here just because search/comments needed
one, or that curl_cffi is fine just because hashtag_search's own "needs a
browser" claim turned out to be wrong - both of those turned out to be
endpoint-specific, confirmed by direct testing, not general TikTok
properties. This endpoint was tested the same way, independently:

Confirmed necessary by direct live A/B test (2026-09-16), against a real
user-captured request (device_id 7643007170271839760, a channel's own
/api/post/item_list/ call, real cookies/X-Gnarly/X-Dynosaur - not one of
this project's own platform_accounts rows, used specifically so the test
couldn't burn one of the pool's few remaining live accounts):

  1. Replayed verbatim through curl_cffi (Chrome TLS impersonation): real
     data back (itemList of 16 videos) - the capture wasn't stale.
  2. The exact same URL, byte-for-byte identical except X-Dynosaur deleted
     (or replaced with an obviously-wrong value): empty 200. Nothing else
     changed - same real X-Gnarly, same param order, same everything.
  3. Immediately re-replaying the original untouched URL again still
     worked - ruling out "this identity got rate-limited mid-test" as an
     alternative explanation for step 2's emptiness.
  4. Separately: rebuilding the request from this project's own
     STATIC_PARAMS-style dict plus a freshly-computed local X-Gnarly (via
     signature/gnarly.py) - even keeping the real captured X-Dynosaur
     verbatim - ALSO came back empty. So unlike hashtag_search, a locally-
     signed request isn't equivalent to the browser's here even before
     X-Dynosaur is touched. This project has no working local X-Dynosaur
     implementation (an earlier reverse-engineering attempt,
     signature/dynasaur.py, was removed 2026-09-17 - never confirmed
     against a real response, not wired into any client) - so this
     endpoint is browser-only. See constants/tiktok.py's own
     POST_ITEM_LIST_URL comment for the same trail in one place, including
     a 2026-09-17 note that this conclusion predates discovering VN-geo
     IPs (not curl_cffi/guest-mode itself) were the real cause of a
     similar "browser-only" scare for the hashtag endpoint - worth
     re-testing against a non-VN proxy before trusting it fully.

The response shape itself needed no new extraction logic: a captured
/api/post/item_list/ response's itemList items carry the exact same fields
(id/desc/video/author/stats/contents/challenges/...) as hashtag_search's
own /api/challenge/item_list/ items, so features/channel_videos/extract.py
just re-exports hashtag_search's extract_response/extract_video rather than
re-implementing them.

* headless=True here is carried over from tiktok_hashtag_search (confirmed there,
  by direct test, that headless works identically to headful for
  /api/search/general/full/) - NOT independently re-confirmed for THIS
  endpoint, to avoid spending another live account+proxy run just to check
  headless vs headful on top of the curl_cffi question this module was
  actually built to answer. tiktok_comments needed headful specifically
  (see its own module docstring) for a DOM-interaction-heavy flow (opening
  a comment panel); this spider's flow (scroll a profile's video grid) is
  structurally closer to tiktok_hashtag_search's than to tiktok_comments', which is
  why headless is the starting default here - but if a real run captures
  zero pages on headless where a manual browser visit clearly shows videos,
  try headless=False here first before suspecting anything else.

Run:
    scrapy crawl tiktok_channel_videos -a username="linzenguyen"

Pass -a dedupe=false to disable cross-run dedupe - on by default whenever
Redis is reachable, silently falls back to in-run-only dedupe otherwise.

Not wired into crawl_request_consumer.py's dispatch - that consumer routes
by keyword text (hashtag vs free-text search), and there's no "channel to
track" concept in D1 yet for this to hook into. Runs standalone via `scrapy
crawl` until/unless that product decision gets made.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import quote

import scrapy
from patchright.sync_api import sync_playwright

from social_crawler.constants.tiktok import POST_ITEM_LIST_URL, SEEN_POSTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.services.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.browser_utils import scroll_feed_to_bottom
from social_crawler.spiders.tiktok.auth.accounts import next_account
from social_crawler.spiders.tiktok.auth.cookies import build_storage_state_from_cookies
from social_crawler.spiders.tiktok.features.channel_videos.extract import extract_response
from social_crawler.spiders.tiktok.items import TikTokChannelVideoItem

logger = get_logger(__name__)

# Total attempts across every rotated account for one crawl_request - same
# rationale as tiktok_hashtag_search's own MAX_ACCOUNT_ATTEMPTS.
MAX_ACCOUNT_ATTEMPTS = 2

# How many consecutive scrolls are allowed to capture no new
# /api/post/item_list/ response before giving up on this account's session -
# same value/rationale as tiktok_hashtag_search's own _MAX_STALL_SCROLLS (a
# channel's grid is the same kind of infinite-scroll feed).
_MAX_STALL_SCROLLS = 10


class TikTokAccountUnusableError(RuntimeError):
    """No enabled tiktok account (of any kind) was available to run this
    crawl at all - not a per-account failure, see next_account()."""


class _ZeroPagesCaptured(Exception):
    """Internal control-flow signal for start()'s retry loop - a rotated
    account's browser session captured no channel-video pages at all,
    distinct from TikTokAccountUnusableError (no account was even
    available to try). Same shape as tiktok_hashtag_search/tiktok_comments' own
    internal signal."""


def _capture_channel_pages(
    username: str,
    max_pages: int,
    storage_state: dict | None,
    proxy: dict | None,
) -> list[dict[str, Any]]:
    """Opens the profile page for `username` in a real browser and scrolls
    to collect up to max_pages worth of /api/post/item_list/ responses -
    see this module's own docstring for why. storage_state (from a
    logged-in account's cookie), when given, means every request TikTok's
    page JS fires is signed as that logged-in session; None runs as a
    guest."""
    pages: list[dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, proxy=proxy)
        context = browser.new_context(
            locale="en-US", viewport={"width": 1366, "height": 900}, storage_state=storage_state
        )
        page = context.new_page()

        def on_response(resp):
            if POST_ITEM_LIST_URL not in resp.url:
                return
            try:
                body = resp.text()
            except Exception as exc:
                logger.warning("channel_response_read_failed", error=str(exc))
                return
            if not body:
                return
            try:
                pages.append(json.loads(body))
            except json.JSONDecodeError as exc:
                logger.warning("channel_response_parse_failed", error=str(exc))

        page.on("response", on_response)
        try:
            try:
                page.goto(
                    f"https://www.tiktok.com/@{quote(username)}", wait_until="domcontentloaded", timeout=30000
                )
            except Exception as exc:
                # Same rationale as tiktok_hashtag_search's own _capture_search_pages:
                # a slow/overloaded proxy timing out here must not crash the
                # whole spider - indistinguishable from "this account/proxy
                # captured zero pages" to the caller, which already knows to
                # retry with a different account/proxy for that.
                logger.warning("channel_navigation_failed", username=username, error=str(exc))
                return pages
            page.wait_for_timeout(3000)

            stalled_scrolls = 0
            while len(pages) < max_pages and stalled_scrolls < _MAX_STALL_SCROLLS:
                before = len(pages)
                scroll_feed_to_bottom(page, item_selector='a[href*="/video/"]')
                page.wait_for_timeout(3500)
                if len(pages) == before:
                    stalled_scrolls += 1
                    if pages and not pages[-1].get("hasMore"):
                        break
                else:
                    stalled_scrolls = 0
        finally:
            browser.close()

    return pages


class TikTokChannelVideosSpider(scrapy.Spider):
    name = "tiktok_channel_videos"

    # This spider drives its own Patchright browser instead of going through
    # Scrapy's downloader, so robots.txt and downloader middlewares don't
    # apply here.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        username: str = "",
        keyword_id: str | None = None,
        max_pages: int = 20,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.username = username.lstrip("@").strip()
        # Opaque to this spider - just threaded through to Kafka on every
        # published post, same as hashtag_search/search's own keyword_id.
        # Typically None for a channel crawl (there's no keyword driving
        # it) - kept only for Kafka payload-shape consistency with every
        # other TikTok video-source spider.
        self.keyword_id = keyword_id
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._post_count = 0
        self._failed_device_ids: set[str] = set()
        self._kafka = KafkaPublisher()

    async def start(self):
        if not self.username:
            logger.error("missing_username", hint='scrapy crawl tiktok_channel_videos -a username="<a channel handle>"')
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            for attempt in range(1, MAX_ACCOUNT_ATTEMPTS + 1):
                zero_pages = False
                try:
                    async for item in self._crawl_with_fresh_account():
                        yield item
                except _ZeroPagesCaptured:
                    zero_pages = True
                except TikTokAccountUnusableError as exc:
                    logger.error("tiktok_account_unusable", telegram=True, error=str(exc))
                    return

                if not zero_pages:
                    break
                if attempt < MAX_ACCOUNT_ATTEMPTS:
                    logger.warning(
                        "blocked_retrying_with_different_account",
                        attempt=attempt,
                        max_attempts=MAX_ACCOUNT_ATTEMPTS,
                    )
                    continue
                logger.error(
                    "blocked",
                    telegram=True,
                    username=self.username,
                    hint="every rotated account captured zero channel-video pages - see module docstring",
                )
                return

            logger.info("crawl_finished", telegram=True, posts=self._post_count, username=self.username)
        finally:
            await self._kafka.stop()

    async def _crawl_with_fresh_account(self):
        """One full attempt: rotate to whatever next_account() picks next
        (excluding device_ids that already captured zero pages this run),
        drive a real browser through self.username's profile, then process
        every page it captured."""
        redis_cache = self._cache or RedisCache()
        account = next_account(redis_cache, exclude_ids=self._failed_device_ids, require_login=False)
        if account is None:
            raise TikTokAccountUnusableError(
                "No enabled tiktok account available (every account already tried this run, or none exist)."
            )

        device_id = account["id"]
        storage_state = build_storage_state_from_cookies(account["cookie"]) if account.get("cookie") else None

        proxy = None
        try:
            proxy_cfg = pool.acquire_proxy_for_account("tiktok", device_id, required=True)
        except pool.ProxyPoolExhaustedError as exc:
            raise TikTokAccountUnusableError(str(exc)) from exc
        if proxy_cfg:
            proxy = {
                "server": f"http://{proxy_cfg['url']}",
                "username": proxy_cfg["username"],
                "password": proxy_cfg["password"],
            }

        pages = await asyncio.to_thread(_capture_channel_pages, self.username, self.max_pages, storage_state, proxy)

        if not pages:
            self._failed_device_ids.add(device_id)
            if proxy_cfg:
                pool.release_proxy(proxy_cfg, success=False)
            raise _ZeroPagesCaptured()

        if proxy_cfg:
            pool.release_proxy(proxy_cfg, success=True)

        async for item in self._process_pages(pages):
            yield item

    async def _process_pages(self, pages: list[dict[str, Any]]):
        for page_number, response in enumerate(pages, start=1):
            videos = extract_response(response)

            new_posts = 0
            for video in videos:
                video_id = video.get("video_id")
                if not video_id:
                    continue
                video_id = str(video_id)
                # Same SEEN_POSTS_KEY as hashtag_search (not a
                # channel-only key) - a video this channel crawl finds that
                # hashtag_search already published (or vice versa) is
                # deliberately treated as the same post, not published twice.
                if self._cache and self._cache.sadd(SEEN_POSTS_KEY, video_id) == 0:
                    continue
                new_posts += 1
                self._post_count += 1
                await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"tiktok:{video_id}",
                    value={"platform": "tiktok", "keyword_id": self.keyword_id, **video},
                )
                yield TikTokChannelVideoItem(username=self.username, **video)

            logger.info("page_crawled", page=page_number, new_posts=new_posts, fetched=len(videos))
