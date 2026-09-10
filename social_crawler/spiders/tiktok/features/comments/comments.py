"""
TikTok comments spider that drives a real, VISIBLE (headful) Patchright
browser and captures /api/comment/list/ responses directly off the page's
own network traffic - it never signs or replays a request itself, unlike
every other spider in this project.

Confirmed necessary by direct experiment, not assumed:
- A byte-for-byte curl_cffi replay of a genuine captured request (correct
  X-Gnarly, correct X-Dynosaur, real cookies) gets an empty 200 back.
- A *headless* Patchright browser hitting the real endpoint also gets an
  empty 200 back, even though the exact same page loads fine otherwise.
- Only a *headful* (visible) Patchright browser gets real data back.
TikTok's device-trust check for this endpoint apparently distinguishes
headless from headful on top of everything else - the same class of wall
constants/tiktok.py already documents for the abandoned keyword-search
endpoint, just one level deeper (that one failed even via a real browser
session's lifted identity; this one fails even via a live real browser
unless it's actually headful).

This means, unlike every other spider in this project, this one needs an
actual display to run against - on a headless server this needs a virtual
framebuffer (e.g. Xvfb) or it won't get real comments either. Not solved
here yet; a real deployment constraint to come back to.

Run:
    scrapy crawl tiktok_comments -a video_id="7670822924022074645" \
        -a video_url="https://www.tiktok.com/@user/video/7670822924022074645"
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import scrapy
from patchright.sync_api import sync_playwright

from social_crawler.constants.tiktok import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.tiktok.features.comments.extract import extract_comments
from social_crawler.spiders.tiktok.items import TikTokCommentItem

logger = get_logger(__name__)

# TikTok shows a cookie-consent/onboarding overlay on a brand-new context,
# same idea as Facebook/Threads' own COOKIE_CONSENT_BUTTON_SELECTORS -
# tried in order, first match wins, silently skipped if none show.
_OVERLAY_DISMISS_TEXTS = ("Accept all", "Accept", "Got it", "OK", "Skip")


def _capture_comment_pages(video_url: str, max_pages: int) -> list[dict[str, Any]]:
    """Opens video_url in a real, visible browser, opens the comment panel,
    and scrolls to collect up to max_pages worth of /api/comment/list/
    responses - see module docstring for why this has to be a real,
    headful browser rather than a signed replay."""
    pages: list[dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(locale="en-US", viewport={"width": 1366, "height": 900})
        page = context.new_page()

        def on_response(resp):
            if "/api/comment/list/" not in resp.url:
                return
            try:
                body = resp.text()
            except Exception as exc:
                logger.warning("comment_response_read_failed", error=str(exc))
                return
            if not body:
                return
            try:
                pages.append(json.loads(body))
            except json.JSONDecodeError as exc:
                logger.warning("comment_response_parse_failed", error=str(exc))

        page.on("response", on_response)
        try:
            page.goto(video_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
            for text in _OVERLAY_DISMISS_TEXTS:
                try:
                    page.get_by_role("button", name=text, exact=False).first.click(timeout=1200)
                    break
                except Exception:
                    continue
            page.wait_for_timeout(500)
            try:
                page.get_by_text("Comments", exact=False).first.click(timeout=5000, force=True)
            except Exception as exc:
                logger.warning("comments_panel_not_opened", error=str(exc))

            while len(pages) < max_pages:
                before = len(pages)
                page.mouse.wheel(0, 700)
                page.wait_for_timeout(1200)
                if len(pages) == before and pages and not pages[-1].get("has_more"):
                    break
        finally:
            browser.close()

    return pages


class TikTokCommentsSpider(scrapy.Spider):
    name = "tiktok_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        video_id: str | None = None,
        video_url: str | None = None,
        max_pages: int = 5,
        dedupe: str = "true",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.video_id = video_id
        self.video_url = video_url
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

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

        pages = await asyncio.to_thread(_capture_comment_pages, self.video_url, self.max_pages)

        new_count = 0
        for page_response in pages:
            for comment in extract_comments(page_response):
                comment_id = comment["comment_id"]
                if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
                    continue
                new_count += 1
                await self._kafka.publish(
                    topic=RAW_COMMENTS_TOPIC,
                    key=f"tiktok:{comment_id}",
                    value={"platform": "tiktok", "video_id": self.video_id, **comment},
                )
                yield TikTokCommentItem(video_id=self.video_id, **comment)

        await self._kafka.stop()
        logger.info(
            "crawl_finished",
            telegram=True,
            video_id=self.video_id,
            pages=len(pages),
            new_comments=new_count,
            note="needs a real headful browser (Xvfb on a headless server) - see module docstring",
        )
