"""
Cào theo nguồn: bảng tin bài của một Page/Profile Facebook (fanpage phim, nhà phát hành, trang review) - 2026-10-16.

Tìm kiếm chỉ trả một phần bài và kéo về nhiều rác; bài trên trang chính chủ của phim gần như luôn liên quan. Gọi lại
query bảng tin mà bootstrap đã bắt (`bootstrap --page-url`, cache facebook:page_feed_query:{account}) với id của trang
cần cào - cùng phiên đăng nhập, bóp nhịp, xử lý lỗi và bộ trích bài với tìm kiếm (extract_response đọc được node Story
của bảng tin). Mới nhất trước; dừng khi hết trang, chạm max_pages, gặp bài cũ hơn since_days, hoặc nhiều trang liền
không có bài mới (đã cào lần trước).

Bài publish lên raw_posts kèm "source": "page" và "source_page" - ingest của cinemark-api coi bài từ trang nguồn của
phim là liên quan (không cần khớp từ khoá).

Sau bảng tin, spider đọc thêm tab Reels của trang (khi đã có query Reels bắt bởi bootstrap --page-reels-url) để lấy lượt
xem reel - thứ tìm kiếm / bảng tin không trả. Mỗi reel gửi một message "kind": "metrics" (chỉ post_id + view_count) lên
raw_posts; ingest chỉ cập nhật view_count của bài đã có. Phần này cố gắng hết mức: lỗi không làm hỏng lượt cào bảng tin.

Chạy thử (chỉ in bài, không gửi Kafka):
    scrapy crawl facebook_page_feed -a page_url="https://www.facebook.com/traibuonnguoi.movie.vn" -a dry_run=true
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator

import scrapy

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.facebook import SEEN_POSTS_KEY, SEEN_POSTS_TTL_SECONDS
from social_crawler.core.logger import get_logger
from social_crawler.runtime import pool
from social_crawler.spiders.common.error_alerts import note_transient_error
from social_crawler.spiders.common.post_log import log_crawled_post
from social_crawler.spiders.facebook.auth.graphql_client import (
    CheckpointRequiredError,
    FacebookGraphQLClient,
    NetworkError,
    RateLimitedError,
    RequestBudgetExceeded,
    SessionExpiredError,
    find_page_info,
)
from social_crawler.spiders.facebook.features.page_reels.extract import extract_reels
from social_crawler.spiders.facebook.features.search.extract import extract_response
from social_crawler.spiders.facebook.items import FacebookPostItem

logger = get_logger(__name__)

# Trang liền không có bài mới (đã cào lần trước) thì dừng - bảng tin mới nhất trước nên phần sau đã có. 29/10/2026: chỉ còn
# 2 tài khoản FB mà muốn theo dõi thêm nhiều trang - trang chung (không gắn phim) dừng ngay ở trang đầu không có bài mới,
# trang của phim chừa thêm một trang (bài ghim đầu bảng tin có thể đẩy bài mới sang trang 2).
MAX_PAGES_WITHOUT_NEW = 2
MAX_PAGES_WITHOUT_NEW_SHARED = 1
# Tab Reels: 10 reel/trang, mới nhất trước - 5 trang = 50 reel gần nhất (dừng sớm khi cả trang cũ hơn since_days).
REELS_MAX_PAGES = 5


class FacebookPageFeedSpider(scrapy.Spider):
    name = "facebook_page_feed"
    custom_settings = {"ROBOTSTXT_OBEY": False}  # noqa: RUF012 - cùng kiểu với các spider khác

    def __init__(
        self,
        page_url: str = "",
        page_id: str | None = None,
        keyword_id: str | None = None,
        max_pages: int = 50,
        since_days: float = 60,
        dedupe: str = "true",
        dry_run: str = "false",
        reels: str = "true",
        account: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.page_url = page_url.strip()
        self.account = (account or "").strip() or None
        self.page_id = (page_id or "").strip() or None
        self.keyword_id = keyword_id
        self.max_pages = int(max_pages)
        self.since_ts = time.time() - float(since_days) * 86400
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self.dry_run = str(dry_run).lower() in ("true", "1", "yes")
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()
        self._post_count = 0
        # Trang chung (keyword_id rỗng - ingest tự tìm phim theo nội dung) không cào Reels: lượt xem reel chỉ cần cho trang
        # của phim, và mỗi trang Reels là thêm tới 5 request trên tài khoản.
        self.shared_page = not keyword_id
        self.reels_enabled = str(reels).lower() not in ("false", "0", "no") and not self.shared_page
        self._reel_count = 0

    async def start(self):
        if not self.page_url and not self.page_id:
            logger.error(
                "missing_page", hint='scrapy crawl facebook_page_feed -a page_url="https://www.facebook.com/<page>"'
            )
            return
        if not self.dry_run:
            await self._kafka.start()
        if self.dedupe_enabled and not self.dry_run:
            self._cache = enable_dedupe_cache(logger)
        try:
            client = FacebookGraphQLClient(redis_cache=self._cache, account=self.account)
            if not self.page_id:
                self.page_id = await asyncio.to_thread(client.resolve_page_id, self.page_url)
                logger.info("page_id_resolved", page_url=self.page_url, page_id=self.page_id)
                if not self.page_id:
                    logger.error(
                        "page_id_not_found", telegram=True, page_url=self.page_url, hint="pass -a page_id=<numeric id>"
                    )
                    return
            async for item in self._crawl(client):
                yield item
            if self.reels_enabled:
                await self._crawl_reels(client)
        except RequestBudgetExceeded as exc:
            # Hết hạn mức request trong ngày (services/search_budget.py): dừng gọn, bài đã lấy vẫn giữ, không phải lỗi.
            logger.info("request_budget_exhausted", error=str(exc))
            return
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc), hint='bootstrap --page-url "<a page url>"')
            sys.exit(1)
        except CheckpointRequiredError as exc:
            logger.error("checkpoint_required", error=str(exc))
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("facebook", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error("network_error", telegram=True, error=str(exc))
            note_transient_error("facebook", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)
        finally:
            if not self.dry_run:
                await self._kafka.stop()
        if self._cache and not self.dry_run and self.page_url:
            # cinemark-api (source_pages._update_quiet_runs) giãn lịch cào trang không có bài mới.
            self._cache.set(
                f"page_feed_last:{self.page_url}",
                # page_id: cinemark-api lưu lại vào source_pages để lần sau không tốn request dò id của trang.
                {"new": self._post_count, "at": int(time.time()), "page_id": self.page_id},
                ttl_seconds=14 * 86400,
            )
        logger.info(
            "crawl_finished",
            posts=self._post_count,
            reels=self._reel_count,
            page_url=self.page_url,
            page_id=self.page_id,
            dry_run=self.dry_run,
        )

    async def _crawl_reels(self, client: FacebookGraphQLClient) -> None:
        """Lượt xem reel từ tab Reels (xem docstring module). Không có query Reels / trang không có tab Reels thì bỏ qua."""
        if not client.has_page_reels_query():
            logger.info("page_reels_skipped", reason="no_reels_query", page_url=self.page_url)
            return
        page_url = self.page_url or f"https://www.facebook.com/profile.php?id={self.page_id}"
        try:
            collection_id = await asyncio.to_thread(client.resolve_reels_collection_id, page_url)
            if not collection_id:
                logger.info("page_reels_skipped", reason="no_reels_tab", page_url=page_url)
                return
            cursor: str | None = None
            for page in range(1, REELS_MAX_PAGES + 1):
                response = await asyncio.to_thread(client.page_reels, collection_id, cursor)
                reels = extract_reels(response)
                fresh = [reel for reel in reels if not reel["timestamp"] or reel["timestamp"] >= self.since_ts]
                for reel in fresh:
                    if self.dry_run:
                        print(
                            f"reel {reel['post_id']} | {reel['play_count_reduced']} -> {reel['view_count']} | {reel['url']}"
                        )
                    elif not await self._kafka.publish(
                        topic=RAW_POSTS_TOPIC,
                        key=f"facebook:{reel['post_id']}",
                        value={"platform": "facebook", "kind": "metrics", **reel},
                    ):
                        continue
                    self._reel_count += 1
                logger.info("page_reels_crawled", page=page, fetched=len(reels), fresh=len(fresh))
                page_info = find_page_info(response)
                if reels and not fresh:
                    break  # cả trang cũ hơn since_days
                if not page_info or not page_info.get("has_next_page") or not page_info.get("end_cursor"):
                    break
                cursor = page_info["end_cursor"]
        except RateLimitedError, NetworkError, SessionExpiredError, CheckpointRequiredError:
            raise
        except Exception as exc:  # noqa: BLE001 - lượt xem reel là phần thêm, không được làm hỏng lượt cào bảng tin
            logger.warning("page_reels_failed", page_url=page_url, error=str(exc)[:300])

    async def _crawl(self, client: FacebookGraphQLClient) -> AsyncIterator[FacebookPostItem]:
        cursor: str | None = None
        without_new = 0
        for page in range(1, self.max_pages + 1):
            response = await asyncio.to_thread(client.page_feed, self.page_id, cursor)
            posts, _ = extract_response(response)
            new_posts = 0
            too_old = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id:
                    continue
                if (post.get("timestamp") or 0) and post["timestamp"] < self.since_ts:
                    too_old += 1
                    continue
                if self.dry_run:
                    print(
                        f"{post_id} | {post.get('author_name')} | reac {post.get('reactions_count')} cmt "
                        f"{post.get('comments_count')} share {post.get('shares_count')} | "
                        f"{(post.get('message') or '').replace(chr(10), ' ')[:100]}"
                    )
                    new_posts += 1
                    self._post_count += 1
                    continue
                if self._cache and not self._cache.add_if_new(f"{SEEN_POSTS_KEY}:{post_id}", SEEN_POSTS_TTL_SECONDS):
                    continue
                published = await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"facebook:{post_id}",
                    value={
                        "platform": "facebook",
                        "keyword_id": self.keyword_id,
                        "source": "page",
                        "source_page": self.page_url or self.page_id,
                        **post,
                    },
                )
                if not published:
                    if self._cache:
                        self._cache.delete(f"{SEEN_POSTS_KEY}:{post_id}")
                    continue
                new_posts += 1
                self._post_count += 1
                log_crawled_post(
                    logger,
                    platform="facebook",
                    post_id=post_id,
                    text=post.get("message"),
                    author=post.get("author_name"),
                    url=post.get("url"),
                    likes=post.get("reactions_count"),
                    comments=post.get("comments_count"),
                    shares=post.get("shares_count"),
                )
                yield FacebookPostItem(query=self.page_url, **post)
            without_new = 0 if new_posts else without_new + 1
            page_info = find_page_info(response)
            logger.info("page_crawled", page=page, fetched=len(posts), new_posts=new_posts, too_old=too_old)
            if posts and too_old == len(posts):
                break  # cả trang cũ hơn since_days - phần sau còn cũ hơn
            if without_new >= (MAX_PAGES_WITHOUT_NEW_SHARED if self.shared_page else MAX_PAGES_WITHOUT_NEW):
                break
            if not page_info or not page_info.get("has_next_page") or not page_info.get("end_cursor"):
                break
            cursor = page_info["end_cursor"]
