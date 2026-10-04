"""
Spider comment TikTok qua curl_cffi — cùng kiến trúc với tiktok_hashtag_search: tạo một
danh tính khách synthetic mới, ký từng request ở local (X-Gnarly + X-Dynosaur), phân trang
/api/comment/list/.

TRẠNG THÁI (2026-09-18): đường trình duyệt Patchright đã được thay sau khi một lần A/B thực
tế cho thấy:
  - comment/list chỉ có Gnarly → HTTP 200 + body rỗng (kể cả với danh tính lấy được
    item_list hashtag thành công).
  - Cùng request + get_X_Dynosaur tính ở local (signature/dynosaur.py) → comment thật, ổn
    định qua 5 danh tính mới / 2 video / phân trang.

item_list hashtag vẫn không dùng Dynosaur (sign_dynosaur=False). Comment luôn truyền
sign_dynosaur=True qua TikTokCommentClient.

Không xoay platform_accounts và không ghim proxy cố định — mỗi lần thử là một
TikTokCommentClient(synthetic=True) mới, giống hashtag_search. Body HTTP rỗng raise
TikTokBlockedError và thử lại với danh tính mới; cạn pool proxy thì thoát
PROXY_EXHAUSTED_EXIT_CODE để consumer xếp hàng lại.

Hai lỗ hổng về độ đầy đủ được vá ngày 2026-09-25 (người dùng báo "không đủ comment"):
  - max_pages mặc định là 5 * count=20 = trần cứng 100 comment, chạm tới bất kể has_more,
    và lời gọi tiến trình con của crawl_request_consumer.py không bao giờ ghi đè cái nào -
    mọi lượt chạy production đều từng chạm đúng trần này với bất kỳ video nào có >100
    comment cấp một. Nâng lên MAX_TOP_LEVEL_PAGES; vẫn có giới hạn (không vô điều kiện
    "cho tới khi has_more là false") để một video viral không chiếm hết pool proxy/tài
    khoản dùng chung.
  - Reply hoàn toàn chưa bao giờ được lấy - list_replies() của client.py đã có và đã được
    xác nhận chạy thực tế (2026-09-18, xem docstring của nó) nhưng không chỗ nào ở đây gọi
    nó. Mọi comment cấp một mà extract_comment tìm thấy đều mang reply_count
    (reply_comment_total thô); giờ được phân trang theo từng comment qua _fetch_replies bên
    dưới, cùng hợp đồng has_more/cursor như cấp một, giới hạn ở
    MAX_REPLY_PAGES_PER_COMMENT.

Chạy:
    scrapy crawl tiktok_comments -a video_id="7670822924022074645" \
        -a video_url="https://www.tiktok.com/@user/video/7670822924022074645"
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator

import scrapy

from social_crawler.clients.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE, SEEN_COMMENTS_KEY
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.error_alerts import note_transient_error
from social_crawler.spiders.tiktok.client import (
    TikTokBlockedError,
    TikTokCommentClient,
    TikTokNetworkError,
    TikTokRateLimitedError,
)
from social_crawler.spiders.tiktok.features.comments.extract import extract_comments
from social_crawler.spiders.tiktok.items import TikTokCommentItem

logger = get_logger(__name__)

# Cùng lý do như hashtag_search: mỗi lần thử là một lần rút synthetic độc lập (danh tính mới
# + lease proxy mới), không phải xoay platform_accounts. Số lần thử là
# tiktok_comments_max_attempts trong proxy_settings của dashboard (mặc định 8).
# Số comment mỗi trang — khớp với lần thăm dò Dynosaur thực tế đã trả về 20.
DEFAULT_COUNT = 20
# Nâng từ 5 (2026-09-25) - đó là trần 100 comment âm thầm chạm tới ở mọi video có nhiều
# comment cấp một hơn vậy, bất kể has_more, vì crawl_request_consumer.py không bao giờ ghi đè
# mặc định này. Vẫn có giới hạn, không vô điều kiện "lặp cho tới khi has_more là false" - một
# video viral có hàng chục nghìn comment không được giữ chân pool proxy/tài khoản dùng chung
# nhỏ của nền tảng này vô thời hạn.
MAX_TOP_LEVEL_PAGES = 50
# Số trang reply cho mỗi comment cấp một có reply - cùng lý do như trên, đặt nhỏ hơn vì nó
# nhân lên theo số comment cấp một thực sự có reply (reply_count > 0), không phải chi phí cố
# định theo video.
MAX_REPLY_PAGES_PER_COMMENT = 10


class TikTokCommentsSpider(scrapy.Spider):
    name = "tiktok_comments"

    # Không bao giờ đi qua downloader của Scrapy — curl_cffi ký trực tiếp.
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
                max_attempts = int(get_setting("tiktok_comments_max_attempts"))
                for attempt in range(1, max_attempts + 1):
                    try:
                        async for item in self._crawl_with_fresh_identity():
                            yield item
                        break
                    except TikTokBlockedError as exc:
                        if attempt < max_attempts:
                            logger.warning(
                                "blocked_retrying_with_different_account",
                                attempt=attempt,
                                max_attempts=max_attempts,
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
                # Mọi lần thử lại đều không nhận được response HTTP nào (xem docstring của
                # TikTokNetworkError) - vấn đề proxy/kết nối, không phải danh tính chết, nên phải thoát theo
                # cùng cách như pool.ProxyPoolExhaustedError (PROXY_EXHAUSTED_EXIT_CODE) thay vì rơi xuống
                # phần hoàn thành "crawl_finished" bình thường bên dưới. Đã xảy ra thật (2026-09-21): trước
                # khi có phần này, một proxy tồi giữa lúc tạo danh tính khiến crawl_request_consumer.py commit
                # job là "xong" với 0 comment thay vì xếp hàng lại - đúng kiểu "thành công lặng lẽ" mà mã
                # thoát này sinh ra để ngăn (xem docstring của pool.ProxyPoolExhaustedError).
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
        """Một lần thử: tạo khách synthetic, phân trang comment/list tới max_pages hoặc
        has_more=false. Raise TikTokBlockedError khi body rỗng để start() tạo lại danh tính."""
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
                data=data,
                comments=len(comments),
                has_more=data.get("has_more"),
                status_code=status_code,
            )

            # status khác 0 mà trang đầu không có comment thường nghĩa là bị chặn mềm / lọc theo vùng,
            # không phải video thật sự không có comment.
            if page_idx == 1 and not comments and status_code not in (0, None):
                raise TikTokBlockedError(f"comment/list status_code={status_code} with zero comments on page 1")

            for comment in comments:
                async for item in self._publish_comment(comment):
                    yield item
                if comment.get("reply_count"):
                    async for item in self._fetch_replies(client, comment):
                        yield item

            # status_code 0 + comment rỗng ở trang 1 là video thật sự không có comment (hoặc bị lọc),
            # không phải bị chặn — dừng gọn gàng.
            if page_idx == 1 and not comments and not data.get("has_more"):
                break

            if not data.get("has_more"):
                break
            next_cursor = data.get("cursor")
            if next_cursor is None or str(next_cursor) == str(cursor):
                break
            try:
                cursor = int(next_cursor)
            except TypeError, ValueError:
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
        """Phân trang mọi reply dưới một comment cấp một - cùng hợp đồng cursor/has_more như vòng
        cấp một ở trên, chỉ là giới hạn ở MAX_REPLY_PAGES_PER_COMMENT thay vì MAX_TOP_LEVEL_PAGES.
        Không bắt TikTokBlockedError, giống một trang cấp một - chỗ gọi của
        _crawl_with_fresh_identity vốn đã tạo lại danh tính mới và thử lại cả video khi gặp lỗi đó."""
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
                data=replies,
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
            except TypeError, ValueError:
                break
