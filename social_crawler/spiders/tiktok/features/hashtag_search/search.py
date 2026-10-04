"""
Spider tìm kiếm hashtag TikTok không bao giờ mở trình duyệt: gọi thẳng
/api/challenge/item_list/ qua curl_cffi (giả dấu vân tay TLS của Chrome), ký mọi request
ở local bằng một X-Gnarly mới tính (xem signature/gnarly.py). Giống
social_crawler.spiders.threads.features.search.search - xem docstring của module đó và
docstring module của client.py để biết đầy đủ lý do.

Chạy như một danh tính khách synthetic mới (TikTokHashtagClient(synthetic=True)) - hoàn toàn
không có dòng platform_accounts nào, xem docstring của client.py cho cơ chế. Cách này thay
thế một thiết kế xoay platform_accounts hoá ra gây hại ở đây: TikTok theo dõi tín hiệu lạm
dụng theo device_id, và một pool nhỏ tài khoản dùng lại cho nhiều lượt crawl cả ngày cuối
cùng đều cần một header X-Dynosaur thật (chỉ JS trình duyệt tính được, không có bản cài đặt
local nào chạy được) dù bản thân request không có gì sai. Đã xác nhận bằng A/B test trực
tiếp thực tế (2026-09-17): ba tài khoản sống lâu khác nhau, ba proxy khác nhau (kể cả hai
proxy chưa từng dùng cho TikTok trước hôm đó), đều nhận response rỗng với một X-Gnarly hoàn
toàn hợp lệ, vừa cấp - trong khi một bộ device_id/odinId/cookie synthetic sinh ra trong
cùng vài phút, trên đúng các proxy đó, chạy ngay lần đầu, mọi lần, không cần X-Dynosaur. Vậy
"chế độ khách đã chết" (kết luận trước đây ở đây) thực ra là "đúng vài danh tính dùng lại
này đã chết" - tạo cái mới cho mỗi lượt crawl né được cả loại lỗi đó thay vì phải quản lý
nó.

Dấu vết điều tra trước đó, đã bị phần trên thay thế nhưng giữ lại để làm bối cảnh: đã thử
một biến thể trình duyệt thật cho đường đã đăng nhập - nó có lấy được nhiều kết quả hơn đáng
kể cho mỗi hashtag khi JS của TikTok ký request, nhưng phân trang qua vài trang đầu phụ
thuộc vào trạng thái SPA nội bộ của TikTok tiến lên, thứ mà cuộn/tải lại từ bên ngoài không
điều khiển được ổn định - kết quả nhiễu hơn 2-5 lần và thường không hơn gì đường khách. Một
project bên thứ ba độc lập (TIKTOK-API.md của github.com/caixax/opentok) đâm vào thứ trông
như cùng bức tường "chỉ qua trình duyệt" và kết luận X-Dynosaur là bắt buộc tuyệt đối -
đúng với danh tính *dùng lại* (khớp phát hiện của project này ở trên), không đúng với danh
tính mới.

Khác Facebook/Threads, phân trang ở đây là cặp cursor/hasMore riêng của TikTok (không phải
page_info GraphQL), và không có query string - tên hashtag được phân giải một lần thành
challenge_id dạng số qua resolve_hashtag(), rồi mọi trang sau đó được lấy theo id đó.

Chạy:
    scrapy crawl tiktok_hashtag_search -a hashtag="holinhtrangsi"

Truyền -a dedupe=false để tắt khử trùng giữa các lượt chạy - mặc định bật mỗi khi kết nối
được Redis, lặng lẽ quay về chỉ khử trùng trong lượt chạy nếu không.

Hai việc spider này tự làm, ngoài việc crawl đúng hashtag được yêu cầu:

  - Thử lại với danh tính mới khi gặp TikTokBlockedError: vì mỗi lần thử vốn đã tạo một
    danh tính synthetic hoàn toàn mới (xem docstring của client.py), TikTokBlockedError ở
    đây nghĩa là lần rút này xui (proxy chập chờn, đua với thứ khác trên cùng IP), không
    phải vấn đề hệ thống - nên thay vì bỏ cuộc luôn, nó phân giải lại và chạy lại một lần
    với client mới. Video đã publish không bị publish lại ở lần thử lại (khử trùng
    SEEN_POSTS_KEY chặn chúng như mọi lần lặp khác), nên chi phí duy nhất của một lần thử
    lại thừa là thêm một lượt resolve_hashtag. Lỗi giới hạn rate/mạng không được xử lý như
    vậy - cả hai đều được ghi rõ là không phải vấn đề danh tính, nên danh tính mới cũng
    không giúp gì.

  - Hashtag liên quan để duyệt trên dashboard: các tag xuất hiện cùng (xem
    extract.top_related_hashtags) được lưu trong Redis dưới RELATED_HASHTAGS_KEY_TMPL khi
    lượt crawl có keyword_id, để người vận hành duyệt một bước nhảy BFS từ bảng từ khoá.
    Spider này không bao giờ tự xếp hàng lượt crawl tiếp theo.
"""

from __future__ import annotations

import asyncio
import sys
from collections import Counter
from collections.abc import AsyncIterator

import scrapy

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.kira import classify_hashtag_relevance
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
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
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.error_alerts import note_transient_error
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

# Tổng số lần thử qua mọi danh tính synthetic mới cho một crawl_request - không phải "có bao
# nhiêu tài khoản" (ở đây không còn pool tài khoản nào, xem docstring của client.py), chỉ là
# trần số lần một TikTokBlockedError đáng để thử lại trước khi chấp nhận lượt chạy này đang
# thất bại vì một lý do mà danh tính mới cũng không sửa được.
#
# Đã nâng hai lần ngày 2026-09-17: lần đầu 2->4, rồi 4->8 khi thử thực tế cho thấy tỉ lệ IP
# sạch thật của pool proxiestrust Mỹ gần 1/4 hoặc 1/5 hơn là ước tính 1/3 ban đầu
# (_MAX_COOLDOWN_WAIT_SECONDS của get_new_proxy vốn đã khiến mỗi lần thử chờ hết cooldown xoay
# vòng khoảng 90s của nhà cung cấp thay vì âm thầm chấp nhận một proxy DB đã biết là tồi, nên
# mỗi lần thử ở đây là một lần rút thực sự độc lập, không phải lần phí) - theo chỉ đạo rõ ràng
# của chính người dùng, ưu tiên "lượt crawl thực sự có dữ liệu" hơn tốc độ (khoảng cách giữa
# các từ khoá dài hơn thì không sao; một từ khoá âm thầm không ra gì thì không được). 8 lần rút
# độc lập với tỉ lệ sạch thận trọng 20% cho khoảng 83% khả năng có ít nhất một lần thành công
# mỗi crawl_request; mỗi lần rút thất bại tốn khoảng một lần chờ cooldown (khoảng 45-90s), nên
# một lượt chạy xui hoàn toàn có thể mất vài phút - chấp nhận được với những điều trên.
# Giờ là tiktok_hashtag_max_attempts trong proxy_settings của dashboard (mặc định 8).


class TikTokHashtagSearchSpider(scrapy.Spider):
    name = "tiktok_hashtag_search"

    # Spider này không bao giờ đi qua downloader của Scrapy (nó gọi thẳng curl_cffi để giả dấu
    # vân tay TLS của Chrome thật), nên robots.txt và downloader middleware không áp dụng ở đây.
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
        # Spider này không cần hiểu bên trong - chỉ truyền tiếp lên Kafka ở mỗi bài được publish,
        # giống keyword_id của facebook_search.
        self.keyword_id = keyword_id
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        # 0 với hashtag xếp hàng bằng tay; > 0 chỉ với một bước nhảy BFS đã được người vận hành duyệt
        # (xem crawl_request_consumer.py). Giới hạn ở BFS_MAX_PAGES.
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

        # Mọi thứ cần Kafka producer còn chạy đều nằm trong khối try này. Chip hashtag liên quan được
        # ghi gần cuối; dừng producer phải chờ tới sau đó (gọi publish() trên một KafkaPublisher đã
        # dừng sẽ treo).
        try:
            try:
                max_attempts = int(get_setting("tiktok_hashtag_max_attempts"))
                for attempt in range(1, max_attempts + 1):
                    try:
                        async for item in self._crawl_with_fresh_account():
                            yield item
                        break
                    except TikTokBlockedError as exc:
                        if attempt < max_attempts:
                            logger.warning(
                                "blocked_retrying_with_different_account",
                                attempt=attempt,
                                max_attempts=max_attempts,
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
        """Một lần thử trọn vẹn: tạo một danh tính khách synthetic hoàn toàn mới, phân giải
        self.hashtag với nó, rồi crawl mọi trang. Tách khỏi start() để một lần thử lại do
        TikTokBlockedError chạy lại cả quá trình này (danh tính mới, lời gọi resolve_hashtag mới)
        thay vì dùng lại một client gắn với danh tính vừa bị chặn - xem docstring của client.py để
        biết vì sao danh tính mới mới là cách sửa thật ở đây."""
        # to_thread: giờ bản thân __init__ làm I/O mạng chặn (tạo lease proxy mới, tạo cookie khách)
        # và có thể chặn tới khoảng 100s nếu phải chờ hết cooldown xoay vòng của proxiestrust (xem
        # docstring của proxy_provider.get_new_proxy) - không được chặn event loop mà Kafka
        # producer/các spider khác dùng chung.
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
        """Lưu các tag liên quan để duyệt trên dashboard. Không bao giờ publish crawl_requests - một cú
        bấm của người vận hành tạo từ khoá và xếp hàng bước nhảy kèm bfs_depth. Ngừng gợi ý tag ở
        BFS_MAX_DEPTH. Khi AI settings đang bật, các tag xuất hiện cùng chung chung bị bỏ."""
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
            response = await asyncio.to_thread(client.search_hashtag, challenge_id, cursor, self.count, self.hashtag)
            videos = extract_response(response)
            update_related_hashtag_counts(response, self._related_hashtag_counts, exclude_ids={challenge_id})

            new_posts = 0
            for video in videos:
                video_id = video.get("video_id")
                if not video_id:
                    continue
                video_id = str(video_id)
                # Giá trị trả về của sadd() vốn đã trả lời "cái này có mới không" trong một lượt nguyên tử -
                # không cần kiểm tra sismember riêng (và không có cuộc đua giữa lần kiểm tra và lần add sau
                # đó).
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
