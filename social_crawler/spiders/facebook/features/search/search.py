"""
Spider tìm kiếm Facebook không bao giờ mở trình duyệt: gọi thẳng endpoint GraphQL qua
curl_cffi (giả dấu vân tay TLS của Chrome), dùng token mà
`social_crawler.spiders.facebook.auth.bootstrap` đã cache.

Chạy:
    scrapy crawl facebook_search -a query="keyword"

max_pages mặc định 100 là trần an toàn, không phải mục tiêu - vòng lặp vốn tự dừng khi
Facebook báo không còn trang (has_next_page là False), nên trên thực tế hiếm khi chạm tới.
Truyền -a max_pages=N để giới hạn thấp hơn.

Truyền -a dedupe=false để tắt khử trùng giữa các lượt chạy (ví dụ để lấy lại bài đã thấy
ở lượt trước) - mặc định bật mỗi khi kết nối được Redis, và lặng lẽ quay về chỉ khử trùng
trong lượt chạy nếu không.

Truyền -a start_date=YYYY-MM-DD -a end_date=YYYY-MM-DD (phải đi cùng nhau) để chỉ lấy bài
tạo trong khoảng đó, dùng bộ lọc tìm kiếm "Ngày đăng" của Facebook - ví dụ:
    scrapy crawl facebook_search -a query="keyword" -a start_date=2026-08-01 -a end_date=2026-08-15

Facebook giới hạn số kết quả mà một query tìm kiếm trả về, dù phân trang tới đâu - nhưng
mỗi lần tìm có lọc `start_date`/`end_date` được đánh giá độc lập, nên quét cùng một query
qua nhiều khoảng ngày nhỏ sẽ vượt được giới hạn đó thay vì chạm nó một lần rồi dừng.
Truyền -a sweep_days=N để quét N ngày gần nhất (mới nhất trước) theo các khoảng
-a sweep_window_days=M ngày mỗi khoảng - ví dụ:
    scrapy crawl facebook_search -a query="keyword" -a sweep_days=30 -a sweep_window_days=3
Bỏ sweep_window_days (hoặc truyền 0) để tự tính độ rộng từ sweep_days - xem
_auto_window_days. Độ rộng cố định không co giãn được: sweep_days=85 với độ rộng cố định 3
ngày nghĩa là 29 khoảng, mỗi khoảng chịu bóp nhịp theo trang và khoảng nghỉ giữa các
khoảng riêng dù phần lớn chẳng ra gì mới với một lượt quét dài như vậy. Khi đặt thì cái
này ghi đè start_date/end_date. Giữa các khoảng (không phải giữa các trang trong một
khoảng - đó là việc của bóp nhịp riêng của graphql_client), spider nghỉ một khoảng thời
gian ngẫu nhiên - mặc định 5-20s, ghi đè bằng -a sweep_pause_min=N -a sweep_pause_max=N -
vì bắn 30 lần tìm kiếm riêng liên tiếp không nghỉ tự nó đã là kiểu mẫu của bot.

Các thực thể (Group/User/Hashtag/Photo/Video/... gói chung trong cùng response tìm kiếm)
mặc định được yield cùng với bài - truyền -a include_entities=false để chỉ lấy bài.
"""

from __future__ import annotations

import asyncio
import random
import sys
from datetime import date, timedelta
from typing import AsyncIterator, Iterator

import scrapy

from social_crawler.clients.kafka import RAW_POSTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache, enable_dedupe_cache
from social_crawler.constants.facebook import (
    MAX_CONSECUTIVE_EMPTY_NEW_PAGES,
    SEEN_ENTITIES_KEY,
    SEEN_POSTS_KEY,
    SEEN_POSTS_TTL_SECONDS,
)
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.error_alerts import note_transient_error
from social_crawler.spiders.facebook.auth.graphql_client import (
    CheckpointRequiredError,
    FacebookGraphQLClient,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    find_page_info,
)
from social_crawler.spiders.facebook.features.search.extract import extract_response
from social_crawler.spiders.facebook.items import FacebookEntityItem, FacebookPostItem
from social_crawler.spiders.search_query import build_search_query

logger = get_logger(__name__)


_TARGET_WINDOW_COUNT = 10
_MAX_AUTO_WINDOW_DAYS = 7


def _auto_window_days(sweep_days: int) -> int:
    """Chọn độ rộng khoảng giữ tổng số khoảng gần _TARGET_WINDOW_COUNT bất kể lượt quét dài bao
    nhiêu, thay vì độ rộng cố định không phụ thuộc sweep_days (mặc định cũ) - sweep_days=30
    vẫn ra 3 (30/10), khớp đúng mặc định cũ, nhưng sweep_days=85 ra khoảng 9 thay vì 29 khoảng
    mà độ rộng cố định 3 cần. Giới hạn ở _MAX_AUTO_WINDOW_DAYS: khoảng rộng hơn có nguy cơ âm
    thầm bỏ sót bài với từ khoá đông, vì Facebook giới hạn số kết quả mà một query có lọc ngày
    trả về dù phân trang tới đâu - khoảng hẹp hơn mới vượt được giới hạn đó (xem docstring
    module), và giới hạn đó không nới ra chỉ vì cả lượt quét dài hơn. Làm tròn lên (không làm
    tròn gần nhất) để một sweep_days không chia hết cho _TARGET_WINDOW_COUNT không bao giờ ra
    *dưới* mục tiêu - quét 14 ngày làm tròn xuống độ rộng 1 sẽ cần 14 khoảng thay vì 7 khoảng
    khi làm tròn lên độ rộng 2."""
    window_days = -(-sweep_days // _TARGET_WINDOW_COUNT)  # chia làm tròn lên
    return min(_MAX_AUTO_WINDOW_DAYS, max(1, window_days))


def _date_windows(sweep_days: int, window_days: int, anchor: date) -> Iterator[tuple[date, date]]:
    """Yield các khoảng ngày (start, end) phủ `sweep_days` ngày kết thúc ở `anchor` (tính cả
    anchor), khoảng mới nhất trước, mỗi khoảng rộng tối đa `window_days`. Dùng để quét vượt
    giới hạn kết quả mỗi query của Facebook - xem docstring module. `anchor` là `date.today()`
    cho lượt quét mặc định "N ngày gần nhất", hoặc một end_date trong quá khứ do chỗ gọi chọn
    - các khoảng luôn rơi đúng vào khoảng lịch thật thay vì đếm lùi từ hôm nay bất kể end_date
    được chọn là gì."""
    offset = 0
    while offset < sweep_days:
        window_end = anchor - timedelta(days=offset)
        window_start = anchor - timedelta(days=min(offset + window_days - 1, sweep_days - 1))
        yield window_start, window_end
        offset += window_days


class FacebookSearchSpider(scrapy.Spider):
    name = "facebook_search"

    # Spider này không bao giờ đi qua downloader của Scrapy (nó gọi thẳng curl_cffi để giả dấu
    # vân tay TLS của Chrome thật), nên robots.txt và downloader middleware không áp dụng ở đây.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        query: str = "test",
        keyword_id: str | None = None,
        count: int = 5,
        max_pages: int = 100,
        dedupe: str = "true",
        start_date: str | None = None,
        end_date: str | None = None,
        include_entities: str = "true",
        sweep_days: int = 0,
        sweep_window_days: int = 0,
        sweep_pause_min: float = 5.0,
        sweep_pause_max: float = 20.0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        # Chỉ lời gọi tìm kiếm gửi đi thật mới dùng cái này - bản thân self.query vẫn là từ khoá
        # trơn ở mọi chỗ khác (item Kafka, log, keyword_match) để việc khớp phía sau với text từ
        # khoá lưu trong D1 không bị ảnh hưởng. Xem docstring của build_search_query để biết vì sao
        # có cái này.
        self.search_query = build_search_query(query)
        # Spider này không cần hiểu bên trong - chỉ truyền tiếp lên Kafka ở mỗi bài được publish để
        # ingest consumer của cinemark-api tra movie_id/keyword_id trực tiếp thay vì khớp mờ theo
        # text query (không chắc duy nhất giữa các phim). Chỉ được đặt khi lượt crawl được kích hoạt
        # từ nút kích hoạt từ khoá/job hằng ngày của cinemark-api - None với một lần chạy tay
        # `scrapy crawl` tuỳ hứng.
        self.keyword_id = keyword_id
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        self.start_date = date.fromisoformat(start_date) if start_date else None
        self.end_date = date.fromisoformat(end_date) if end_date else None
        self.include_entities = str(include_entities).lower() not in ("false", "0", "no")
        self.sweep_days = int(sweep_days)
        # 0/bỏ trống - "tự động" - độ rộng cố định không co giãn theo sweep_days, xem
        # _auto_window_days. Truyền giá trị dương để ép một độ rộng cụ thể.
        sweep_window_days = int(sweep_window_days)
        self.sweep_window_days = _auto_window_days(self.sweep_days) if sweep_window_days <= 0 else sweep_window_days
        # Khoảng nghỉ giữa các khoảng quét (không phải giữa các trang trong một khoảng - bóp nhịp
        # riêng của graphql_client vốn đã lo việc đó): người thật nghỉ giữa các lần tìm kiếm riêng
        # thay vì bắn liên tiếp, và nó cũng rải một lượt quét dài ra nhiều hơn trong khoảng xoay IP
        # của proxy thay vì dồn qua như một đợt.
        self.sweep_pause_min = float(sweep_pause_min)
        self.sweep_pause_max = float(sweep_pause_max)
        self._cache: RedisCache | None = None
        # Dùng chung giữa các khoảng để cùng một bài/thực thể được hai khoảng chồng nhau trong cùng
        # lượt chạy đưa ra chỉ được yield một lần.
        self._seen_ids: set[str] = set()
        self._post_count = 0
        self._entity_count = 0
        self._kafka = KafkaPublisher()

    async def start(self):
        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            # Dùng lại RedisCache/connection của chính spider này thay vì để client tự mở thêm một cái
            # thứ hai, độc lập bên trong.
            client = FacebookGraphQLClient(redis_cache=self._cache)
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.facebook.auth.bootstrap --query "{self.query}"',
            )
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("facebook", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='facebook' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("facebook", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)

        # SessionExpiredError/RateLimitedError từ một khoảng được phép lan ra khỏi _crawl_window
        # (nó không tự bắt nữa) để đúng một try/except này huỷ *cả* lượt quét ở lỗi đầu tiên, thay
        # vì vòng ngoài mù quáng chuyển sang khoảng kế tiếp rồi lại đâm vào (và cảnh báo Telegram
        # về) cùng token chết/giới hạn rate ở mọi khoảng còn lại.
        try:
            if self.sweep_days > 0:
                anchor = self.end_date or date.today()
                windows = list(_date_windows(self.sweep_days, self.sweep_window_days, anchor))
                logger.info(
                    "sweep_enabled",
                    windows=len(windows),
                    sweep_days=self.sweep_days,
                    window_days=self.sweep_window_days,
                )
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
        except SessionExpiredError as exc:
            logger.error(
                "session_expired",
                error=str(exc),
                hint=f'python -m social_crawler.spiders.facebook.auth.bootstrap --query "{self.query}"',
            )
            sys.exit(1)
        except CheckpointRequiredError as exc:
            # _run() đã tắt tài khoản và gửi cảnh báo Telegram (xem comet_graphql_client.py) - chỉ cần
            # dừng lượt crawl ở đây.
            logger.error("checkpoint_required", error=str(exc))
            sys.exit(1)
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            note_transient_error("facebook", "rate_limited", self._cache)
            sys.exit(1)
        except NetworkError as exc:
            logger.error(
                "network_error",
                telegram=True,
                error=str(exc),
                hint="check connectivity to the platform_proxies row for platform='facebook' - "
                "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help.",
            )
            note_transient_error("facebook", "network_error", self._cache)
            pool.abort_spider_for_network_error(exc)
        finally:
            await self._kafka.stop()

        logger.info(
            "crawl_finished", telegram=True, posts=self._post_count, entities=self._entity_count, query=self.query
        )

    async def _crawl_window(
        self, client: FacebookGraphQLClient, start_date: date | None, end_date: date | None
    ) -> AsyncIterator[FacebookPostItem | FacebookEntityItem]:
        """Chạy vòng tìm kiếm có phân trang một lần cho một khoảng start_date/end_date (hoặc tìm
        kiếm toàn bộ lịch sử không lọc nếu cả hai là None), dừng khi Facebook báo không còn trang
        hoặc chạm max_pages."""
        window_label = f"{start_date}:{end_date}" if start_date else "unfiltered"
        cursor: str | None = None
        page = 1
        empty_new_streak = 0

        while True:
            # SessionExpiredError/RateLimitedError cố ý không bị bắt ở đây - nó lan lên start(), nơi
            # huỷ cả lượt quét ở lỗi đầu tiên thay vì khoảng này lặng lẽ kết thúc trong khi vòng ngoài
            # chuyển sang khoảng kế tiếp.
            if cursor is None:
                response = await asyncio.to_thread(client.search, self.search_query, self.count, start_date, end_date)
            else:
                response = await asyncio.to_thread(
                    client.search_next_page, self.search_query, cursor, self.count, start_date, end_date
                )

            posts, others = extract_response(response)

            new_posts = 0
            for post in posts:
                post_id = post.get("post_id")
                if not post_id or post_id in self._seen_ids:
                    continue
                self._seen_ids.add(post_id)
                # add_if_new() (key TTL theo từng id, không phải set sadd() vĩnh viễn) coi lại cùng
                # post_id là mới sau SEEN_POSTS_TTL_SECONDS - comments_count/reactions_count/shares_count
                # của một bài cứ thay đổi sau lần crawl đầu, nên khử trùng vĩnh viễn sẽ đóng băng các số đó
                # ở giá trị lần đầu thấy mãi mãi (cùng lý do như SEEN_POSTS_TTL_SECONDS của TikTok).
                is_new = not self._cache or self._cache.add_if_new(
                    f"{SEEN_POSTS_KEY}:{post_id}", SEEN_POSTS_TTL_SECONDS
                )
                if not is_new:
                    continue
                new_posts += 1
                self._post_count += 1
                await self._kafka.publish(
                    topic=RAW_POSTS_TOPIC,
                    key=f"facebook:{post_id}",
                    value={"platform": "facebook", "keyword_id": self.keyword_id, **post},
                )
                yield FacebookPostItem(query=self.query, **post)

            new_entities = 0
            if self.include_entities:
                for entity in others:
                    entity_id = entity.get("id")
                    if not entity_id or entity_id in self._seen_ids:
                        continue
                    self._seen_ids.add(entity_id)
                    if self._cache and self._cache.sadd(SEEN_ENTITIES_KEY, entity_id) == 0:
                        continue
                    new_entities += 1
                    self._entity_count += 1
                    yield FacebookEntityItem(query=self.query, **entity)

            if new_posts == 0:
                empty_new_streak += 1
            else:
                empty_new_streak = 0

            logger.info(
                "page_crawled",
                window=window_label,
                page=page,
                new_posts=new_posts,
                new_entities=new_entities,
                empty_new_streak=empty_new_streak,
            )

            if empty_new_streak >= MAX_CONSECUTIVE_EMPTY_NEW_PAGES:
                logger.info(
                    "keyword_skipped_no_new_posts",
                    telegram=True,
                    query=self.query,
                    keyword_id=self.keyword_id,
                    window=window_label,
                    pages=page,
                    empty_new_streak=empty_new_streak,
                    hint=f"{MAX_CONSECUTIVE_EMPTY_NEW_PAGES} consecutive pages with 0 new posts - stopping this keyword window",
                )
                break

            page_info = find_page_info(response)
            if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1
