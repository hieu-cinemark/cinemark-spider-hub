"""
Spider danh sách video kênh/người dùng TikTok - điều khiển một trình duyệt Patchright
thật, headless* tới trang profile của một creator (https://www.tiktok.com/@<username>) và
đọc thẳng các response mạng /api/post/item_list/ của nó.

Đây là endpoint "lấy mọi video một kênh đã đăng" - khác với tiktok_hashtag_search (feed
video của một #hashtag, chỉ curl_cffi, không trình duyệt). ĐỪNG giả định ở đây cần trình
duyệt chỉ vì search/comments từng cần, hay curl_cffi là đủ chỉ vì khẳng định "cần trình
duyệt" của hashtag_search hoá ra sai - cả hai hoá ra đều riêng theo endpoint, đã xác nhận
bằng thử trực tiếp, không phải đặc tính chung của TikTok. Endpoint này đã được thử theo
cùng cách, độc lập:

Đã xác nhận là cần thiết bằng A/B test trực tiếp thực tế (2026-09-16), với một request
thật do người dùng bắt (device_id 7643007170271839760, một lời gọi /api/post/item_list/ của
một kênh, cookie/X-Gnarly/X-Dynosaur thật - không phải một dòng platform_accounts của
project này, dùng riêng để bài test không đốt một trong số ít tài khoản còn sống của pool):

  1. Phát lại nguyên văn qua curl_cffi (giả TLS Chrome): ra dữ liệu thật (itemList 16
     video) - bản bắt không bị cũ.
  2. Đúng URL đó, giống từng byte trừ việc xoá X-Dynosaur (hoặc thay bằng giá trị sai rõ
     ràng): 200 rỗng. Không gì khác thay đổi - cùng X-Gnarly thật, cùng thứ tự tham số,
     cùng mọi thứ.
  3. Phát lại ngay URL gốc chưa sửa vẫn chạy - loại trừ khả năng "danh tính này bị giới
     hạn rate giữa lúc test" như một cách giải thích khác cho kết quả rỗng ở bước 2.
  4. Riêng ra: dựng lại request từ dict kiểu STATIC_PARAMS của project này cộng một
     X-Gnarly mới tính ở local (qua signature/gnarly.py) - kể cả giữ nguyên X-Dynosaur thật
     đã bắt - CŨNG ra rỗng. Vậy khác hashtag_search, ở đây một request ký ở local không
     tương đương với request của trình duyệt ngay cả trước khi đụng tới X-Dynosaur.
     Project này không có bản cài đặt X-Dynosaur local nào chạy được (một lần thử dịch
     ngược trước đó, signature/dynasaur.py, đã bị xoá 2026-09-17 - chưa bao giờ được xác
     nhận với response thật, không được nối vào client nào) - nên endpoint này chỉ dùng
     được qua trình duyệt. Xem comment POST_ITEM_LIST_URL trong constants/tiktok.py cho
     cùng dấu vết ở một chỗ, kể cả ghi chú 2026-09-17 rằng kết luận này có từ trước khi
     phát hiện IP vùng VN (không phải curl_cffi/chế độ khách) mới là nguyên nhân thật của
     một lần báo động "chỉ qua trình duyệt" tương tự với endpoint hashtag - nên thử lại với
     proxy ngoài VN trước khi tin hoàn toàn.

Bản thân dạng response không cần logic trích xuất mới: các item itemList của một response
/api/post/item_list/ bắt được mang đúng các trường
(id/desc/video/author/stats/contents/challenges/...) như item /api/challenge/item_list/
của hashtag_search, nên features/channel_videos/extract.py chỉ re-export
extract_response/extract_video của hashtag_search thay vì viết lại.

* headless=True ở đây được mang sang từ tiktok_hashtag_search (ở đó đã xác nhận bằng thử
  trực tiếp rằng headless chạy giống hệt có giao diện cho /api/search/general/full/) -
  CHƯA được xác nhận lại độc lập cho endpoint NÀY, để khỏi tốn thêm một lượt chạy tài
  khoản+proxy thật chỉ để kiểm tra headless với có giao diện, ngoài câu hỏi curl_cffi mà
  module này thực sự được xây để trả lời. tiktok_comments từng cần có giao diện (xem
  docstring module của nó) cho một luồng nặng tương tác DOM (mở panel comment); luồng của
  spider này (cuộn lưới video của profile) về cấu trúc gần với tiktok_hashtag_search hơn
  tiktok_comments, đó là lý do headless là mặc định khởi đầu ở đây - nhưng nếu một lượt
  chạy thật bắt được 0 trang khi headless trong khi tự mở trình duyệt thì rõ ràng thấy
  video, hãy thử headless=False ở đây trước khi nghi ngờ thứ khác.

Chạy:
    scrapy crawl tiktok_channel_videos -a username="linzenguyen"

Chỉ ra output, không DB/Kafka (2026-09-25): khác mọi spider TikTok khác, spider này không
publish lên RAW_POSTS_TOPIC - video của một kênh được đọc thẳng từ
output/<spider_name>_<timestamp>.json (xem FEEDS trong settings.py), không lưu vào D1. Khử
trùng là một set thường trong lượt chạy, không phải SEEN_POSTS_KEY - key đó là dấu "đã
publish lên Kafka" dùng chung của hashtag_search/comments; sadd vào nó ở đây (trong khi
không bao giờ thực sự publish) sẽ âm thầm khiến hashtag_search bỏ qua một video nó chưa
publish, chỉ vì spider này tình cờ thấy nó trước.

Chưa nối vào phần điều phối của crawl_request_consumer.py - consumer đó định tuyến theo
text từ khoá (hashtag hay tìm kiếm tự do), và D1 chưa có khái niệm "kênh cần theo dõi" để
cái này móc vào. Chạy độc lập qua `scrapy crawl` cho tới khi/nếu có quyết định sản phẩm
đó.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import quote

import scrapy
from patchright.sync_api import sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.tiktok import POST_ITEM_LIST_URL
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.browser_utils import scroll_feed_to_bottom
from social_crawler.spiders.tiktok.auth.accounts import next_account
from social_crawler.spiders.tiktok.auth.cookies import build_storage_state_from_cookies
from social_crawler.spiders.tiktok.features.channel_videos.extract import extract_response
from social_crawler.spiders.tiktok.items import TikTokChannelVideoItem

logger = get_logger(__name__)

# Tổng số lần thử qua mọi tài khoản được xoay cho một crawl_request - cùng lý do như
# MAX_ACCOUNT_ATTEMPTS của tiktok_hashtag_search.
MAX_ACCOUNT_ATTEMPTS = 2

# Số lần cuộn liên tiếp được phép không bắt được response /api/post/item_list/ mới nào trước
# khi bỏ cuộc với session của tài khoản này - cùng giá trị/lý do như _MAX_STALL_SCROLLS của
# tiktok_hashtag_search (lưới của một kênh là cùng loại feed cuộn vô hạn).
_MAX_STALL_SCROLLS = 10


class TikTokAccountUnusableError(RuntimeError):
    """Hoàn toàn không có tài khoản tiktok đang bật nào (loại nào cũng vậy) để chạy lượt crawl
    này - không phải lỗi theo từng tài khoản, xem next_account()."""


class _ZeroPagesCaptured(Exception):
    """Tín hiệu điều khiển nội bộ cho vòng thử lại của start() - phiên trình duyệt của một tài
    khoản được xoay tới hoàn toàn không bắt được trang video kênh nào, khác với
    TikTokAccountUnusableError (không có tài khoản nào để thử). Cùng dạng với tín hiệu nội bộ
    của tiktok_hashtag_search/tiktok_comments."""


def _capture_channel_pages(
    username: str,
    max_pages: int,
    storage_state: dict | None,
    proxy: dict | None,
) -> list[dict[str, Any]]:
    """Mở trang profile của `username` trong trình duyệt thật và cuộn để thu tối đa max_pages
    response /api/post/item_list/ - xem docstring module này để biết lý do. storage_state (từ
    cookie của một tài khoản đã đăng nhập), khi có, nghĩa là mọi request mà JS trang của TikTok
    bắn đều được ký như session đã đăng nhập đó; None thì chạy với tư cách khách."""
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
                page.goto(f"https://www.tiktok.com/@{quote(username)}", wait_until="domcontentloaded", timeout=30000)
            except Exception as exc:
                # Cùng lý do như _capture_search_pages của tiktok_hashtag_search: một proxy chậm/quá tải
                # timeout ở đây không được làm crash cả spider - với chỗ gọi thì không phân biệt được với
                # "tài khoản/proxy này bắt được 0 trang", trường hợp nó vốn đã biết thử lại với tài
                # khoản/proxy khác.
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

    # Spider này tự điều khiển trình duyệt Patchright thay vì đi qua downloader của Scrapy, nên
    # robots.txt và downloader middleware không áp dụng ở đây.
    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        username: str = "",
        keyword_id: str | None = None,
        max_pages: int = 20,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.username = username.lstrip("@").strip()
        # Spider này không cần hiểu bên trong - chỉ truyền tiếp lên từng item được yield để cùng
        # dạng với mọi item nguồn video TikTok khác, dù ở đây không có gì publish nó đi đâu. Thường
        # là None với lượt crawl kênh (không có từ khoá nào điều khiển).
        self.keyword_id = keyword_id
        self.max_pages = int(max_pages)
        # Set thường trong lượt chạy, không phải SEEN_POSTS_KEY - xem docstring module để biết vì sao
        # đây không được là cache khử trùng Redis dùng chung.
        self._seen_video_ids: set[str] = set()
        self._post_count = 0
        self._failed_device_ids: set[str] = set()

    async def start(self):
        if not self.username:
            logger.error("missing_username", hint='scrapy crawl tiktok_channel_videos -a username="<a channel handle>"')
            return

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

    async def _crawl_with_fresh_account(self):
        """Một lần thử trọn vẹn: xoay sang tài khoản nào next_account() chọn tiếp (loại các device_id
        đã bắt được 0 trang trong lượt chạy này), điều khiển trình duyệt thật qua profile của
        self.username, rồi xử lý mọi trang nó bắt được."""
        redis_cache = RedisCache()
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
                # Chỉ trong lượt chạy - xem docstring module để biết vì sao đây không phải SEEN_POSTS_KEY:
                # các trang /api/post/item_list/ chồng nhau trong lúc cuộn có thể bắt lại cùng một video
                # nhiều lần, cái này chỉ giữ file output khỏi lặp lại nó.
                if video_id in self._seen_video_ids:
                    continue
                self._seen_video_ids.add(video_id)
                new_posts += 1
                self._post_count += 1
                yield TikTokChannelVideoItem(username=self.username, **video)

            logger.info("page_crawled", page=page_number, new_posts=new_posts, fetched=len(videos))
