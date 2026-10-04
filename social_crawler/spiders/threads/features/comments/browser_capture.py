"""Điều khiển một trình duyệt Patchright thật qua permalink của một bài và cuộn, cố kích hoạt
DirectRepliesRefetchQuery. Đã tạm gác: threads_comments giờ GET
/api/v1/text_feed/<id>/replies/ thay vào (xem comments.py). Để lại đây vì phát lại refetch
GraphQL bằng curl_cffi vẫn trả về direct_replies: null, và lần tải permalink nguội không
bao giờ bắn query đó (BarcelonaPermalinkMobilePostColumnRoute khi đăng xuất dùng /ajax/bz).

Đã xác nhận bằng debug trực tiếp (2026-09-15) vì sao cái này hiện vẫn trả rỗng kể cả với
bài có hơn 1000 reply thật: một lần page.goto() nguội thẳng tới permalink của bài được
Threads phục vụ route đăng xuất/công khai riêng
(comet.barcelonawebloggedout.BarcelonaPermalinkMobilePostColumnRoute - thấy trong tham số
__crn của mọi request ajax), bất kể cookie của tài khoản này có hợp lệ - đây là Threads
phục vụ biến thể chia sẻ công khai, thân thiện SEO/xem trước của permalink, không phải
session hỏng (nội dung bài thật, kể cả reply, vẫn render trên route này - bản sửa phân
trang reply bằng cuộn bên dưới có nạp thêm reply vào DOM, đã xác nhận bằng một lần bắt
thật tăng từ 51 lên 75 reply được render khi cuộn). Cơ chế "tải thêm reply" của route đăng
xuất đó hoá ra là endpoint trang-một-phần kiểu BigPipe cũ hơn của Meta (/ajax/bz, GET)
thay vì một doc GraphQL - nó không bao giờ bắn POST DirectRepliesRefetchQuery nào, nên
listener response của hàm này không có gì để bắt dù cuộn đúng tới đâu.
DirectRepliesRefetchQuery có thật (đã xác nhận có trong bundle client của Threads) nhưng
chỉ được dùng bởi trải nghiệm SPA đã hydrate đầy đủ, đã đăng nhập - tới được bằng cách
điều hướng bên trong app khi đã xác thực (ví dụ bấm vào một bài từ feed/kết quả tìm kiếm
đã xác thực), không phải bằng page.goto() mới thẳng tới URL permalink. Sửa thật cần cách
điều hướng khác đó, không phải chỉnh thêm cuộn/selector - chưa được cài đặt.

Cùng loại vấn đề "cần trình duyệt thật, không phải phát lại có ký" như việc bắt video kênh
TikTok (spiders/tiktok/features/channel_videos), chỉ là sâu hơn một tầng ở đây (trình duyệt
thật lấy được *nội dung* thật, nhưng không nhất thiết là đúng các lời gọi mạng mà phần này
được xây quanh).

Dùng lại bộ máy xoay tài khoản/đăng nhập/proxy của threads/auth/bootstrap.py
(_get_authenticated_context) thay vì dựng một bộ riêng ở đây - cùng pool tài khoản, cùng
cách dùng lại proxy đã ghim/storage_state mà mọi luồng trình duyệt Threads khác (bootstrap
search, open_browser.py) vốn đi qua, nên cái này không cần cache bootstrap sẵn riêng như
đường comment curl_cffi cũ (xem crawl_request_consumer.py, vốn không còn liệt kê "threads"
trong COMMENTS_PLATFORMS_NEEDING_CACHE vì lý do này - tương tự spider comment TikTok cũng
không có trong tập đó, cùng lý do "tự chứa hoàn toàn theo từng lượt crawl")."""

from __future__ import annotations

import json
from typing import Any

from patchright.sync_api import sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.browser_utils import scroll_feed_to_bottom
from social_crawler.spiders.threads.auth.bootstrap import _get_authenticated_context
from social_crawler.spiders.threads.auth.request_capture import THREADS_GRAPHQL_URL_MARKERS
from social_crawler.spiders.threads.auth.triggers import comments_trigger
from social_crawler.spiders.threads.features.comments.extract import find_direct_replies_in_json

logger = get_logger(__name__)

# Cuộn liên tiếp mà không có trang reply mới nghĩa là reply của bài này đã hết (hoặc giao
# diện Threads ngừng phản hồi việc cuộn), không phải trục trặc tạm thời đáng chờ.
_MAX_STALL_SCROLLS = 5


def capture_reply_pages(post_url: str, max_pages: int = 20) -> list[dict[str, Any]]:
    """Mọi body response DirectRepliesRefetchQuery bắt được trong lúc cuộn permalink của
    `post_url`, theo thứ tự cuộn (trang 1 trước) - mỗi cái là một object JSON đã parse thô,
    chưa rút thành dict reply (xem extract.extract_replies_from_json, chỗ gọi gọi mỗi trang
    một lần). Hiện luôn rỗng - xem docstring module này để biết lý do (một lần tải permalink
    nguội không bao giờ bắn query này, không phải chuyện ngẫu nhiên theo bài hay theo tài
    khoản)."""
    pages: list[dict[str, Any]] = []
    checkpointed = False
    redis_cache = RedisCache()

    with sync_playwright() as pw:
        browser, context, page, account_key, account = _get_authenticated_context(pw, redis_cache, headless=True)

        def on_response(resp):
            nonlocal checkpointed
            if resp.request.method != "POST" or not any(marker in resp.url for marker in THREADS_GRAPHQL_URL_MARKERS):
                return
            try:
                body = resp.text()
            except Exception as exc:
                logger.warning("replies_response_read_failed", error=str(exc))
                return
            if not body:
                return
            # Đã xác nhận với một lần bắt thật: mọi POST GraphQL của một session bị checkpoint đều trả về
            # đúng body nhỏ xíu này (status 200, không phải 401/403 - tầng HTTP trông ổn) thay vì dạng
            # thật của query, ở *mọi* query mà trang bắn, không chỉ query này - một tình trạng của cả
            # session, không phải chuyện ngẫu nhiên theo query đáng thử lại.
            if '"checkpoint_required"' in body:
                checkpointed = True
                return
            # Một response có thể chứa nhiều hơn một object JSON phân cách bằng dòng mới (các query có
            # chú thích @stream của Relay làm vậy) - parse từng dòng thay vì giả định đúng một document,
            # cùng kiểu phòng thủ như lượt duyệt __bbox trên HTML của extract.py.
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if find_direct_replies_in_json(data) is not None:
                    pages.append(data)

        page.on("response", on_response)
        try:
            try:
                comments_trigger(post_url)(page)
            except Exception as exc:
                # Selector hỏng/timeout điều hướng không được làm crash cả spider - với chỗ gọi thì không
                # phân biệt được với "tài khoản này bắt được 0 trang", trường hợp mà nó vốn đã thử lại với
                # tài khoản khác (xem comments.py).
                logger.warning("comments_trigger_failed", post_url=post_url, error=str(exc))
                return pages

            stalled_scrolls = 0
            while len(pages) < max_pages and stalled_scrolls < _MAX_STALL_SCROLLS:
                before = len(pages)
                # Không dùng page.mouse.wheel() - đã xác nhận bằng thử trực tiếp (xem docstring của
                # scroll_feed_to_bottom, phát hiện lần đầu trên feed hashtag của TikTok) rằng một event lăn
                # chuột giả lập ở vị trí màn hình cố định hoàn toàn không di chuyển vùng cuộn thật của trang
                # này (một div lớn bên trong, không phải window/body - đã xác nhận bằng kiểm tra trực tiếp:
                # window.scrollY giữ 0 suốt). Cách này *có* nạp thêm reply được render trong DOM khi cuộn
                # (đã xác nhận: 51 -> 75 trong một lần bắt) - chỉ là không làm query cụ thể này bắn, xem
                # docstring module này để biết lý do.
                scroll_feed_to_bottom(page)
                page.wait_for_timeout(1500)
                if len(pages) == before:
                    stalled_scrolls += 1
                else:
                    stalled_scrolls = 0
        finally:
            page.remove_listener("response", on_response)
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()

    if checkpointed and not pages:
        # Cùng cách xử lý như account_disabled_logged_out_mid_session của
        # facebook/auth/bootstrap.py: một session trông đủ hợp lệ để bỏ qua đăng nhập mới
        # (need_login=False) vẫn có thể đã chết phía server, chỉ phát hiện ra khi một request thật
        # được gửi đi. Tắt ngay bây giờ (thay vì để nó ở trạng thái đã nhận cho vòng xoay sau đâm vào
        # cùng bức tường) mới là điều thực sự quan trọng ở đây - `pages` rỗng với một bài thật sự đã
        # hết reply là trường hợp bình thường, không hỏng, không được nhầm với chuyện này, đó là lý do
        # chỉ kích hoạt khi *cả hai* điều kiện cùng đúng.
        reason = f"Account {account_key!r} is checkpointed (every GraphQL response came back checkpoint_required)."
        if account is not None:
            pool.release_account("threads", account["id"], success=False, hard_failure=True, reason=reason)
            logger.error("account_disabled_checkpointed", telegram=True, platform="threads", account=account_key)

    return pages
