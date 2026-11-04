"""
Các luồng Playwright riêng của Threads: điều khiển trang tìm
kiếm để bắn các request GraphQL mà request_capture.py lắng nghe. Các helper chung về
chuột/gõ phím/selector dự phòng lấy thẳng từ package auth của Facebook - không đoạn code
nào trong đó riêng của Facebook, xem docstring của nó.
"""

from __future__ import annotations

import random

from social_crawler.constants.threads import (
    COOKIE_CONSENT_BUTTON_SELECTORS,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    click_first,
    click_first_selector,
    human_wait,
    type_like_human,
)

logger = get_logger(__name__)


def dismiss_cookie_banner(page, timeout_ms: int = 3000) -> None:
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def search_trigger(query: str):
    def trigger(page):
        # Vào trang tìm kiếm trơn và gõ vào ô tìm kiếm (thay vì link thẳng tới /search?q=...) - đã
        # xác nhận với các lượt chạy thật rằng gõ + Enter mới thực sự bắn request GraphQL kết quả
        # tìm kiếm; riêng một URL link thẳng chỉ render trang trắng, không bắt được request nào.
        page.goto("https://www.threads.com/search", wait_until="domcontentloaded")
        human_wait(page, 1500, 1000)
        search_input = page.locator('input[type="search"]').first
        search_input.wait_for(state="visible", timeout=15000)
        # force=True: cùng vấn đề lớp phủ đang chạy animation như ô 2FA ở trên.
        search_input.click(force=True)
        type_like_human(search_input, query)
        human_wait(page, 1000, 500)
        search_input.press("Enter")
        human_wait(page, 1500, 1000)
        # Threads mặc định kết quả tìm kiếm là "Top" (xếp theo mức liên quan, cá nhân hoá theo đồ thị
        # mạng xã hội của chính tài khoản này) với tab "Recent" bên cạnh - đã xác nhận là nguyên nhân
        # khiến kết quả crawl khác với khi người tự tìm cùng query trên cùng tài khoản: tab nào đang
        # active lúc trigger này chạy là tab mà công thức query bắt được của bootstrap.py dùng lại
        # mãi về sau, và trước đây "Top" chưa bao giờ được chuyển sang "Recent" ở đây. Cố gắng hết
        # mức (không bắt buộc) vì selector/label chính xác chưa được xác nhận độc lập với một lần bắt
        # thật, cùng lưu ý như comments_trigger của module này bên dưới - chỉnh text/role ở đây nếu
        # một lượt chạy thật cho thấy cú bấm trượt mục tiêu.
        if not click_first(
            (page.get_by_role("tab", name=text) for text in ("Recent", "Gần đây", "Mới nhất")),
            timeout_ms=3000,
        ):
            click_first((page.get_by_text(text, exact=True) for text in ("Recent", "Gần đây", "Mới nhất")))
        human_wait(page, 1200, 800)
        # Cho danh sách kết quả ban đầu đủ thời gian mount hoàn toàn trước khi cuộn - cuộn quá sớm
        # sẽ rơi vào nội dung đã tải sẵn và không bao giờ chạm ngưỡng "tải thêm", nên request phân
        # trang BarcelonaSearchResultsRefetchableQuery không bao giờ bắn (đã xác nhận: bắt được ở
        # một lượt chạy và thiếu ở lượt khác với cùng code, khác biệt duy nhất là thời điểm).
        page.wait_for_timeout(4000)
        # Cuộn xa hơn và nghỉ lâu hơn trước vì cùng lý do - trigger tải thêm cần thực sự chạm gần đáy
        # danh sách đang tải, không chỉ di chuyển xuống một phần.
        for _ in range(6):
            page.mouse.wheel(0, random.randint(2500, 4000))
            page.wait_for_timeout(1500)

    return trigger


def comments_trigger(post_url: str):
    """Mở permalink của một bài và cuộn qua các reply của nó - khác bài video của Facebook, reply
    của một bài Threads render ngay trên cùng trang, không cần bước "mở comment"/sắp xếp riêng
    (cần xác nhận/chỉnh lại với một lần bắt thật, giống comments_trigger của Facebook đã phải
    lặp lại với giao diện thật trước đây)."""

    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        dismiss_cookie_banner(page)
        human_wait(page, 2000, 1000)
        for _ in range(6):
            page.mouse.wheel(0, random.randint(1500, 2500))
            human_wait(page, 1000, 800)

    return trigger
