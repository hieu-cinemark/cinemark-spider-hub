"""
Các luồng Playwright riêng của Threads: điền và gửi form đăng nhập gốc của threads.com (ở
/login/ - form username+mật khẩu thường; "Continue with Instagram" cũng có ở đó nhưng
không được dùng ở đây, xem docstring của auto_login để biết lý do), và điều khiển trang tìm
kiếm để bắn các request GraphQL mà request_capture.py lắng nghe. Các helper chung về
chuột/gõ phím/selector dự phòng lấy thẳng từ package auth của Facebook - không đoạn code
nào trong đó riêng của Facebook, xem docstring của nó.
"""

from __future__ import annotations

import random

import pyotp

from social_crawler.constants.threads import (
    COOKIE_CONSENT_BUTTON_SELECTORS,
    LOGIN_BUTTON_TEXTS,
    LOGIN_EMAIL_SELECTORS,
    LOGIN_PASSWORD_SELECTORS,
    TWO_FA_CODE_SELECTORS,
    TWO_FA_CONTINUE_BUTTON_TEXTS,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    click_first,
    click_first_by_role,
    click_first_selector,
    find_first_visible,
    human_wait,
    move_mouse_naturally,
    type_like_human,
)
from social_crawler.spiders.facebook.auth.triggers import MissingTotpSecretError, TwoFactorPromptNotHandledError

logger = get_logger(__name__)


def dismiss_cookie_banner(page, timeout_ms: int = 3000) -> None:
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def auto_login(page, account: dict) -> None:
    """Điền và gửi form đăng nhập riêng của threads.com bằng một tài khoản đã lưu thay vì dừng
    chờ nhập tay. account["id"] là định danh đăng nhập (email/số điện thoại/username).

    Đăng nhập trực tiếp ở đây - thay vì ở instagram.com rồi bắc cầu sang - chỉ chạy với tài
    khoản đã tham gia Threads (đã chọn username, v.v.) ít nhất một lần trước đó, ví dụ bằng tay
    qua màn hình "Continue with Instagram". Một tài khoản Instagram mới tinh chưa từng đụng tới
    Threads thì chưa có đăng nhập threads.com riêng và cần làm bước tham gia một lần đó trước;
    hàm này không thử làm việc đó. Đã xác nhận với một lượt chạy thật: sau khi tham gia một
    lần, đăng nhập gốc này đặt ds_user_id/sessionid trên domain .threads.com ngay lập tức,
    hoàn toàn không cần vòng qua instagram.com."""
    page.goto("https://www.threads.com/login/", wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    email_box = find_first_visible(
        page, LOGIN_EMAIL_SELECTORS, "the login username field", "debug_login", timeout_ms=15000
    )
    move_mouse_naturally(page, email_box)
    email_box.click()
    type_like_human(email_box, account["id"])
    human_wait(page, 300, 400)
    password_box = find_first_visible(page, LOGIN_PASSWORD_SELECTORS, "the login password field", "debug_login")
    move_mouse_naturally(page, password_box)
    password_box.click()
    type_like_human(password_box, account["password"])
    human_wait(page, 400, 500)
    password_box.press("Enter")
    page.wait_for_load_state("load")
    human_wait(page, 1500, 1000)
    click_first_by_role(page, LOGIN_BUTTON_TEXTS)

    two_fa_secret = account.get("2fa")
    if two_fa_secret:
        submit_two_factor_code(page, two_fa_secret)

    page.wait_for_timeout(4000)

    # Cùng cơ chế chống chẩn đoán nhầm như triggers của facebook: màn hình 2FA vẫn còn mà chưa
    # có ds_user_id là lỗ hổng tự động hoá/cấu hình (chưa có secret, hoặc mã không qua được),
    # không phải tài khoản bị checkpoint - raise riêng biệt để chỗ gọi không tắt cứng một tài
    # khoản tốt.
    logged_in = any(c["name"] == "ds_user_id" for c in page.context.cookies())
    if not logged_in and find_first_visible(
        page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=2000, required=False
    ):
        if not two_fa_secret:
            raise MissingTotpSecretError(
                "Threads is asking for a 2FA code but this account has no totp_secret configured "
                "(platform_accounts.totp_secret) - save the account's authenticator-app secret, then retry."
            )
        raise TwoFactorPromptNotHandledError(
            "Threads' 2FA prompt is still showing after submitting a TOTP code - the code field/Continue "
            "button markup may have changed, or the stored totp_secret is wrong."
        )


def submit_two_factor_code(page, secret: str, timeout_ms: int = 8000) -> bool:
    """Nếu threads.com đang hiện màn hình nhắc mã 2FA sau khi đăng nhập, sinh mã TOTP từ secret
    của tài khoản và gửi đi. Trả về False (lặng lẽ, không chụp màn hình) nếu màn hình không bao
    giờ hiện - phần lớn lượt chạy dùng lại session mà Threads đã tin, nên đây là trường hợp
    thường gặp, không phải lỗi.

    Khác trang đăng nhập của Instagram, threads.com/login/ giữ ô username vẫn mount phía sau
    modal 2FA, nên selector input[type="text"] trơn khớp hai phần tử - ô mã được nhắm theo
    text placeholder riêng của nó (xem TWO_FA_CODE_SELECTORS)."""
    code_box = find_first_visible(
        page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=timeout_ms, required=False
    )
    if code_box is None:
        # Text placeholder đã đổi/được dịch khác với dự kiến - quay về tìm input[type="text"] nào còn
        # trống (cả ô username cũ lẫn ô mã đều khớp selector trơn, nhưng chỉ ô mã là trống lúc đầu).
        text_inputs = page.locator('input[type="text"]')
        try:
            text_inputs.first.wait_for(state="visible", timeout=2000)
        except Exception:
            return False
        for i in range(text_inputs.count()):
            candidate = text_inputs.nth(i)
            if candidate.input_value() == "":
                code_box = candidate
                break
    if code_box is None:
        logger.warning("two_factor_prompt_detected_but_no_empty_input_found")
        return False

    logger.info("two_factor_prompt_detected")
    code = pyotp.TOTP(secret).now()
    # force=True: một lớp phủ modal đang chạy animation thường xuyên chặn event con trỏ trên ô
    # này trong khoảng 1-2s đầu khi nó hiện (đã xác nhận với một lượt chạy thật) - một cú bấm
    # thường sẽ timeout khi chờ nó ổn định dù bản thân ô đã tương tác được.
    code_box.click(force=True)
    type_like_human(code_box, code)
    human_wait(page, 400, 400)
    click_first_by_role(page, TWO_FA_CONTINUE_BUTTON_TEXTS)
    page.wait_for_timeout(6000)
    return True


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
