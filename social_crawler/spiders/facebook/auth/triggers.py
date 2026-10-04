"""
Các luồng Playwright riêng của Facebook: điền và gửi form đăng nhập, và điều khiển các
trang search/comments để bắn các request GraphQL mà request_capture.py lắng nghe.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from urllib.parse import quote

import pyotp

from social_crawler.constants.facebook import (
    COMMENT_OPEN_BUTTON_TEXTS,
    COMMENT_REPLY_TEXTS,
    COMMENT_SORT_NEWEST_TEXTS,
    COMMENT_SORT_TRIGGER_TEXTS,
    COMMENT_VIEW_REPLIES_PATTERN,
    COOKIE_CONSENT_BUTTON_SELECTORS,
    LOGIN_BUTTON_TEXTS,
    LOGIN_EMAIL_SELECTORS,
    LOGIN_PASSWORD_SELECTORS,
    TWO_FA_CODE_SELECTORS,
    TWO_FA_CONTINUE_BUTTON_TEXTS,
    TWO_FA_PROMPT_TEXT_HINTS,
)
from social_crawler.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    BASE_DIR,
    click_first,
    click_first_by_role,
    click_first_selector,
    click_first_via_js,
    click_via_ai_fallback,
    find_first_visible,
    human_wait,
    move_mouse_naturally,
    natural_scroll,
    type_like_human,
)

logger = get_logger(__name__)


class MissingTotpSecretError(RuntimeError):
    """Facebook hiện màn hình nhắc mã 2FA cho một tài khoản có dòng platform_accounts không có
    totp_secret (cột '2fa') - một lỗ hổng cấu hình (chưa ai từng lấy secret app authenticator
    của tài khoản này), không phải bằng chứng bản thân tài khoản bị checkpoint/khoá.
    bootstrap.py không được disable_account() vì lỗi này như cách nó làm với lỗi thật "không
    có c_user vì lý do khác" - đã xác nhận một cách đau đớn: một tài khoản có mật khẩu hoàn
    toàn đúng đã bị tự động tắt chỉ vì ô mã 2FA không bao giờ được điền (không có secret để
    điền), và chuyện đó hiện ra y hệt một checkpoint thật."""


class TwoFactorPromptNotHandledError(RuntimeError):
    """Text trang Facebook khớp một gợi ý màn hình 2FA đã biết (xem TWO_FA_PROMPT_TEXT_HINTS)
    nhưng không selector/locator đã biết nào tìm được ô nhập mã thật - gần như chắc chắn
    Facebook lại đưa ra một biến thể markup mới cho màn hình này (đã xảy ra thật: một giao
    diện thẻ 2FA thiết kế lại hoàn toàn mà không selector nào trong TWO_FA_CODE_SELECTORS
    khớp, nên ô mã âm thầm không bao giờ được điền, nút Continue cứ bị vô hiệu, đăng nhập
    "thất bại" không có cookie c_user, và đường xử lý lỗi chung của chỗ gọi đã tắt một tài
    khoản hoàn toàn tốt vì tưởng là checkpoint thật - cùng loại chẩn đoán nhầm mà
    MissingTotpSecretError vốn chặn cho trường hợp "không có secret"). bootstrap.py cũng KHÔNG
    được disable_account() vì lỗi này - cần người xem ảnh chụp màn hình đã lưu và cập nhật
    selector/locator, không phải phạt tài khoản."""


def dismiss_cookie_banner(page, timeout_ms: int = 3000) -> None:
    """Bấm qua modal đồng ý cookie của Facebook nếu nó đang che trang - không làm gì (nhanh, lặng
    lẽ) nếu nó không bao giờ hiện, ví dụ một context dùng lại đã lưu sẵn lựa chọn đồng ý."""
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def auto_login(page, account: dict, code_provider: Callable[[], str | None] | None = None) -> None:
    """Điền và gửi form đăng nhập Facebook bằng một tài khoản đã lưu thay vì dừng chờ nhập tay.
    account["id"] là định danh đăng nhập (email/số điện thoại/username tuỳ cách tài khoản
    được thiết lập). code_provider được truyền tiếp cho submit_two_factor_code với tài khoản
    không có secret TOTP."""
    page.goto("https://www.facebook.com/login", wait_until="domcontentloaded")
    dismiss_cookie_banner(page)
    email_box = find_first_visible(
        page, LOGIN_EMAIL_SELECTORS, "the login email field", "debug_login", timeout_ms=15000
    )
    move_mouse_naturally(page, email_box)
    email_box.click()
    # Gõ từng ký tự với jitter theo từng phím (như search_trigger làm với ô tìm kiếm) thay vì
    # .fill(), thứ đặt giá trị tức thì mà không có event phím nào - một tín hiệu tự động hoá
    # mạnh hơn nhiều, khiến Facebook dễ thử thách lần đăng nhập bằng checkpoint hơn kể cả khi
    # mật khẩu đúng.
    type_like_human(email_box, account["id"])
    human_wait(page, 300, 400)
    password_box = find_first_visible(page, LOGIN_PASSWORD_SELECTORS, "the login password field", "debug_login")
    move_mouse_naturally(page, password_box)
    password_box.click()
    type_like_human(password_box, account["password"])
    human_wait(page, 400, 500)
    # Gửi kiểu "đeo cả thắt lưng lẫn dây đeo quần": Enter chạy với form submit gốc, nhưng form
    # render bằng React này có thể nuốt nó mà không gửi - nên thử thêm bấm nút đăng nhập theo
    # tên truy cập (khớp cả <button> thật lẫn <div role="button">, vì bản thiết kế lại hiện tại
    # không có name="login"/type="submit" ổn định). Việc một trong hai có thực sự chạy không
    # được chỗ gọi kiểm chứng bằng cách kiểm tra cookie c_user sau đó, không giả định ở đây.
    password_box.press("Enter")
    human_wait(page, 1000, 800)
    click_first_by_role(page, LOGIN_BUTTON_TEXTS)
    # Không phải "networkidle" - trang chủ Facebook giữ kết nối nền mở vô thời hạn
    # (websocket chat/thông báo, polling), nên "0 kết nối mạng trong 500ms" không bao giờ xảy
    # ra và cái này chỉ treo tới timeout 30s của Playwright dù trang đã thực sự tải xong.
    # "load" vốn bắn một lần và trả về ngay.
    page.wait_for_load_state("load")
    human_wait(page, 1000, 1000)

    submit_two_factor_code(page, account.get("2fa"), code_provider=code_provider)


def submit_two_factor_code(
    page, secret: str | None, timeout_ms: int = 6000, code_provider: Callable[[], str | None] | None = None
) -> bool:
    """Nếu Facebook đang hiện màn hình nhắc mã 2FA sau khi đăng nhập, sinh mã TOTP từ secret của
    tài khoản và gửi đi. Trả về False (lặng lẽ, không chụp màn hình) nếu màn hình không bao
    giờ hiện - phần lớn lượt chạy dùng lại session mà Facebook đã tin, nên đây là trường hợp
    thường gặp, không phải lỗi.

    Luôn kiểm tra màn hình nhắc kể cả khi `secret` là falsy (thay vì chỗ gọi bỏ qua hẳn hàm
    này như trước) - raise MissingTotpSecretError nếu màn hình CÓ hiện mà không có gì để
    điền, để trường hợp đó hiện ra riêng biệt thay vì âm thầm rơi xuống một lỗi "không có
    c_user" trơn vài dòng sau, không phân biệt được với checkpoint thật.

    Không có secret thì hỏi code_provider (khi có) lấy mã - ví dụ mã Facebook gửi vào hộp thư
    của tài khoản (xem social_crawler/auto_login/email_2fa.py). Nó chỉ được gọi khi màn hình
    nhắc thực sự đang hiện; None từ nó (không có gì tới kịp) được coi như không có secret."""
    code_box = find_first_visible(
        page, TWO_FA_CODE_SELECTORS, "the 2FA code field", "debug_2fa", timeout_ms=timeout_ms, required=False
    )
    if code_box is None:
        # TWO_FA_CODE_SELECTORS là danh sách CSS cố định - Facebook đã từng đưa ra ít nhất một biến
        # thể markup 2FA không khớp (một input có label nổi, không có name/autocomplete/aria-label/
        # placeholder nào khớp trong các selector đó). get_by_label/get_by_placeholder xác định tên
        # truy cập theo cách nó thực sự được nối (<label> liên kết, aria-labelledby, placeholder,
        # ...) thay vì đoán thêm một thuộc tính cố định nữa.
        for locator_factory in (
            lambda: page.get_by_label("Code", exact=False),
            lambda: page.get_by_label("Mã", exact=False),
            lambda: page.get_by_placeholder("Code", exact=False),
            lambda: page.get_by_placeholder("Mã", exact=False),
        ):
            try:
                candidate = locator_factory().first
                candidate.wait_for(state="visible", timeout=2000)
                code_box = candidate
                break
            except Exception:
                continue

    if code_box is None:
        page_text = page.inner_text("body").lower()
        if any(hint in page_text for hint in TWO_FA_PROMPT_TEXT_HINTS):
            debug_path = BASE_DIR / "debug_2fa_prompt_not_handled.png"
            page.screenshot(path=str(debug_path))
            raise TwoFactorPromptNotHandledError(
                "Facebook's page text matches a known 2FA-prompt hint, but no known selector/locator "
                f"could find the code input - Facebook likely changed this screen's markup again. "
                f"Saved a screenshot to {debug_path} for inspection."
            )
        return False

    logger.info("two_factor_prompt_detected")
    if secret:
        code = pyotp.TOTP(secret).now()
    elif code_provider is not None:
        code = code_provider()
        if not code:
            raise MissingTotpSecretError(
                "Facebook is asking for a 2FA code, this account has no totp_secret configured, and no "
                "code arrived in its email inbox in time - check the inbox/IMAP access, or save the "
                "account's authenticator-app secret (platform_accounts.totp_secret), then retry."
            )
    else:
        raise MissingTotpSecretError(
            "Facebook is asking for a 2FA code but this account has no totp_secret configured "
            "(platform_accounts.totp_secret) - set up (or re-view) Facebook's own Security Settings "
            "> Two-Factor Authentication (authenticator app) for this account and save the secret "
            "key shown there, then retry."
        )
    move_mouse_naturally(page, code_box)
    code_box.click()
    type_like_human(code_box, code)
    human_wait(page, 400, 400)
    click_first_by_role(page, TWO_FA_CONTINUE_BUTTON_TEXTS)
    # Không dùng wait_for_load_state("load"): đã xác nhận với một lượt chạy thật rằng thẻ 2FA
    # thiết kế lại này là một lần chuyển SPA trong trang (spinner riêng của Continue, không có
    # điều hướng/event load đầy đủ) - "load" resolve ngay vì document không bao giờ tải lại,
    # nên phép kiểm tra c_user của chỗ gọi chạy khi Facebook vẫn đang xác minh mã phía server và
    # một lần đăng nhập hoàn toàn hợp lệ bị đọc nhầm là thất bại. Kiểm tra định kỳ tới khi ô mã
    # thực sự biến mất (bằng chứng bước xác minh đã đi tiếp) thay vì đoán một độ trễ cố định là
    # đủ dài.
    try:
        code_box.wait_for(state="hidden", timeout=15000)
    except Exception:
        logger.warning("two_factor_code_field_still_visible_after_submit", timeout_ms=15000)
    human_wait(page, 1000, 1000)
    return True


def search_trigger(query: str):
    def trigger(page):
        # Điều hướng thẳng tới URL kết quả tìm kiếm thay vì gõ vào ô tìm kiếm rồi nhấn Enter. Cách
        # đó từng chạy, nhưng dropdown gợi ý của Facebook giờ có thể đang highlight một gợi ý
        # (Page/Profile/Group) vào lúc nhấn Enter, nên Enter điều hướng tới gợi ý đó thay vì gửi tìm
        # kiếm - âm thầm bỏ qua lời gọi GraphQL kết quả mà request_capture.py cần (xem
        # pick_initial_request trong request_capture.py, vốn sau đó lỗi "No search-results GraphQL
        # request was captured"). Đi thẳng tới URL né hoàn toàn dropdown.
        # /search/posts/ (không phải /search/top/): "Top" là cách xếp hạng theo thuật toán, cá nhân
        # hoá theo tài khoản của Facebook - nó trộn kết quả người/page/group và có thể xếp một bài
        # cũ hơn, tương tác cao lên trên một bài mới tinh cũng khớp, đó chính xác là lý do một lượt
        # crawl qua công thức đã cache này trả về bài khác với một người tự tìm cùng query rồi bấm
        # tab lọc "Bài viết" (đã xác nhận chỗ lệch bằng cách so hai bên). /search/posts/ là tab chỉ
        # có bài riêng của Facebook - cũng không hoàn toàn theo thời gian, nhưng giới hạn trong nội
        # dung bài thật thay vì một cách xếp hạng cá nhân hoá trộn nhiều loại, khớp với ý "tìm bài
        # nhắc tới X" ở đây.
        page.goto(f"https://www.facebook.com/search/posts/?q={quote(query)}", wait_until="domcontentloaded")
        human_wait(page, 1500, 1000)
        # cuộn xuống để ép Facebook tải trang kế tiếp, để ta bắt được cả một request
        # SearchCometResultsPaginatedResultsQuery thật. min_scrolls/min_px giữ ở mức sàn cũ của
        # vòng cố định (4 x 1400px) - đó là mức tối thiểu đã xác nhận thực sự kích hoạt việc tải
        # trang tiếp; chỉ số lần/khoảng cách/nhịp độ trên mức sàn đó được ngẫu nhiên hoá, cộng thỉnh
        # thoảng một lần cuộn quá rồi cuộn ngược lên.
        natural_scroll(
            page, min_scrolls=4, max_scrolls=7, min_px=1400, max_px=2600, pause_base_ms=700, pause_jitter_ms=600
        )

    return trigger


def _open_comments_sorted_newest(page) -> None:
    """Điều hướng permalink của một bài tới danh sách comment sắp xếp "Mới nhất" - dùng chung
    cho comments_trigger và replies_trigger bên dưới, vì lấy reply cần đúng cùng thiết lập đó
    (comment đã mở, đã sắp xếp) trước khi tìm được một comment có reply để mở rộng."""
    # URL /videos/ (trình phát Video Home) hoặc /reel/ hoàn toàn không hiện danh sách comment
    # cho tới khi bấm cái này - permalink bài thường đã mở sẵn comment, nên đây là cố gắng hết
    # mức (click_first lặng lẽ nuốt "không tìm thấy gì", giống dismiss_cookie_banner) thay vì
    # bắt buộc.
    opened = click_first((page.get_by_text(t, exact=False) for t in COMMENT_OPEN_BUTTON_TEXTS), timeout_ms=3000)
    if not opened:
        # Nút comment của Reels là nút chỉ có icon - text hiển thị của nó chỉ là số tương tác ("6",
        # "3,6K"), không bao giờ là chữ "Comment"/"Bình luận", thứ chỉ có trong aria-label - đã xác
        # nhận thực tế (2026-09-16) rằng get_by_text không bao giờ khớp nó, nên cả panel comment âm
        # thầm không bao giờ mở với bất kỳ reel nào, đó mới là nguyên nhân thật của
        # debug_comments_sort_trigger_not_found (nút sắp xếp mà hàm này tìm tiếp theo hoàn toàn chưa
        # có trong DOM, không phải selector nút sắp xếp bị đổi). Thử lại theo role/tên truy cập, thứ
        # xác định tốt các nút chỉ có aria-label.
        #
        # click_first_via_js, không phải click_first(force=True): cũng đã xác nhận thực tế rằng màn
        # hình reel có một thẻ lỗi widget chat Messenger không liên quan ("Không thể tải đoạn chat")
        # mà vùng chứa của nó che kín toàn bộ bounding box của thanh hành động - elementFromPoint tại
        # tâm nút comment trả về thẻ chat, không phải nút, nên một cú bấm chuột thật ở đó (kể cả với
        # force=True, vốn chỉ bỏ các phép kiểm tra trước khi bấm của Playwright, không bỏ việc xác
        # định theo toạ độ thật của trình duyệt) rơi vào thẻ chat và không làm gì. Gửi .click() trực
        # tiếp trên phần tử đã xác định bỏ qua hẳn việc xác định theo toạ độ - handler uỷ quyền của
        # React vẫn nhận vì nó dựa vào target thật của event, không phải vị trí màn hình - đã xác
        # nhận thực tế là thật sự mở được panel comment trong khi force=True thì không.
        #
        # reversed(): context của project này luôn là locale="vi-VN" (xem comment của
        # COMMENT_SORT_TRIGGER_TEXTS ở trên) - thử "Comment" trước sẽ đốt trọn timeout của nó, chắc
        # chắn thất bại, trước khi tới được locator thực sự có thể khớp. timeout_ms=8000, cao hơn
        # nhiều so với 3000ms thường của COMMENT_OPEN_BUTTON_TEXTS ở trên: đã xác nhận thực tế điều
        # này quan trọng - thanh hành động của reel (khác với của bài thường, có ngay trong DOM)
        # được gắn dần khi video buffer, và một lượt chạy thật chỉ với 3000ms ở đây thỉnh thoảng
        # thua cuộc đua đó kể cả với tài khoản có session hoàn toàn hợp lệ.
        opened = click_first_via_js(
            (page.get_by_role("button", name=t) for t in reversed(COMMENT_OPEN_BUTTON_TEXTS)), timeout_ms=8000
        )
    if not opened:
        # Phương án cuối, chỉ tới đây khi mọi chiến lược gán cứng ở trên đều đã thất bại: nhờ Kira
        # chọn đúng phần tử từ snapshot trực tiếp các phần tử tương tác của trang thay vì sửa tay
        # thêm một selector nữa mỗi lần Facebook xáo trộn markup này (xem suggest_element_index
        # trong clients/kira.py để biết đầy đủ lý do). Không làm gì (trả False, không exception) khi
        # Kira chưa được cấu hình (KIRA_ENABLED, mặc định tắt) - phần này thuần là bổ sung bên trên
        # các chiến lược ở trên, không bao giờ thay thế chúng.
        opened = click_via_ai_fallback(
            page, goal="Open this post's comment list (an icon-only button may show only a number, not text)"
        )
    if opened:
        human_wait(page, 1200, 800)

    # Mọi context project này tạo đều có locale="vi-VN" (xem browser_interaction.new_context),
    # nên Facebook hiển thị giao diện này bằng tiếng Việt ("Phù hợp nhất"/"Mới nhất"/"Phản
    # hồi") - một chuỗi cố định chỉ tiếng Anh ở đây chỉ timeout và không bao giờ bắn request
    # GraphQL comment (đã xảy ra thật).
    #
    # Cả hai bước bên dưới đều là cố gắng hết mức, không bắt buộc: đã xác nhận thực tế
    # (2026-09-16) rằng panel comment của Reels đơn giản là không có nút sắp xếp nào (khác với
    # permalink bài thường) - không phải selector bị đổi, mà là một giao diện thật sự khác, gọn
    # hơn cho loại nội dung này. Bộ khớp danh sách comment của request_capture.py
    # (pick_comments_request) không quan tâm request bắt được sắp theo thứ tự nào, và crawler
    # production thật (features/comments/comments.py) cũng không giả định "mới nhất trước"
    # (phân trang/khử trùng không phụ thuộc thứ tự) - nên thiếu nút sắp xếp không phải lý do để
    # làm hỏng cả lần bắt, cứ tiếp tục với thứ tự mặc định mà loại nội dung này đưa ra.
    if click_first((page.get_by_text(t, exact=False) for t in COMMENT_SORT_TRIGGER_TEXTS), timeout_ms=5000):
        human_wait(page, 600, 500)
        if not click_first(
            (page.locator('div[role="menuitem"]').filter(has_text=t) for t in COMMENT_SORT_NEWEST_TEXTS),
            timeout_ms=5000,
        ):
            logger.warning(
                "comment_sort_newest_menu_item_not_found",
                hint=f"tried {COMMENT_SORT_NEWEST_TEXTS} - continuing with whatever order was already showing",
            )
        else:
            human_wait(page, 1500, 1000)
    else:
        logger.warning(
            "comment_sort_control_not_found",
            hint=f"tried {COMMENT_SORT_TRIGGER_TEXTS} - this content type (e.g. Reels) may not expose a sort "
            "control at all; continuing with whatever default order it renders",
        )


def comments_trigger(post_url: str):
    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        human_wait(page, 2000, 1000)
        _open_comments_sorted_newest(page)

        reply_link = None
        for reply_text in COMMENT_REPLY_TEXTS:
            candidate = page.get_by_text(reply_text, exact=True).first
            if candidate.count() > 0:
                reply_link = candidate
                break
        box = reply_link.bounding_box(timeout=5000) if reply_link else None
        if box:
            page.mouse.move(box["x"], box["y"])
            # min_scrolls/min_px đã nâng lên (2026-09-16, từ mức sàn ban đầu 8 x 500px) - xem docstring
            # của _facebook_comments_cache_usable về việc cuộn chưa đủ trên một bài ít comment âm thầm
            # tạo ra một cache comment không bao giờ phân trang quá khoảng 2 comment cho bất kỳ bài nào.
            # Mức sàn ban đầu vốn đủ để *cuối cùng* chạm tới việc tải trang tiếp của Facebook trên bài
            # đông, nhưng đã xác nhận thực tế (2026-09-16) là thường không đủ: bootstrap cứ ra cache
            # comment không có phần `pagination` kể cả với bài có hàng trăm comment, buộc phải
            # bootstrap trình duyệt lại ở từng lần dùng tài khoản đó thay vì một lần mỗi TTL. Cuộn xa
            # hơn trước khi trigger này bỏ cuộc tăng khả năng chính lần bootstrap này chạm tới việc tải
            # trang 2 của Facebook thay vì phải trông vào một lần thử lại may mắn sau này.
            natural_scroll(
                page, min_scrolls=18, max_scrolls=25, min_px=800, max_px=1600, pause_base_ms=500, pause_jitter_ms=500
            )

    return trigger


def replies_trigger(post_url: str):
    """Giống comments_trigger, nhưng đi tiếp tới việc thực sự mở rộng reply của một comment (bấm
    "N phản hồi"/"N replies") thay vì chỉ cuộn qua - cú bấm đó là thứ bắn request GraphQL mà
    bootstrap.py cần bắt cho `--type replies` (xem pick_comments_request trong
    request_capture.py, dùng lại nguyên cho lần bắt này)."""

    def trigger(page):
        page.goto(post_url, wait_until="domcontentloaded")
        human_wait(page, 2000, 1000)
        _open_comments_sorted_newest(page)

        # Cuộn xa hơn nhiều so với mức sàn cũ 3-5x500-1000 (2026-09-16) - một comment có reply không
        # chắc là cái đầu tiên được render, và mục tiêu thật ở đây không chỉ là "tìm bất kỳ link
        # reply nào" (mức sàn cũ đã làm tốt việc đó) mà là "tìm một cái có chuỗi reply đủ lớn để
        # phân trang" - càng nhiều comment được nạp vào DOM, phép chọn theo số lượng bên dưới càng
        # có nhiều ứng viên.
        natural_scroll(
            page, min_scrolls=10, max_scrolls=15, min_px=700, max_px=1400, pause_base_ms=500, pause_jitter_ms=400
        )

        pattern = re.compile(COMMENT_VIEW_REPLIES_PATTERN, re.IGNORECASE)
        candidates = page.get_by_text(pattern).all()
        if not candidates:
            debug_path = BASE_DIR / "debug_no_replies_link_found.png"
            page.screenshot(path=str(debug_path))
            raise RuntimeError(
                f"Could not find a 'view replies' link (tried pattern {COMMENT_VIEW_REPLIES_PATTERN!r}) among "
                "this post's visible comments - either none of them have replies yet (try a post/URL where a "
                f"top-level comment clearly has replies), or Facebook changed this UI. Saved a screenshot to "
                f"{debug_path}."
            )
        # Chọn link có số reply cao nhất, không chỉ cái đầu tiên (2026-09-16) - phép chọn .first cũ
        # lấy chuỗi nào tình cờ render đầu tiên trong DOM, vốn cũng hay là chuỗi "2 phản hồi" như
        # chuỗi "200 phản hồi"; toàn bộ nội dung một chuỗi nhỏ nằm gọn trong một response, nên cú
        # bấm mở rộng của nó không bao giờ bắn request reply *có phân trang*, dù trigger này cuộn
        # trước đó thế nào. Cố gắng hết mức: ứng viên nào không parse được số lượng thì chỉ xếp
        # cuối thay vì huỷ cả lần bootstrap vì một dạng text bất ngờ.
        number_pattern = re.compile(r"\d+")

        def _reply_count(locator) -> int:
            try:
                match = number_pattern.search(locator.inner_text())
                return int(match.group()) if match else -1
            except Exception:
                return -1

        candidate = max(candidates, key=_reply_count)
        move_mouse_naturally(page, candidate)
        candidate.click()
        human_wait(page, 1500, 1000)

        # Cuộn cả chuỗi vừa mở rộng (2026-09-16) - Facebook nạp lười reply của một chuỗi đông theo
        # cùng cách với danh sách comment cấp một, nên cú bấm mở rộng duy nhất ở trên chỉ bao giờ
        # bắt được trang *đầu tiên* của chuỗi đó; đây là thứ thực sự cho nó cơ hội trả (và cho lần
        # bootstrap này cơ hội bắt) một lần tải trang reply tiếp theo thật.
        natural_scroll(
            page, min_scrolls=8, max_scrolls=12, min_px=500, max_px=1000, pause_base_ms=500, pause_jitter_ms=400
        )

    return trigger
