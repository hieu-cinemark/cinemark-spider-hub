"""
Các luồng Playwright riêng của Facebook: điền và gửi form đăng nhập, và điều khiển các
trang search/comments để bắn các request GraphQL mà request_capture.py lắng nghe.
"""

from __future__ import annotations

import re
from urllib.parse import quote

from social_crawler.constants.facebook import (
    COMMENT_OPEN_BUTTON_TEXTS,
    COMMENT_REPLY_TEXTS,
    COMMENT_SORT_NEWEST_TEXTS,
    COMMENT_SORT_TRIGGER_TEXTS,
    COMMENT_VIEW_REPLIES_PATTERN,
    COOKIE_CONSENT_BUTTON_SELECTORS,
    SEARCH_BOX_SELECTORS,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.facebook.auth.browser_interaction import (
    BASE_DIR,
    click_first,
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


def dismiss_cookie_banner(page, timeout_ms: int = 3000) -> None:
    """Bấm qua modal đồng ý cookie của Facebook nếu nó đang che trang - không làm gì (nhanh, lặng
    lẽ) nếu nó không bao giờ hiện, ví dụ một context dùng lại đã lưu sẵn lựa chọn đồng ý."""
    click_first_selector(page, COOKIE_CONSENT_BUTTON_SELECTORS, timeout_ms=timeout_ms)


def pass_consent_page(page, back_to: str) -> None:
    """Facebook đưa phiên mới / phiên đổi thiết bị sang trang đồng ý cookie TOÀN MÀN HÌNH
    (/privacy/consent/?flow=user_cookie_choice_v2) thay vì trang định mở - 31/10/2026 bootstrap của một tài khoản vừa
    nhập cookie dừng ở đó, không bắt được request nào và đánh dấu nhầm là đăng xuất. Bấm đồng ý (cùng các nút của modal
    cookie) rồi quay lại đúng trang định mở. Không ở trang đó thì không làm gì."""
    if "/privacy/consent" not in (page.url or ""):
        return
    logger.info("facebook_consent_page_seen", url=(page.url or "")[:200], back_to=back_to[:200])
    dismiss_cookie_banner(page, timeout_ms=8000)
    human_wait(page, 2500, 1500)
    if "/privacy/consent" in (page.url or ""):
        logger.warning("facebook_consent_page_not_passed", url=(page.url or "")[:200])
        return
    if page.url.rstrip("/") != back_to.rstrip("/"):
        page.goto(back_to, wait_until="domcontentloaded")
        human_wait(page, 1500, 1500)


def type_search(page, query: str) -> bool:
    """Gõ `query` vào ô tìm kiếm trên thanh trên cùng rồi nhấn Enter, như người thật. True khi đã tới một trang kết quả
    (/search/...). False khi không thấy ô tìm kiếm, hoặc Enter rơi vào một gợi ý trong dropdown (Page/Profile/Group đang
    được chọn sẵn - lý do trước đây bootstrap mở thẳng URL) - chỗ gọi tự quyết định có mở URL kết quả không."""
    box = find_first_visible(
        page, SEARCH_BOX_SELECTORS, "search box", "debug_search_box", timeout_ms=6000, required=False
    )
    if box is None:
        logger.warning("search_box_not_found", url=page.url)
        return False
    move_mouse_naturally(page, box)
    box.click()
    human_wait(page, 500, 700)
    type_like_human(box, query)
    # Người gõ xong thường liếc dropdown gợi ý một chút rồi mới Enter.
    human_wait(page, 900, 1300)
    box.press("Enter")
    try:
        page.wait_for_url(re.compile(r"/search/"), timeout=15000)
    except Exception:  # noqa: BLE001 - Enter vào gợi ý / trang chậm: chỗ gọi xử lý
        logger.info("typed_search_left_search", url=page.url)
        return False
    return True


def search_trigger(query: str):
    def trigger(page):
        # 27/10/2026: vào trang chủ, lướt feed, GÕ từ khoá vào ô tìm kiếm rồi bấm tab "Bài viết" - như người thật.
        # Trước đó điều hướng thẳng tới URL kết quả (né dropdown gợi ý: Enter lúc một gợi ý Page/Profile đang được chọn
        # đi tới gợi ý đó thay vì tìm kiếm), nhưng một phiên vừa nạp cookie mà việc đầu tiên là mở /search/posts/?q=...
        # dính checkpoint ngay (2 tài khoản liền, khác proxy). Dropdown vẫn được lo: rơi vào gợi ý thì mới mở URL kết quả
        # - lúc đó phiên đã có lịch sử lướt feed + gõ tìm kiếm.
        # /search/posts/ (không phải /search/top/): "Top" là cách xếp hạng cá nhân hoá, trộn người/page/group và có thể
        # xếp bài cũ tương tác cao lên trên bài mới - lượt crawl qua công thức đã cache sẽ trả bài khác với người tự tìm
        # rồi bấm tab lọc "Bài viết" (đã so hai bên). /search/posts/ chỉ có bài, khớp ý "tìm bài nhắc tới X".
        posts_url = f"https://www.facebook.com/search/posts/?q={quote(query)}"
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
        pass_consent_page(page, "https://www.facebook.com/")
        human_wait(page, 2500, 2500)
        natural_scroll(
            page, min_scrolls=2, max_scrolls=4, min_px=400, max_px=1000, pause_base_ms=1500, pause_jitter_ms=2000
        )
        human_wait(page, 1000, 2000)
        if type_search(page, query):
            human_wait(page, 1500, 1500)
            # Tab "Bài viết" tìm theo href, không theo chữ (giao diện theo ngôn ngữ tài khoản).
            tab = page.locator('a[href*="/search/posts"]').first
            try:
                move_mouse_naturally(page, tab)
                tab.click(timeout=5000)
                page.wait_for_url(re.compile(r"/search/posts"), timeout=15000)
            except Exception:  # noqa: BLE001 - không thấy tab: mở URL tab đó (đã ở trang kết quả của chính câu này)
                logger.info("search_posts_tab_not_clicked", url=page.url)
                page.goto(posts_url, wait_until="domcontentloaded")
        else:
            page.goto(posts_url, wait_until="domcontentloaded")
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


def page_feed_trigger(page_url: str):
    """Mở một Page/Profile và cuộn qua phần giới thiệu tới bảng tin để Facebook tải trang bài đầu và ít nhất một trang
    kế tiếp (query bảng tin + query phân trang của nó). Cuộn sâu hơn search vì cột giới thiệu/ảnh nằm trước bài viết."""

    def trigger(page):
        page.goto(page_url, wait_until="domcontentloaded")
        pass_consent_page(page, page_url)
        human_wait(page, 2000, 1000)
        natural_scroll(
            page, min_scrolls=7, max_scrolls=11, min_px=1400, max_px=2600, pause_base_ms=900, pause_jitter_ms=700
        )

    return trigger


def page_reels_trigger(page_url: str):
    """Mở tab Reels của một Page và cuộn để Facebook tải thêm reel (bắn ProfileCometAppCollectionReelsRenderer
    PaginationQuery) - trang đầu nằm sẵn trong HTML nên phải cuộn mới thấy query."""

    def trigger(page):
        page.goto(f"{page_url.rstrip('/')}/reels", wait_until="domcontentloaded")
        pass_consent_page(page, f"{page_url.rstrip('/')}/reels")
        human_wait(page, 2500, 1000)
        natural_scroll(
            page, min_scrolls=6, max_scrolls=9, min_px=1200, max_px=2200, pause_base_ms=1000, pause_jitter_ms=700
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
        # Phương án cuối, chỉ tới đây khi mọi chiến lược gán cứng ở trên đều đã thất bại: nhờ AI (Bee)
        # chọn đúng phần tử từ snapshot trực tiếp các phần tử tương tác của trang thay vì sửa tay
        # thêm một selector nữa mỗi lần Facebook xáo trộn markup này (xem suggest_element_index
        # trong clients/ai.py để biết đầy đủ lý do). Không làm gì (trả False, không exception) khi
        # AI chưa được cấu hình hoặc đang tắt (ai_is_enabled) - phần này thuần là bổ sung bên trên
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
        pass_consent_page(page, post_url)
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
        pass_consent_page(page, post_url)
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
