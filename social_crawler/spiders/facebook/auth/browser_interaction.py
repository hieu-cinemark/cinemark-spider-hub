"""
Các helper tương tác Playwright chung, dùng chung cho mọi luồng trong package này (đăng
nhập, search trigger, comments trigger, ...) - không cái nào biết gì về DOM riêng của
Facebook, chỉ biết cách nhìn/thao tác bớt giống trình duyệt tự động trong khi đang dùng
một cái.
"""

from __future__ import annotations

import hashlib
import os
import random
import re
import sys
from pathlib import Path

from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Chỉ dùng cho file debug local (ảnh chụp màn hình), không bao giờ cho dữ liệu cache.
BASE_DIR = Path(__file__).resolve().parent

# Mọi phần tử đang hiển thị mà click_via_ai_fallback coi là ứng viên - cố ý cùng phạm vi
# rộng như các role mà click_first_via_js nhắm cộng thêm [aria-label] trơn, vì một nút chỉ
# có icon (đúng trường hợp sinh ra phần này - xem bản sửa nút comment Reels trong
# triggers.py) thường không mang role ARIA nào ngoài aria-label.
_AI_FALLBACK_CANDIDATE_SELECTOR = '[role="button"], [role="link"], [role="menuitem"], [aria-label]'
# Snapshot () => {...}: chỉ role/aria-label/text hiển thị của mọi phần tử khớp đang hiển
# thị, cố ý loại mọi thứ không có cả hai - Kira không có gì hữu ích để đánh giá mức liên
# quan của một phần tử không có tên truy cập hay text nào, và đưa nó vào chỉ đốt token của
# prompt (lớn, bị giới hạn rate ở gói miễn phí) mà không được lợi gì.
_AI_FALLBACK_SNAPSHOT_JS = f"""
    () => {{
        const nodes = document.querySelectorAll('{_AI_FALLBACK_CANDIDATE_SELECTOR}');
        const out = [];
        for (const el of nodes) {{
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 || rect.height === 0) continue;
            const label = el.getAttribute('aria-label') || '';
            const text = (el.innerText || '').trim().slice(0, 40);
            if (!label && !text) continue;
            out.push({{role: el.getAttribute('role') || el.tagName.toLowerCase(), label, text}});
        }}
        return out;
    }}
"""

# Ghi đè navigator.webdriver, tín hiệu tự động hoá phổ biến nhất mà các hệ thống phát hiện
# bot kiểm tra đầu tiên - nếu không thì Chromium mặc định của Playwright để nó là true trên
# mọi trang. Áp cho mọi context mới (đăng nhập hay chỉ bắt request), không chỉ auto-login,
# vì rẻ và vô hại.
_STEALTH_INIT_SCRIPT = "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"

# Một tập nhỏ các kích thước viewport desktop phổ biến - mọi tài khoản đăng nhập với đúng
# dấu vân tay 1366x768 tự nó đã là dấu hiệu dùng chung dấu vân tay giữa những thứ lẽ ra
# trông như các người dùng thật không liên quan.
_VIEWPORT_POOL = (
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1600, "height": 900},
    {"width": 1920, "height": 1080},
)


def _viewport_for(account_key: str | None) -> dict:
    """Chọn tất định theo tài khoản từ _VIEWPORT_POOL, không chọn ngẫu nhiên mới mỗi lần chạy -
    Facebook tin một dấu vân tay thiết bị *nhất quán* giữa các phiên hơn là bất kỳ độ phân
    giải cụ thể nào, nên viewport của một tài khoản không nên dao động giữa các lần bootstrap
    như khoảng cuộn/nhịp độ nên dao động. Không có account_key (luồng tay/ẩn danh) thì quay
    về kích thước cố định ban đầu."""
    if not account_key:
        return _VIEWPORT_POOL[0]
    digest = hashlib.sha256(account_key.encode()).hexdigest()
    return _VIEWPORT_POOL[int(digest, 16) % len(_VIEWPORT_POOL)]


# User-agent mặc định của mọi context trình duyệt = đúng UA mà curl_cffi impersonate="chrome" gửi ở các request
# crawl (comet_graphql_client.py, tiktok/client.py) - đo ngày 2026-10-07 với curl_cffi đang cài; nâng curl_cffi thì
# đo lại. Một session mà lúc lướt/làm mới token hiện ra là trình duyệt này còn lúc crawl là trình duyệt khác là tín hiệu
# gian lận - và trước ngày này trình duyệt headless còn tự khai "HeadlessChrome" cả trong UA lẫn Client Hints.
DEFAULT_DESKTOP_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
_ACCEPT_LANGUAGE = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"


def ua_override(user_agent: str) -> dict | None:
    """Tham số Emulation.setUserAgentOverride (CDP) cho một UA Chrome: chuỗi UA cộng Client Hints (sec-ch-ua,
    navigator.userAgentData) khớp đúng phiên bản/nền tảng của nó. new_context(user_agent=...) của Playwright chỉ
    đổi chuỗi UA - Client Hints vẫn khai "HeadlessChrome";v="151" (đã kiểm chứng), mâu thuẫn ngay với chính UA.
    None với UA không phải Chrome (Firefox/Safari): Client Hints của Chrome đi kèm UA đó cũng sai, nên để
    nguyên và chỗ gọi tự chịu."""
    match = re.search(r"Chrome/(\d+)", user_agent)
    if not match or "Edg/" in user_agent or "OPR/" in user_agent:
        return None
    major = match.group(1)
    if "Android" in user_agent:
        platform, platform_version, arch, nav_platform, mobile = "Android", "14.0.0", "", "Linux armv8l", True
    elif "Macintosh" in user_agent:
        platform, platform_version, arch, nav_platform, mobile = "macOS", "15.5.0", "arm", "MacIntel", False
    elif "Windows" in user_agent:
        platform, platform_version, arch, nav_platform, mobile = "Windows", "10.0.0", "x86", "Win32", False
    else:
        platform, platform_version, arch, nav_platform, mobile = "Linux", "", "x86", "Linux x86_64", False
    brands = [
        {"brand": "Chromium", "version": major},
        {"brand": "Not-A.Brand", "version": "24"},
        {"brand": "Google Chrome", "version": major},
    ]
    return {
        "userAgent": user_agent,
        "acceptLanguage": _ACCEPT_LANGUAGE,
        "platform": nav_platform,
        "userAgentMetadata": {
            "brands": brands,
            "fullVersionList": [{**b, "version": f"{b['version']}.0.0.0"} for b in brands],
            "fullVersion": f"{major}.0.0.0",
            "platform": platform,
            "platformVersion": platform_version,
            "architecture": arch,
            "model": "",
            "mobile": mobile,
            "bitness": "64",
            "wow64": False,
        },
    }


def _apply_ua_override(context, page, override: dict) -> None:
    try:
        context.new_cdp_session(page).send("Emulation.setUserAgentOverride", override)
    except Exception as exc:  # noqa: BLE001 - không đặt được thì trang vẫn chạy với UA mặc định, chỉ log
        logger.warning("ua_override_failed", error=str(exc))


def has_display() -> bool:
    """Máy này có mở được trình duyệt có giao diện không - luôn được trên macOS/Windows, trên
    Linux chỉ khi có màn hình X/Wayland (máy crawl systemd không có, và khởi chạy có giao
    diện ở đó crash ngay). Đăng nhập tự động chỉ mặc định có giao diện khi hàm này là True."""
    if not sys.platform.startswith("linux"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def new_context(browser, account_key: str | None = None, **kwargs):
    """browser.new_context() cộng một dấu vân tay desktop VN hợp lý (locale/timezone/viewport
    thay vì mặc định trống của Playwright) và bản vá navigator.webdriver ở trên - dùng cho mọi
    context package này tạo để cả đăng nhập lẫn bắt request headless đều trông như trình duyệt
    bình thường, không phải tự động hoá. Truyền account_key để viewport ổn định cho tài khoản
    đó qua các lần chạy (xem _viewport_for) thay vì mọi tài khoản dùng chung một kích thước gán
    cứng.

    User-agent: kwargs["user_agent"] nếu chỗ gọi truyền (UA lưu theo tài khoản, UA cố định của TikTok), không thì
    DEFAULT_DESKTOP_UA (= UA của request crawl). Client Hints khớp UA được đặt bằng CDP cho MỌI trang của context,
    kể cả tab/popup mở sau, qua sự kiện "page" - xem ua_override."""
    user_agent = kwargs.pop("user_agent", None) or DEFAULT_DESKTOP_UA
    context = browser.new_context(
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
        viewport=_viewport_for(account_key),
        user_agent=user_agent,
        **kwargs,
    )
    context.add_init_script(_STEALTH_INIT_SCRIPT)
    override = ua_override(user_agent)
    if override is not None:
        # Sự kiện "page" của API sync chạy trễ - với trang từ context.new_page() nó tới SAU lần goto đầu (đã kiểm
        # chứng: request đầu vẫn mang "HeadlessChrome"), nên new_page được bọc để đặt override ngay khi trang vừa
        # tạo. Sự kiện vẫn cần cho tab/popup do chính trang mở ra.
        original_new_page = context.new_page

        def new_page(*args, **kw):
            page = original_new_page(*args, **kw)
            _apply_ua_override(context, page, override)
            return page

        context.new_page = new_page
        context.on("page", lambda page: _apply_ua_override(context, page, override))
    return context


def find_first_visible(
    page, selectors: tuple[str, ...], label: str, debug_name: str, timeout_ms: int = 4000, required: bool = True
):
    """Thử lần lượt từng selector tới khi một cái khớp một phần tử đang hiển thị - Facebook đổi
    giao diện/ngôn ngữ/markup thường xuyên (id form đăng nhập giờ do React sinh lúc chạy, ví
    dụ "_r_2_", không ổn định), nên một selector gán cứng duy nhất dễ hỏng. Chụp màn hình và
    raise khi tất cả đều thất bại, trừ khi required=False - dùng giá trị đó cho phép kiểm tra
    tuỳ cơ hội (ví dụ "màn hình tuỳ chọn này có đang hiện không?") nơi không tìm thấy gì là kết
    quả bình thường, lặng lẽ, không phải lỗi đáng chụp màn hình."""
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout_ms)
            return locator
        except Exception:
            continue
    if not required:
        return None
    debug_path = BASE_DIR / f"{debug_name}.png"
    page.screenshot(path=str(debug_path))
    raise RuntimeError(
        f"Could not find {label} (Facebook may have changed its UI, shown a cookie-consent/checkpoint "
        f"screen, or a locale-specific variant - proxy/IP geolocation can trigger this). "
        f"Landed on: {page.url!r}. Saved a screenshot to {debug_path} for inspection."
    )


def click_first(locators, timeout_ms: int = 2000, force: bool = False) -> bool:
    """Thử bấm lần lượt từng locator tới khi một cái thành công - Facebook đổi text/markup nút
    qua các lần deploy/ngôn ngữ, nên một locator gán cứng duy nhất rất mong manh. Trả về có
    lần bấm nào thành công không; không bao giờ raise - một nút đơn giản là không hiện (ví dụ
    không có banner cookie, không có màn hình 2FA) là kết quả bình thường ở đây, không phải
    lỗi.

    force=True bỏ qua các phép kiểm tra khả năng thao tác của Playwright (hiển thị/ổn định/
    không bị che) - chỉ truyền cho locator đã được xác nhận trỏ đúng phần tử theo role+tên
    truy cập, khi lý do duy nhất khiến cú bấm thường thất bại là một lớp phủ không liên quan
    che đúng vị trí màn hình đó (xem bản sửa nút comment Reels trong triggers.py cho trường
    hợp sinh ra phần này)."""
    for locator in locators:
        try:
            locator.first.click(timeout=timeout_ms, force=force)
            return True
        except Exception:
            continue
    return False


def click_first_via_js(locators, timeout_ms: int = 3000) -> bool:
    """Giống click_first, nhưng gửi cú bấm bằng cách gọi .click() trực tiếp trên phần tử DOM đã
    xác định thay vì một cú bấm chuột Playwright thường - một cú bấm chuột thật (kể cả với
    force=True, vốn chỉ bỏ các phép kiểm tra trước khi bấm của Playwright) vẫn đi qua việc
    xác định phần tử theo toạ độ trên màn hình của trình duyệt, nên một lớp phủ không liên
    quan thực sự che pixel đó (đã xác nhận thực tế: thẻ lỗi widget chat Messenger của màn
    hình Reels nằm đè lên thanh hành động) nuốt mất cú bấm bất kể đặt tuỳ chọn Playwright gì.
    Gọi .click() trên chính phần tử bỏ qua hẳn việc xác định theo toạ độ - handler React của
    Facebook vẫn nhận được (listener uỷ quyền dựa vào target thật của event, không phải vị trí
    màn hình), đã xác nhận thực tế là thật sự mở được panel comment của reel trong khi
    force=True thì không."""
    for locator in locators:
        try:
            locator.first.wait_for(state="visible", timeout=timeout_ms)
            locator.first.evaluate("el => el.click()")
            return True
        except Exception:
            continue
    return False


def click_via_ai_fallback(page, goal: str, max_candidates: int = 40) -> bool:
    """Cú bấm phương án cuối khi mọi chiến lược selector gán cứng cho một tương tác giao diện đều
    đã thất bại - xem suggest_element_index trong clients/kira.py để biết đầy đủ lý do (phần
    này tồn tại riêng để giảm việc sửa tay selector mỗi lần DOM của Facebook thay đổi). Chụp
    snapshot role/tên truy cập/text của mọi phần tử tương tác đang hiển thị, hỏi Kira cái nào
    khớp `goal`, rồi bấm ĐÚNG phần tử đó bằng cách chạy lại y hệt bộ lọc/thứ tự và lấy theo
    chỉ số - Kira chỉ bao giờ chọn từ một danh sách phần tử đã tồn tại, kiểm chứng được; nó
    không bao giờ thấy hay tự nghĩ ra selector, nên chọn sai chỉ có thể là "bấm nhầm một thứ
    có thật", không bao giờ là "bấm thứ không tồn tại" hay crash vì markup Kira tưởng tượng.

    Trả về False (không bao giờ raise) khi có bất kỳ lỗi nào - Kira chưa cấu hình, bị giới
    hạn rate, nói không có ứng viên nào khớp, hoặc DOM đổi giữa hai lần snapshot - để chỗ gọi
    giữ đường lỗi/chụp màn hình sẵn có của mình cho trường hợp cái này cũng không ra gì, như
    trước khi có nó."""
    from social_crawler.clients.kira import suggest_element_index

    elements = page.evaluate(_AI_FALLBACK_SNAPSHOT_JS)[:max_candidates]
    if not elements:
        return False
    descriptions = [f"role={e['role']} aria-label={e['label']!r} text={e['text']!r}" for e in elements]
    index = suggest_element_index(goal, descriptions)
    if index is None:
        return False
    try:
        clicked = page.evaluate(
            f"""
            (i) => {{
                const nodes = document.querySelectorAll('{_AI_FALLBACK_CANDIDATE_SELECTOR}');
                const visible = [];
                for (const el of nodes) {{
                    const rect = el.getBoundingClientRect();
                    if (rect.width === 0 || rect.height === 0) continue;
                    if (!el.getAttribute('aria-label') && !el.innerText.trim()) continue;
                    visible.push(el);
                }}
                if (i >= visible.length) return false;
                visible[i].click();
                return true;
            }}
            """,
            index,
        )
    except Exception as exc:
        logger.warning("ai_selector_fallback_click_failed", goal=goal, error=str(exc))
        return False
    if clicked:
        logger.warning("ai_selector_fallback_used", goal=goal, chosen=descriptions[index])
    return bool(clicked)


def click_first_selector(page, selectors: tuple[str, ...], timeout_ms: int = 3000) -> bool:
    return click_first((page.locator(selector) for selector in selectors), timeout_ms=timeout_ms)


def click_first_by_role(page, texts: tuple[str, ...], role: str = "button", timeout_ms: int = 2000) -> bool:
    return click_first((page.get_by_role(role, name=text) for text in texts), timeout_ms=timeout_ms)


def human_wait(page, base_ms: int, jitter_ms: int) -> None:
    """Chờ base_ms cộng một khoảng ngẫu nhiên tối đa jitter_ms - cùng ý tưởng với
    MIN_REQUEST_INTERVAL_SECONDS/REQUEST_INTERVAL_JITTER_SECONDS trong graphql_client.py: một
    khoảng dừng đều tăm tắp giữa các thao tác tự nó đã là dấu hiệu bot, nên mọi khoảng giữa
    các thao tác Playwright nên thay đổi thay vì đúng một con số cố định mỗi lần chạy."""
    page.wait_for_timeout(base_ms + random.randint(0, jitter_ms))


def type_like_human(locator, text: str, min_delay_ms: int = 40, max_delay_ms: int = 180) -> None:
    """Gõ từng ký tự một với độ trễ ngẫu nhiên độc lập trước mỗi phím. `delay` của
    press_sequentially() áp một giá trị cố định cho mọi ký tự trong chuỗi, tự nó đã là một
    nhịp phát hiện được (tốc độ gõ thật thay đổi theo từng phím) - hàm này tái tạo sự biến
    thiên đó bằng cách gọi nó mỗi ký tự một lần thay vì mỗi chuỗi một lần."""
    for ch in text:
        locator.press_sequentially(ch, delay=random.randint(min_delay_ms, max_delay_ms))


def natural_scroll(
    page,
    min_scrolls: int,
    max_scrolls: int,
    min_px: int,
    max_px: int,
    pause_base_ms: int,
    pause_jitter_ms: int,
    backscroll_chance: float = 0.15,
) -> None:
    """Cuộn xuống một số lần ngẫu nhiên, mỗi lần một khoảng và một quãng nghỉ ngẫu nhiên, thỉnh
    thoảng chen một lần cuộn ngược lên ngắn - một vòng "cuộn N lần, mỗi lần đúng X px" cố định
    tự nó đã là một nhịp phát hiện được (con lăn chuột/trackpad thật thay đổi số lần, khoảng
    cách và nhịp độ, đôi khi cuộn quá rồi chỉnh lại). min_scrolls/min_px là mức sàn, không
    chỉ để cho có - chỗ gọi cần một lượng cuộn tối thiểu để kích hoạt việc tải trang tiếp (xem
    search_trigger/comments_trigger) nên giữ chúng ít nhất bằng mức đã xác nhận là chạy được."""
    for _ in range(random.randint(min_scrolls, max_scrolls)):
        page.mouse.wheel(0, random.randint(min_px, max_px))
        human_wait(page, pause_base_ms, pause_jitter_ms)
        if random.random() < backscroll_chance:
            page.mouse.wheel(0, -random.randint(150, 400))
            human_wait(page, 300, 400)


def move_mouse_naturally(page, locator) -> None:
    """Di con trỏ về phía `locator` qua hai bước với một quãng nghỉ ngắn giữa chúng, thay vì để
    .click() dịch chuyển tức thời nó thẳng tới tâm phần tử trong một cú nhảy - con trỏ thật
    tiến tới từ chỗ nó đang ở, không phải từ hư không. Lặng lẽ không làm gì nếu phần tử chưa
    có bounding box (không đáng làm hỏng cả thao tác vì chuyện đó)."""
    box = locator.bounding_box()
    if not box:
        return
    target_x = box["x"] + box["width"] / 2
    target_y = box["y"] + box["height"] / 2
    page.mouse.move(target_x + random.uniform(-150, 150), target_y + random.uniform(-100, 100))
    page.wait_for_timeout(random.randint(80, 220))
    page.mouse.move(target_x, target_y, steps=random.randint(5, 15))
