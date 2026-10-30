"""
Dấu vân tay trình duyệt dùng chung - MỘT nguồn sự thật duy nhất cho cả hai đường đi của
một phiên Facebook/Threads:

  1. Đường TRÌNH DUYỆT (patchright/Chromium thật): dùng để đăng nhập và bắt request
     -> UA + Client Hints (sec-ch-ua*) đặt qua CDP.
  2. Đường HTTP REPLAY (curl_cffi): dùng để phát lại request đã bắt
     -> impersonate=<target> (dấu vân tay TLS/HTTP2) + UA + Client Hints.

Hai đường này PHẢI khai cùng một trình duyệt. Facebook ràng buộc cookie phiên
(c_user/xs/datr) với dấu vân tay đã tạo ra nó: đăng nhập bằng Chromium 151 rồi phát lại
bằng dấu vân tay TLS của Chrome 146 trông y hệt "cookie bị nhân bản" - đúng dạng tín hiệu
mà checkpoint nhắm vào.

Sự lệch trước đây (đo 2026-10-27):
  - patchright 1.62.1 đóng gói Chromium 151.0.7922.34 (browsers.json của wheel) nhưng
    DEFAULT_DESKTOP_UA khai Chrome/146 -> trình duyệt nói dối phiên bản của chính nó.
  - impersonate="chrome" là ALIAS đổi nghĩa theo từng bản curl_cffi: 0.16 -> chrome146,
    0.17 -> chrome152 (Windows!). Nâng curl_cffi mà không ghim target tường minh là tự tạo
    lệch mới.
  - UA của browser là vi-VN (new_context đặt locale) nhưng curl_cffi mặc định gửi
    accept-language: en-US,en;q=0.9, và accept-language KHÔNG nằm trong
    STATIC_HEADER_FIELDS nên request replay không ghi đè nó -> hai đường lệch locale.

Cách dùng:
  - comet_graphql_client.py: impersonate=<target của profile tài khoản>
  - browser_interaction.py: new_context() dùng user_agent + ua_override_for_account()
  - post_metrics.py / proxy_health_check.py / nurture_accounts.py: target mặc định
"""

from __future__ import annotations

import hashlib
import re

# Phiên bản Chrome mà patchright đóng gói (Chromium 151.0.7922.34) - đổi con số này thì phải
# nâng patchright VÀ nâng curl_cffi tới bản có target tương ứng, rồi chạy lại
# tests/test_fingerprint_consistency.py (test đó tự so với header curl_cffi gửi thật).
CHROME_MAJOR = 151
CHROME_FULL_VERSION = "151.0.7922.34"

# Target của curl_cffi phải khớp CHROME_MAJOR + hệ điều hành. "chrome151_macos" (curl_cffi
# >= 0.17.0) là cặp macOS của 151; KHÔNG dùng "chrome151" (cùng version nhưng là Windows) và
# KHÔNG dùng alias "chrome" (đổi nghĩa theo từng bản curl_cffi).
CURL_CFFI_IMPERSONATE_TARGET = "chrome151_macos"

# UA của trình duyệt thật. Chrome trên desktop đóng băng version trong UA thành
# major.0.0.0 (không phải build đầy đủ) - đây đúng là chuỗi curl_cffi chrome151_macos gửi.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
    f"Chrome/{CHROME_MAJOR}.0.0.0 Safari/537.36"
)

# Client Hints đo được từ chính curl_cffi chrome151_macos (probe tới server local, 27/10/2026):
#   sec-ch-ua: "Not=A?Brand";v="99", "Google Chrome";v="151", "Chromium";v="151"
# Thứ tự brand KHÔNG giống Chrome 146 ("Chromium" đứng đầu) - nên phải đo theo từng version,
# không tự chế. Giá trị này được dùng cho cả CDP override (đường trình duyệt) lẫn header
# gửi đi (đường replay), nên hai đường luôn khớp nhau.
SEC_CH_UA = f'"Not=A?Brand";v="99", "Google Chrome";v="{CHROME_MAJOR}", "Chromium";v="{CHROME_MAJOR}"'

# Brand GREASE (thành phần "Not=A?Brand" / "Not-A.Brand" / ...) và THỨ TỰ brand đổi theo
# từng phiên bản Chrome - đây là lý do phải đo chứ không đoán. Bảng dưới đây là kết quả
# probe tới server local với curl_cffi 0.17.0 (27/10/2026), dùng cho các tài khoản có
# User-Agent riêng (import cookie) - chúng có thể khai một phiên bản khác CHROME_MAJOR.
SEC_CH_UA_BY_MAJOR: dict[str, tuple[str, ...]] = {
    "142": ("Chromium", "Google Chrome", "Not_A Brand"),
    "145": ("Not:A-Brand", "Google Chrome", "Chromium"),
    "146": ("Chromium", "Not-A.Brand", "Google Chrome"),
    "150": ("Not;A=Brand", "Chromium", "Google Chrome"),
    "151": ("Not=A?Brand", "Google Chrome", "Chromium"),
    "152": ("Chromium", "Not?A_Brand", "Google Chrome"),
}

# Giá trị version của brand GREASE cho từng phiên bản (đo cùng lần probe).
_GREASE_VERSION_BY_MAJOR: dict[str, str] = {
    "142": "99",
    "145": "99",
    "146": "24",
    "150": "8",
    "151": "99",
    "152": "24",
}

# Brand của các nền tảng khác, dùng khi một tài khoản có UA riêng (cookie import).
_CHROME_BRANDS_BY_PLATFORM = {
    "macOS": (f'"Not=A?Brand";v="99", "Google Chrome";v="{CHROME_MAJOR}", "Chromium";v="{CHROME_MAJOR}"',),
    "Windows": (f'"Not=A?Brand";v="99", "Google Chrome";v="{CHROME_MAJOR}", "Chromium";v="{CHROME_MAJOR}"',),
    "Linux": (f'"Not=A?Brand";v="99", "Google Chrome";v="{CHROME_MAJOR}", "Chromium";v="{CHROME_MAJOR}"',),
    "Android": (f'"Not=A?Brand";v="99", "Google Chrome";v="{CHROME_MAJOR}", "Chromium";v="{CHROME_MAJOR}"',),
}

# sec-ch-ua-full-version-list: Chrome đưa build đầy đủ vào đây (vẫn giữ major.0.0.0 cho
# brand GREASE). Thứ tự brand đi theo đúng thứ tự của sec-ch-ua. Giá trị đo được từ
# chính trình duyệt (probe 27/10/2026) - khớp với CHROME_FULL_VERSION là build Chromium
# mà patchright thực sự đóng gói.
SEC_CH_UA_FULL_VERSION_LIST = (
    f'"Not=A?Brand";v="99.0.0.0", "Google Chrome";v="{CHROME_FULL_VERSION}", "Chromium";v="{CHROME_FULL_VERSION}"'
)

# sec-ch-ua-platform-version: macOS của trình duyệt (đo 27/10/2026).
SEC_CH_UA_PLATFORM_VERSION = "15.5.0"

# Accept-Language: browser của project này luôn chạy locale vi-VN (new_context đặt
# locale="vi-VN"), nhưng curl_cffi mặc định gửi "en-US,en;q=0.9" và không có cách nào
# ghi đè nếu không nằm trong STATIC_HEADER_FIELDS. Thiếu nó thì cùng một phiên gửi hai
# locale khác nhau: đường trình duyệt vi-VN, đường replay en-US.
#
# Giá trị đo được từ chính trình duyệt (probe 27/10/2026, Chromium 151 của patchright):
# header accept-language chỉ có "vi-VN" - KHÔNG phải chuỗi nhiều thành phần. Dùng đúng
# giá trị đó để hai đường khớp từng byte, không chỉ khớp "cùng là tiếng Việt".
ACCEPT_LANGUAGE = "vi-VN"

# --- Hồ sơ trình duyệt RIÊNG cho từng tài khoản -------------------------------------------------
#
# Vì sao cần: trước đây MỌI tài khoản đều khai cùng một UA (Chrome 151 trên macOS) và cùng
# một bộ Client Hints. Với mắt thường thì "nhiều người dùng Việt Nam trên cùng một cấu
# hình" là chuyện thường, nhưng Facebook gom dấu vân tay của nhiều tài khoản trên cùng
# một hạ tầng (cùng datacenter, cùng nhịp crawl, cùng lịch nuôi) - một giá trị UA lặp lại
# y hệt ở mọi tài khoản là dấu hiệu "một người điều khiển nhiều tài khoản", mạnh hơn hẳn
# việc mỗi tài khoản một thiết bị riêng.
#
# Cách chọn: TẤT ĐỊNH theo account_key (hash của email) - không random mỗi lần chạy. Lý do:
# Facebook tin một dấu vân tay *nhất quán* giữa các phiên hơn là một dấu vân tay đẹp. Một
# tài khoản phải luôn trông như CÙNG một máy trên mọi lần bootstrap, mọi lần nuôi, mọi
# lần crawl - nếu đổi version giữa hai lần chạy thì chính cái sự đổi đó mới là dấu hiệu.
#
# Vì sao KHÔNG đổi phiên bản Chrome thật của trình duyệt: patchright đóng gói đúng một
# Chromium (151). Nếu khai UA là Chrome 146 cho một tài khoản thì:
#   - đường replay giả được TLS của Chrome 146 (curl_cffi có target đó) -> khớp với UA,
#     nhưng đường trình duyệt vẫn là Chromium 151 -> Client Hints (sec-ch-ua) của nó là của
#     151, lệch với UA 146.
# Nên: giữ CHROME_MAJOR cho mọi tài khoản (khớp cả hai đường), và đa dạng hóa ở tầng
# khác - viewport + mức băm/bóp nhịp.

# Các "biến thể thiết bị" mà mỗi tài khoản được xếp vào. Mỗi biến thể là một cặp
# (target curl_cffi, User-Agent) đã đo là khớp nhau.
#
# Lưu ý: chỉ những target curl_cffi có thật mới dùng được - xem danh sách bằng
# `from curl_cffi.requests.impersonate import BrowserTypeLiteral`. Test
# tests/test_fingerprint_consistency.py kiểm tra từng target trong bảng này tồn tại.
#
# CHỈ dùng target có hệ điều hành macOS cho bước đầu: server chạy Linux, nhưng đường
# trình duyệt chạy trên máy có màn hình (Xvfb) hoặc headless - UA khai macOS trên máy Linux
# là điều Facebook KHÔNG kiểm tra được từ phía server (nó chỉ thấy header), nên không
# gây vấn đề. Đổi sang Windows sẽ bắt đầu lệch với phần "nền tảng" mà Facebook suy ra từ
# địa chỉ IP (datacenter VN khó lắm) - nên giữ macOS cho tất cả.
#
# MỌI biến thể phải có target cùng major với CHROME_MAJOR (27/10/2026: từng có thêm
# chrome152_macos + chrome150 - bị gỡ). CDP chỉ đổi được UA/Client Hints, KHÔNG đổi được TLS
# của Chromium thật: tài khoản xếp vào 152 thì lúc bootstrap trình duyệt khai UA 152 nhưng
# bắt tay TLS của 151, rồi lúc replay lại bắt tay TLS 152 - cùng một phiên đổi TLS giữa
# chừng, và trình duyệt nói dối phiên bản của chính nó. Muốn thêm biến thể thì chỉ đổi
# những thứ không đụng tới engine (viewport, platform_version...) hoặc phải chạy thật một
# Chromium khác. tests/test_fingerprint_consistency.py chặn target lệch major.
_DEVICE_VARIANTS: tuple[dict[str, str], ...] = (
    {
        "target": "chrome151_macos",
        "platform": "macOS",
        "platform_version": "15.5.0",
        "os_ua": "Macintosh; Intel Mac OS X 10_15_7",
        "nav_platform": "MacIntel",
        "architecture": "arm",
    },
)

# Cùng một target dùng cho mọi tài khoản - khác biệt giữa các tài khoản nằm ở viewport (đã
# có sẵn ở browser_interaction._viewport_for) và ở nhịp bóp/thời gian chờ (đã có ở từng chỗ).
_DEVICE_VARIANTS_BY_TARGET = {v["target"]: v for v in _DEVICE_VARIANTS}


def _variant_for(account_key: str | None) -> dict[str, str]:
    """Biến thể thiết bị của một tài khoản - tất định theo account_key.

    Không có account_key (luồng tay, ẩn danh) thì dùng biến thể đầu tiên, tức là đúng
    browser của patchright."""
    if not account_key:
        return _DEVICE_VARIANTS[0]
    digest = hashlib.sha256(account_key.encode()).hexdigest()
    return _DEVICE_VARIANTS[int(digest, 16) % len(_DEVICE_VARIANTS)]


def device_profile(account_key: str | None) -> dict[str, str]:
    """Hồ sơ thiết bị của một tài khoản: User-Agent, Client Hints và target curl_cffi.

    Dùng ở CẢ HAI đường:
      - đường trình duyệt: new_context(user_agent=...) + ua_override(...)
      - đường replay: impersonate=<target> + header user-agent/sec-ch-ua*

    Nhờ vậy hai đường của cùng một tài khoản luôn khai cùng một bộ dấu vân tay, còn các
    tài khoản khác nhau thì khác nhau."""
    variant = _variant_for(account_key)
    # Version trong UA = version của target curl_cffi. Lý do: target quyết định dấu vân
    # tay TLS, còn UA là thứ Facebook đối chiếu với TLS của cùng phiên đó. Khớp hai
    # thứ này là điều kiện để một tài khoản trông như một máy thật.
    target_major = re.search(r"chrome(\d+)", variant["target"])
    major = target_major.group(1) if target_major else str(CHROME_MAJOR)
    order = SEC_CH_UA_BY_MAJOR.get(major, SEC_CH_UA_BY_MAJOR[str(CHROME_MAJOR)])
    grease_version = _GREASE_VERSION_BY_MAJOR.get(major, _GREASE_VERSION_BY_MAJOR[str(CHROME_MAJOR)])
    full_version = CHROME_FULL_VERSION if major == str(CHROME_MAJOR) else f"{major}.0.0.0"

    user_agent = (
        f"Mozilla/5.0 ({variant['os_ua']}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"
    )
    brands = [{"brand": brand, "version": grease_version if brand.startswith("Not") else major} for brand in order]
    sec_ch_ua = ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in brands)
    full_version_list = ", ".join(
        f'"{b["brand"]}";v="{b["version"]}.0.0.0"'
        if b["brand"].startswith("Not")
        else f'"{b["brand"]}";v="{full_version}"'
        for b in brands
    )
    return {
        "major": major,
        "full_version": full_version,
        "target": variant["target"],
        "user_agent": user_agent,
        "sec_ch_ua": sec_ch_ua,
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": f'"{variant["platform"]}"',
        "sec_ch_ua_platform_version": f'"{variant["platform_version"]}"',
        "sec_ch_ua_full_version_list": full_version_list,
        "accept_language": ACCEPT_LANGUAGE,
        "nav_platform": variant["nav_platform"],
        "architecture": variant["architecture"],
        "brands": brands,
    }


def _platform_of(user_agent: str) -> str:
    if "Android" in user_agent:
        return "Android"
    if "Macintosh" in user_agent:
        return "macOS"
    if "Windows" in user_agent:
        return "Windows"
    return "Linux"


def build_ua_override(user_agent: str) -> dict | None:
    """Payload cho CDP Emulation.setUserAgentOverride, khai đúng trình duyệt mà UA mô tả.

    None với UA không phải Chrome (Firefox/Safari): Client Hints của Chrome đi kèm UA đó
    cũng sai, nên để nguyên và chỗ gọi tự chịu.

    Vì sao cần Client Hints khớp: Playwright đặt user_agent= chỉ đổi CHUỖI UA, còn
    sec-ch-ua vẫn khai "HeadlessChrome" (đã kiểm chứng) - mâu thuẫn ngay trong chính UA.
    Vì sao cần khớp CẢ HAI ĐƯỜNG: giá trị ở đây được gửi tới Facebook qua header của
    request replay, còn trình duyệt thật gửi đúng giá trị này - nếu hai bên lệch nhau thì
    Facebook thấy cùng một phiên với hai bộ client hints khác nhau.
    """
    match = re.search(r"Chrome/(\d+)", user_agent)
    if not match or "Edg/" in user_agent or "OPR/" in user_agent:
        return None
    major = match.group(1)
    platform = _platform_of(user_agent)
    if "Android" in user_agent:
        platform_version, arch, nav_platform, mobile = "14.0.0", "", "Linux armv8l", True
    elif platform == "macOS":
        platform_version, arch, nav_platform, mobile = "15.5.0", "arm", "MacIntel", False
    elif platform == "Windows":
        platform_version, arch, nav_platform, mobile = "10.0.0", "x86", "Win32", False
    else:
        platform_version, arch, nav_platform, mobile = "", "x86", "Linux x86_64", False

    # Brand + thứ tự lấy từ bảng đo; phiên bản lạ thì dùng bộ của 151 (đường replay đang
    # giả bản đó) - tốt hơn là tự bịa một bộ brand khác.
    order = SEC_CH_UA_BY_MAJOR.get(major, SEC_CH_UA_BY_MAJOR[str(CHROME_MAJOR)])
    grease_version = _GREASE_VERSION_BY_MAJOR.get(major, _GREASE_VERSION_BY_MAJOR[str(CHROME_MAJOR)])
    brands = []
    for brand in order:
        version = grease_version if brand.startswith("Not") else major
        brands.append({"brand": brand, "version": version})
    full_version = CHROME_FULL_VERSION if major == str(CHROME_MAJOR) else f"{major}.0.0.0"
    return {
        "userAgent": user_agent,
        "acceptLanguage": ACCEPT_LANGUAGE,
        "platform": nav_platform,
        "userAgentMetadata": {
            "brands": brands,
            "fullVersionList": [
                {"brand": b["brand"], "version": f"{b['version']}.0.0.0"}
                if b["brand"].startswith("Not")
                else {"brand": b["brand"], "version": full_version}
                for b in brands
            ],
            "fullVersion": full_version,
            "platform": platform,
            "platformVersion": platform_version,
            "architecture": arch,
            "model": "",
            "mobile": mobile,
            "bitness": "64",
            "wow64": False,
        },
    }


def ua_override_for_account(account_key: str | None) -> dict:
    """Payload CDP Emulation.setUserAgentOverride cho hồ sơ của một tài khoản.

    Dùng hồ sơ (device_profile) thay vì build_ua_override(UA) để đường trình duyệt khai
    đúng bộ Client Hints mà đường replay của CÙNG tài khoản đó sẽ gửi - cả hai lấy từ
    một nguồn nên không thể lệch nhau."""
    profile = device_profile(account_key)
    brands = profile["brands"]
    full_version = profile["full_version"]
    return {
        "userAgent": profile["user_agent"],
        "acceptLanguage": profile["accept_language"],
        "platform": profile["nav_platform"],
        "userAgentMetadata": {
            "brands": brands,
            "fullVersionList": [
                {"brand": b["brand"], "version": f"{b['version']}.0.0.0"}
                if b["brand"].startswith("Not")
                else {"brand": b["brand"], "version": full_version}
                for b in brands
            ],
            "fullVersion": full_version,
            "platform": profile["sec_ch_ua_platform"].strip('"'),
            "platformVersion": profile["sec_ch_ua_platform_version"].strip('"'),
            "architecture": profile["architecture"],
            "model": "",
            "mobile": False,
            "bitness": "64",
            "wow64": False,
        },
    }


# --- UA riêng của tài khoản (cookie import mang mục useragent=...) ---------------------------
#
# Một UA riêng chỉ dùng được khi CẢ HAI đường khai được nó trung thực:
#   - đường trình duyệt: Chromium thật là CHROME_MAJOR -> UA phải là Chrome CHROME_MAJOR
#     (CDP đổi được chuỗi UA/Client Hints, không đổi được TLS/engine);
#   - đường replay: curl_cffi phải có target cùng major + cùng hệ điều hành.
# UA nào không thoả (Chrome 146, Edge, Firefox, Android, Linux...) bị bỏ qua -> hồ sơ mặc
# định của tài khoản, ở CẢ HAI đường (trước 27/10/2026: trình duyệt dùng UA riêng còn replay
# ghi đè bằng hồ sơ -> một phiên hai trình duyệt).
_CUSTOM_UA_TARGET_BY_PLATFORM = {
    "macOS": f"chrome{CHROME_MAJOR}_macos",
    "Windows": f"chrome{CHROME_MAJOR}",
}


def upgrade_user_agent(user_agent: str) -> str:
    """UA Chrome / Edge / Opera desktop (Windows, macOS) của bản khác -> UA Chrome CHROME_MAJOR cùng hệ điều hành (30/10/2026).

    Trình duyệt của ta luôn là Chromium CHROME_MAJOR, nên không khai trung thực được bản cũ hơn. Trước đây UA như vậy bị
    bỏ hẳn và thay bằng hồ sơ ngẫu nhiên theo tài khoản (device_profile) - có khi khác cả hệ điều hành của máy đã tạo
    cookie. Cùng hệ điều hành + Chrome tự cập nhật lên bản mới là chuyện bình thường của một máy thật; đổi hệ điều hành
    thì không. UA di động / hệ điều hành khác giữ nguyên (custom_ua_target trả None -> hồ sơ mặc định như cũ)."""
    if not user_agent or "Mobile" in user_agent or "Chrome/" not in user_agent:
        return user_agent
    os_part = {"Windows": "Windows NT 10.0; Win64; x64", "macOS": "Macintosh; Intel Mac OS X 10_15_7"}.get(
        _platform_of(user_agent)
    )
    if os_part is None:
        return user_agent
    return f"Mozilla/5.0 ({os_part}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{CHROME_MAJOR}.0.0.0 Safari/537.36"


def custom_ua_target(user_agent: str | None) -> str | None:
    """Target curl_cffi khớp với một UA riêng, hoặc None nếu UA đó không dùng được (xem trên)."""
    if not user_agent or "Edg/" in user_agent or "OPR/" in user_agent or "Mobile" in user_agent:
        return None
    match = re.search(r"Chrome/(\d+)", user_agent)
    if not match or match.group(1) != str(CHROME_MAJOR):
        return None
    return _CUSTOM_UA_TARGET_BY_PLATFORM.get(_platform_of(user_agent))


def resolve_ua_override(account_key: str | None, user_agent: str | None = None) -> dict:
    """Payload CDP cho đường trình duyệt: UA riêng nếu dùng được, không thì hồ sơ tài khoản."""
    if custom_ua_target(user_agent):
        return build_ua_override(user_agent)
    return ua_override_for_account(account_key)


def resolve_profile(account_key: str | None, user_agent: str | None = None) -> dict:
    """Hồ sơ cho đường replay, cùng quy tắc với resolve_ua_override - dựng thẳng TỪ payload
    CDP mà trình duyệt dùng, nên header replay không thể lệch với thứ trình duyệt gửi.

    user_agent: UA bắt được trong cache bootstrap (= UA trình duyệt đã khai)."""
    target = custom_ua_target(user_agent)
    if target is None:
        return device_profile(account_key)
    override = build_ua_override(user_agent)
    meta = override["userAgentMetadata"]
    return {
        "major": str(CHROME_MAJOR),
        "full_version": meta["fullVersion"],
        "target": target,
        "user_agent": user_agent,
        "sec_ch_ua": ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in meta["brands"]),
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": f'"{meta["platform"]}"',
        "sec_ch_ua_platform_version": f'"{meta["platformVersion"]}"',
        "sec_ch_ua_full_version_list": ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in meta["fullVersionList"]),
        "accept_language": override["acceptLanguage"],
        "nav_platform": override["platform"],
        "architecture": meta["architecture"],
        "brands": meta["brands"],
    }
