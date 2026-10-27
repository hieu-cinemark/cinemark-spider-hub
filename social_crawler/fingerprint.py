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
  - comet_graphql_client.py: impersonate=CURL_CFFI_IMPERSONATE_TARGET
  - browser_interaction.py: new_context() dùng BROWSER_UA + build_ua_override()
  - post_metrics.py / proxy_health_check.py / nurture_accounts.py: cùng target
"""

from __future__ import annotations

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
