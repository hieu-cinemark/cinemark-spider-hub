"""Dấu vân tay trình duyệt phải KHỚP giữa đường trình duyệt và đường replay HTTP.

Một phiên Facebook có hai đường đi: browser (patchright/Chromium) dùng để đăng nhập và bắt
request, rồi curl_cffi phát lại request đó. Facebook ràng buộc cookie phiên với dấu vân tay
đã tạo ra nó - nếu hai đường khai khác nhau thì phiên trông như "cookie bị nhân bản".

Test này tự đo: bắt header curl_cffi gửi thật tới một server local, rồi so với những gì
code khai. Nhờ vậy nâng patchright/curl_cffi mà quên sửa fingerprint.py thì test đỏ, không
phải tới lúc tài khoản bị checkpoint mới biết.
"""

from __future__ import annotations

import http.server
import socketserver
import threading

import pytest

from social_crawler import fingerprint


def _capture_headers(impersonate: str, *, method: str = "GET", headers: dict[str, str] | None = None) -> dict[str, str]:
    """Header mà curl_cffi thật sự gửi đi khi impersonate=impersonate (và header override
    nếu có - mô phỏng đúng cách client replay gửi request)."""
    from curl_cffi import requests as curl_requests

    captured: dict[str, str] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self):
            captured.update({k.lower(): v for k, v in self.headers.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")

        do_GET = _respond
        do_POST = _respond

        def log_message(self, *args):  # noqa: D102 - im lặng log của server
            pass

    server = socketserver.TCPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        session = curl_requests.Session(impersonate=impersonate)
        url = f"http://127.0.0.1:{port}/"
        if method == "POST":
            session.post(url, data={"a": "b"}, headers=headers, timeout=10)
        else:
            session.get(url, headers=headers, timeout=10)
    finally:
        server.shutdown()
        server.server_close()
    return captured


@pytest.fixture(scope="module")
def curl_headers() -> dict[str, str]:
    """Header thô của curl_cffi, KHÔNG override - để thấy được cái curl_cffi tự gửi
    (ví dụ accept-language mặc định là en-US, khác locale của trình duyệt)."""
    return _capture_headers(fingerprint.CURL_CFFI_IMPERSONATE_TARGET)


@pytest.fixture(scope="module")
def replay_headers() -> dict[str, str]:
    """Header của một request replay THẬT - tức là curl_cffi + bộ header mà
    CometGraphQLClient gửi (lấy từ cache, đã chuẩn hoá theo fingerprint.py)."""
    from social_crawler.spiders.comet_graphql_client import normalize_cached_fingerprint

    cache = normalize_cached_fingerprint(
        {
            "captured_at": 0,
            "cookies": {},
            "headers": {"user-agent": fingerprint.BROWSER_UA},
            "body_static": {},
        }
    )
    return _capture_headers(
        fingerprint.CURL_CFFI_IMPERSONATE_TARGET,
        method="POST",
        headers=cache["headers"],
    )


def test_impersonate_target_exists_in_installed_curl_cffi() -> None:
    """Target trong fingerprint.py phải có thật trong bản curl_cffi đang cài - nếu không,
    mọi request sẽ ném lỗi lúc chạy chứ không phải lúc test."""
    from curl_cffi.requests.impersonate import BrowserTypeLiteral

    assert fingerprint.CURL_CFFI_IMPERSONATE_TARGET in set(BrowserTypeLiteral.__args__)


def test_browser_ua_matches_curl_cffi_user_agent(curl_headers: dict[str, str]) -> None:
    """UA mà trình duyệt khai phải BẰNG UA mà curl_cffi gửi - lệch là trình duyệt nói dối
    phiên bản của chính nó (Chrome 146 trong khi browser thật là 151)."""
    assert curl_headers["user-agent"] == fingerprint.BROWSER_UA


def test_client_hints_match_curl_cffi(curl_headers: dict[str, str]) -> None:
    """sec-ch-ua mà trình duyệt khai (qua CDP) phải bằng đúng thứ curl_cffi gửi."""
    assert curl_headers["sec-ch-ua"] == fingerprint.SEC_CH_UA
    assert curl_headers["sec-ch-ua-mobile"] == "?0"
    assert curl_headers["sec-ch-ua-platform"] == '"macOS"'


def test_ua_override_carries_the_same_client_hints(curl_headers: dict[str, str]) -> None:
    """Payload CDP của trình duyệt phải mang đúng bộ Client Hints mà đường replay gửi -
    cùng một phiên không được có hai bộ client hints khác nhau."""
    override = fingerprint.build_ua_override(fingerprint.BROWSER_UA)
    assert override is not None
    brands = override["userAgentMetadata"]["brands"]
    rebuilt = ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in brands)
    assert rebuilt == curl_headers["sec-ch-ua"]


def test_ua_override_rejects_non_chrome() -> None:
    assert (
        fingerprint.build_ua_override("Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0") is None
    )


def test_accept_language_is_vietnamese_for_replay() -> None:
    """Đường replay phải gửi cùng locale với browser (vi-VN), không phải en-US mặc định
    của curl_cffi. Giá trị phải đúng bằng giá trị trình duyệt thật gửi (chỉ "vi-VN",
    không phải chuỗi nhiều thành phần)."""
    assert fingerprint.ACCEPT_LANGUAGE == "vi-VN"


def test_jazoest_matches_lsd_checksum() -> None:
    """jazoest là checksum của lsd: "2" + tổng mã ord. Sai công thức là body bị sửa tay.

    Cặp (lsd, jazoest) dưới đây là giá trị ĐO được từ traffic thật của Chromium 151
    (27/10/2026) - không phải ví dụ bịa."""
    from social_crawler.spiders.comet_graphql_client import _jazoest

    # Cặp bắt được thật: lsd="AdQwYHdldL7sGQkEuO2CFPdh8RY" -> jazoest="22302".
    assert _jazoest("AdQwYHdldL7sGQkEuO2CFPdh8RY") == "22302"
    # Và công thức tổng quát.
    lsd = "AVq1-Z9_2s"
    assert _jazoest(lsd) == "2" + str(sum(ord(c) for c in lsd))


def test_session_counters_advance_and_keep_jazoest_in_sync() -> None:
    """__req phải TĂNG mỗi request (không đứng yên), và jazoest phải luôn khớp lsd."""
    from social_crawler.spiders.comet_graphql_client import _apply_session_counters, _jazoest

    body = {"__req": "10", "lsd": "AVq1-Z9_2s", "jazoest": "stale", "__hs": "hashvalue"}

    first = _apply_session_counters(body, 11)
    second = _apply_session_counters(body, 14)

    assert first["__req"] == "11"
    assert second["__req"] == "14"
    assert first["jazoest"] == _jazoest("AVq1-Z9_2s")
    # __hs là hash nội dung request, không phải bộ đếm -> không được đụng vào.
    assert first["__hs"] == "hashvalue"
    # Không sửa tại chỗ body gốc (cache đã lưu không được bị bẩn).
    assert body["__req"] == "10"
    assert body["jazoest"] == "stale"


def test_req_counter_starts_at_captured_value_and_increases() -> None:
    """Bộ đếm __req phải bắt đầu từ giá trị bootstrap bắt được (không nhảy về 0 - phiên mà
    __req tụt về 0 trông như tab mới) và tăng đúng +1 mỗi request.

    +1 là nhịp đo được từ traffic thật của Chromium 151 (27/10/2026): chuỗi __req gửi đi
    là 1, 2, 3, 4 - mỗi request tăng đúng một đơn vị."""
    from social_crawler.spiders.comet_graphql_client import CometGraphQLClient

    client = CometGraphQLClient.__new__(CometGraphQLClient)
    client._cache = {"body_static": {"__req": "42"}}
    client._req_counter = client._initial_req_counter()
    assert client._req_counter == 42

    values = [client._next_req_counter() for _ in range(8)]
    assert values == [43, 44, 45, 46, 47, 48, 49, 50]


def test_req_counter_falls_back_when_cache_has_no_req() -> None:
    from social_crawler.spiders.comet_graphql_client import CometGraphQLClient

    client = CometGraphQLClient.__new__(CometGraphQLClient)
    client._cache = {"body_static": {}}
    assert client._initial_req_counter() == 1
    client._cache = {"body_static": {"__req": "khong-phai-so"}}
    assert client._initial_req_counter() == 1


def test_legacy_cache_fingerprint_is_normalized() -> None:
    """Cache token bắt từ trước khi nâng phiên bản vẫn mang UA/Client Hints của Chrome 146.
    Đọc cache phải đồng bộ lại về dấu vân tay đang dùng, nếu không cùng một phiên sẽ có
    TLS của 151 nhưng sec-ch-ua của 146."""
    from social_crawler.spiders.comet_graphql_client import normalize_cached_fingerprint

    legacy = {
        "captured_at": 0,
        "cookies": {"c_user": "123"},
        "headers": {
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
            ),
            "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        },
        "body_static": {"__req": "5"},
    }
    normalized = normalize_cached_fingerprint(legacy)

    assert normalized["headers"]["user-agent"] == fingerprint.BROWSER_UA
    assert normalized["headers"]["sec-ch-ua"] == fingerprint.SEC_CH_UA
    assert normalized["headers"]["accept-language"] == fingerprint.ACCEPT_LANGUAGE
    assert normalized["headers"]["sec-ch-ua-full-version-list"] == fingerprint.SEC_CH_UA_FULL_VERSION_LIST
    # Cookie/session không được đụng vào - chúng gắn với dấu vân tay đã tạo ra chúng.
    assert normalized["cookies"] == legacy["cookies"]
    # Bản gốc trong Redis không bị sửa tại chỗ.
    assert legacy["headers"]["user-agent"].endswith("Chrome/146.0.0.0 Safari/537.36")


def test_replay_headers_match_a_real_browser_session(replay_headers: dict[str, str]) -> None:
    """Header mà đường replay gửi phải khớp từng cái với header mà trình duyệt thật
    gửi - cùng một phiên mà hai bộ client hints khác nhau thì Facebook thấy ngay.

    Bộ header kỳ vọng ở đây là giá trị ĐO được từ Chromium 151 của patchright
    (probe 27/10/2026) - nếu nâng patchright thì probe lại và cập nhật, test này sẽ đỏ.
    """
    expected = {
        "user-agent": fingerprint.BROWSER_UA,
        "sec-ch-ua": fingerprint.SEC_CH_UA,
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-ch-ua-platform-version": f'"{fingerprint.SEC_CH_UA_PLATFORM_VERSION}"',
        "sec-ch-ua-full-version-list": fingerprint.SEC_CH_UA_FULL_VERSION_LIST,
        "accept-language": fingerprint.ACCEPT_LANGUAGE,
    }
    for header, value in expected.items():
        assert replay_headers.get(header) == value, f"{header} lech: {replay_headers.get(header)!r} != {value!r}"


def test_curl_cffi_default_accept_language_would_have_leaked(curl_headers: dict[str, str]) -> None:
    """Bảo đảm lý do phải override accept-language là có thật: nếu không override,
    curl_cffi gửi en-US - lệch locale với trình duyệt (vi-VN)."""
    assert curl_headers.get("accept-language") != fingerprint.ACCEPT_LANGUAGE
    assert curl_headers.get("accept-language", "").startswith("en-US")


# --- Hồ sơ thiết bị RIÊNG cho từng tài khoản ------------------------------------------------


def test_device_profile_is_deterministic() -> None:
    """Cùng một tài khoản phải luôn ra CÙNG một hồ sơ - Facebook tin một dấu vân tay
    nhất quán giữa các phiên hơn là một dấu vân tay đẹp. Đổi hồ sơ giữa hai lần chạy
    chính là dấu hiệu "tài khoản bị đổi máy", tệ hơn cả việc dùng chung một hồ sơ."""
    first = fingerprint.device_profile("nguyen.van.a@example.com")
    second = fingerprint.device_profile("nguyen.van.a@example.com")
    assert first == second


def test_every_device_variant_matches_the_bundled_chromium() -> None:
    """Mọi biến thể phải cùng major với Chromium thật của patchright. CDP chỉ đổi được
    UA/Client Hints, không đổi được TLS: biến thể chrome152 nghĩa là lúc bootstrap trình
    duyệt khai 152 nhưng bắt tay TLS 151, lúc replay lại bắt tay TLS 152 - cùng một phiên
    đổi TLS giữa chừng (27/10/2026 cả 3 tài khoản FB rơi đúng vào trường hợp này)."""
    import re

    for variant in fingerprint._DEVICE_VARIANTS:
        assert re.search(r"chrome(\d+)", variant["target"]).group(1) == str(fingerprint.CHROME_MAJOR), variant


def test_device_profile_keeps_the_fingerprint_existing_sessions_were_made_with() -> None:
    """Các phiên đang sống được tạo với BROWSER_UA + build_ua_override(BROWSER_UA). Hồ sơ
    theo tài khoản phải ra đúng bộ đó - lệch một byte là mọi tài khoản đột ngột "đổi máy"
    ngay lần deploy."""
    for account in [f"account{i}@example.com" for i in range(20)]:
        profile = fingerprint.device_profile(account)
        assert profile["user_agent"] == fingerprint.BROWSER_UA
        assert profile["sec_ch_ua"] == fingerprint.SEC_CH_UA
        assert profile["sec_ch_ua_full_version_list"] == fingerprint.SEC_CH_UA_FULL_VERSION_LIST
        assert fingerprint.ua_override_for_account(account) == fingerprint.build_ua_override(fingerprint.BROWSER_UA)


def test_graphql_replay_sends_fetch_headers_not_navigation_headers() -> None:
    """Request GraphQL replay phải mang header của một fetch() cùng origin như trình duyệt
    gửi cho /api/graphql/ - không phải bộ mặc định "điều hướng trang" của curl_cffi
    (accept text/html, sec-fetch-mode navigate, sec-fetch-user ?1, ...)."""
    from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient

    client = FacebookGraphQLClient.__new__(FacebookGraphQLClient)
    client._cache = {"headers": {"user-agent": fingerprint.BROWSER_UA}, "cookies": {}}
    sent = _capture_headers(
        fingerprint.CURL_CFFI_IMPERSONATE_TARGET, method="POST", headers=client._headers("SomeQuery", "lsd")
    )
    assert sent["accept"] == "*/*"
    assert sent["origin"] == "https://www.facebook.com"
    assert (sent["sec-fetch-dest"], sent["sec-fetch-mode"], sent["sec-fetch-site"]) == ("empty", "cors", "same-origin")
    assert "sec-fetch-user" not in sent
    assert "upgrade-insecure-requests" not in sent


def test_every_device_variant_target_exists_in_curl_cffi() -> None:
    """Mỗi target trong bảng hồ sơ phải có thật trong bản curl_cffi đang cài - nếu không,
    request sẽ ném lỗi lúc chạy chứ không phải lúc test."""
    from curl_cffi.requests.impersonate import BrowserTypeLiteral

    available = set(BrowserTypeLiteral.__args__)
    for variant in fingerprint._DEVICE_VARIANTS:
        assert variant["target"] in available, f"target khong ton tai: {variant['target']}"


def test_device_profile_ua_version_matches_its_tls_target() -> None:
    """Version trong UA phải là version của target curl_cffi tương ứng - Facebook đối
    chiếu UA với dấu vân tay TLS của cùng phiên đó, UA nói 151 còn TLS là 150 là mâu
    thuẫn ngay trong chính một phiên."""
    import re

    accounts = [f"acc{i}@example.com" for i in range(30)]
    for account in accounts:
        profile = fingerprint.device_profile(account)
        target_major = re.search(r"chrome(\d+)", profile["target"]).group(1)
        ua_major = re.search(r"Chrome/(\d+)", profile["user_agent"]).group(1)
        assert ua_major == target_major, f"UA {ua_major} lech target {profile['target']}"


def test_browser_and_replay_use_the_same_profile_for_an_account() -> None:
    """Đường trình duyệt và đường replay của CÙNG một tài khoản phải khai cùng một bộ
    dấu vân tay - đây là điều kiện để một tài khoản trông như một máy thật."""
    from social_crawler.spiders.comet_graphql_client import normalize_cached_fingerprint

    account = "thu.ha.b@example.com"
    profile = fingerprint.device_profile(account)
    override = fingerprint.ua_override_for_account(account)

    assert override["userAgent"] == profile["user_agent"]
    rebuilt = ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in override["userAgentMetadata"]["brands"])
    assert rebuilt == profile["sec_ch_ua"]

    # Đường replay: cache được chuẩn hoá theo CÙNG hồ sơ đó.
    cache = normalize_cached_fingerprint(
        {"captured_at": 0, "cookies": {}, "headers": {"user-agent": "Chrome/1.0"}, "body_static": {}},
        account,
    )
    assert cache["headers"]["user-agent"] == profile["user_agent"]
    assert cache["headers"]["sec-ch-ua"] == profile["sec_ch_ua"]


def test_per_account_profile_stays_consistent_over_repeated_calls() -> None:
    """Hồ sơ phải ổn định qua nhiều lần gọi (cache đọc lại, retry, bootstrap lại) -
    Facebook so sánh dấu vân tay giữa các phiên của cùng tài khoản."""
    account = "stable@example.com"
    seen = {fingerprint.device_profile(account)["user_agent"] for _ in range(5)}
    assert len(seen) == 1


# --- UA riêng của tài khoản (cookie import) ----------------------------------------------------

_WIN_UA_151 = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    f"Chrome/{fingerprint.CHROME_MAJOR}.0.0.0 Safari/537.36"
)


@pytest.mark.parametrize(
    "user_agent",
    [
        # Chrome 146: Chromium thật là 151 -> trình duyệt sẽ nói dối phiên bản của chính nó.
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        # Edge / Android / Linux / Firefox: không có target curl_cffi khớp hoặc không phải Chrome.
        _WIN_UA_151 + f" Edg/{fingerprint.CHROME_MAJOR}.0.0.0",
        f"Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{fingerprint.CHROME_MAJOR}.0.0.0 Mobile Safari/537.36",
        f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{fingerprint.CHROME_MAJOR}.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
    ],
)
def test_unusable_custom_ua_falls_back_to_profile_on_both_paths(user_agent: str) -> None:
    """UA riêng không khai trung thực được thì CẢ HAI đường cùng bỏ nó, cùng dùng hồ sơ -
    trước đây trình duyệt dùng UA riêng còn replay ghi đè bằng hồ sơ."""
    account = "imported@example.com"
    assert fingerprint.custom_ua_target(user_agent) is None
    assert fingerprint.resolve_ua_override(account, user_agent) == fingerprint.ua_override_for_account(account)
    assert fingerprint.resolve_profile(account, user_agent) == fingerprint.device_profile(account)


def test_usable_custom_ua_is_kept_on_both_paths() -> None:
    """UA riêng Chrome CHROME_MAJOR trên Windows: trình duyệt khai nó, replay khai đúng nó
    với Client Hints dựng từ CÙNG payload CDP và target TLS Windows cùng major."""
    from social_crawler.spiders.comet_graphql_client import normalize_cached_fingerprint

    account = "imported@example.com"
    override = fingerprint.resolve_ua_override(account, _WIN_UA_151)
    assert override["userAgent"] == _WIN_UA_151
    assert override["userAgentMetadata"]["platform"] == "Windows"

    # Đường replay đọc lại UA mà trình duyệt đã khai (nằm trong cache bootstrap).
    cache = normalize_cached_fingerprint({"headers": {"user-agent": _WIN_UA_151}, "cookies": {}}, account)
    headers = cache["headers"]
    meta = override["userAgentMetadata"]
    assert headers["user-agent"] == _WIN_UA_151
    assert headers["sec-ch-ua"] == ", ".join(f'"{b["brand"]}";v="{b["version"]}"' for b in meta["brands"])
    assert headers["sec-ch-ua-platform"] == '"Windows"'
    assert headers["sec-ch-ua-platform-version"] == f'"{meta["platformVersion"]}"'
    assert headers["sec-ch-ua-full-version-list"] == ", ".join(
        f'"{b["brand"]}";v="{b["version"]}"' for b in meta["fullVersionList"]
    )
    assert fingerprint.resolve_profile(account, _WIN_UA_151)["target"] == f"chrome{fingerprint.CHROME_MAJOR}"


def test_custom_ua_targets_send_the_same_ua_and_hints() -> None:
    """Target curl_cffi chọn cho UA riêng phải tự gửi đúng UA + sec-ch-ua đó - tức TLS của
    target đúng là của trình duyệt mà UA mô tả."""
    from curl_cffi.requests.impersonate import BrowserTypeLiteral

    for user_agent in (_WIN_UA_151, fingerprint.BROWSER_UA):
        target = fingerprint.custom_ua_target(user_agent)
        assert target in set(BrowserTypeLiteral.__args__)
        sent = _capture_headers(target)
        profile = fingerprint.resolve_profile("x@example.com", user_agent)
        assert sent["user-agent"] == user_agent
        assert sent["sec-ch-ua"] == profile["sec_ch_ua"]
        assert sent["sec-ch-ua-platform"] == profile["sec_ch_ua_platform"]
