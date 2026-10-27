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
    """jazoest là checksum của lsd: "2" + tổng mã ord. Sai công thức là body bị sửa tay."""
    from social_crawler.spiders.comet_graphql_client import _jazoest

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
    __req tụt về 0 trông như tab mới) và tăng theo bước nhỏ, không đều tăm tắp."""
    from social_crawler.spiders.comet_graphql_client import CometGraphQLClient

    client = CometGraphQLClient.__new__(CometGraphQLClient)
    client._cache = {"body_static": {"__req": "42"}}
    client._req_counter = client._initial_req_counter()
    assert client._req_counter == 42

    values = [client._next_req_counter() for _ in range(8)]
    assert all(b > a for a, b in zip(values, values[1:])), "__req phai tang donn dieu"
    # Bước tăng 1-3 (trình duyệt thật không tăng đều 1, cũng không tăng bất quy tắc).
    steps = [v - p for v, p in zip(values, [42, *values[:-1]])]
    assert all(step in (1, 2, 3) for step in steps), steps


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
