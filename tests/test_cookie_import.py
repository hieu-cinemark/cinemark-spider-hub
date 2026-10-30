"""Nhập cookie Facebook (30/10/2026): định dạng extension, thuộc tính cookie thật, nâng UA giữ hệ điều hành."""

import base64

from social_crawler.core.fingerprint import CHROME_MAJOR, custom_ua_target, upgrade_user_agent
from social_crawler.spiders.facebook.auth.cookies import build_storage_state_from_cookies, extract_user_agent


def test_extension_export_is_normalized():
    exported = [
        {
            "domain": ".facebook.com",
            "expirationDate": 1830000000.5,
            "hostOnly": False,
            "httpOnly": True,
            "name": "xs",
            "path": "/",
            "sameSite": "no_restriction",
            "secure": True,
            "session": False,
            "storeId": "0",
            "value": "1%3Aa",
        },
        {"domain": ".facebook.com", "name": "presence", "sameSite": "unspecified", "session": True, "value": "x"},
    ]
    cookies = build_storage_state_from_cookies(exported)["cookies"]
    assert cookies[0] == {
        "name": "xs",
        "value": "1%3Aa",
        "domain": ".facebook.com",
        "path": "/",
        "expires": 1830000000.5,
        "httpOnly": True,
        "secure": True,
        "sameSite": "None",
    }
    assert cookies[1]["expires"] == -1 and cookies[1]["sameSite"] == "Lax"


def test_header_cookies_match_real_attributes():
    cookies = {c["name"]: c for c in build_storage_state_from_cookies("c_user=1; xs=2; datr=3; sb=4; wd=5")["cookies"]}
    assert cookies["datr"]["httpOnly"] and cookies["sb"]["httpOnly"] and not cookies["wd"]["httpOnly"]
    assert {c["sameSite"] for c in cookies.values()} == {"None"}


def test_old_chrome_ua_upgraded_keeping_os():
    windows = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    upgraded = upgrade_user_agent(windows)
    assert f"Chrome/{CHROME_MAJOR}.0.0.0" in upgraded and "Windows NT 10.0" in upgraded
    assert custom_ua_target(upgraded) == f"chrome{CHROME_MAJOR}"
    edge_mac = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
    )
    assert custom_ua_target(upgrade_user_agent(edge_mac)) == f"chrome{CHROME_MAJOR}_macos"
    mobile = (
        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"
    )
    assert upgrade_user_agent(mobile) == mobile


def test_extract_user_agent_upgrades():
    ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    raw = base64.b64encode(ua.encode()).decode()
    assert f"Chrome/{CHROME_MAJOR}." in extract_user_agent({"useragent": raw})
