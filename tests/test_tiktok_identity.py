from __future__ import annotations

import pytest

from social_crawler.spiders.tiktok.auth.accounts import is_logged_in_cookie
from social_crawler.spiders.tiktok.auth.identity import (
    choose_identity,
    cookies_for_identity,
    identity_from_url,
    prefer_item_list_identity,
    stored_identity,
)

STORED_DEVICE = "7661323956708034049"
STORED_ODIN = "7240111122233344556"
PLAYWRIGHT_DEVICE = "7643007170271839760"
PLAYWRIGHT_ODIN = "7110000000000000001"


def test_stored_identity_ignores_email_account_id():
    assert stored_identity({"id": "joshuasmithuwqxi@outlook.com", "token": STORED_ODIN}) is None


def test_stored_identity_rejects_stale_odin_vs_multi_sids():
    cookie = f"ttwid=x; sessionid=y; multi_sids={PLAYWRIGHT_ODIN}%3Asession"
    assert stored_identity({"id": STORED_DEVICE, "token": STORED_ODIN, "cookie": cookie}) is None
    matched = f"ttwid=x; sessionid=y; multi_sids={STORED_ODIN}%3Asession"
    assert stored_identity({"id": STORED_DEVICE, "token": STORED_ODIN, "cookie": matched}) == (
        STORED_DEVICE,
        STORED_ODIN,
    )


def test_parse_browser_export_reads_curl_device_and_odin():
    from social_crawler.spiders.tiktok.auth.identity import parse_browser_export

    curl = (
        "curl --url 'https://www.tiktok.com/api/challenge/item_list/"
        f"?device_id={PLAYWRIGHT_DEVICE}&odinId={PLAYWRIGHT_ODIN}&user_is_login=true' "
        "-b 'ttwid=abc; sessionid=xyz; multi_sids="
        f"{PLAYWRIGHT_ODIN}%3Axyz'"
    )
    header, pair = parse_browser_export(curl)
    assert pair == (PLAYWRIGHT_DEVICE, PLAYWRIGHT_ODIN)
    assert "sessionid=xyz" in header


def test_stored_identity_requires_both_numeric_ids():
    assert stored_identity({"id": STORED_DEVICE, "token": STORED_ODIN}) == (STORED_DEVICE, STORED_ODIN)
    assert stored_identity({"id": STORED_DEVICE, "token": ""}) is None


def test_choose_identity_keeps_stored_pair_when_playwright_differs():
    choice = choose_identity(
        {"id": STORED_DEVICE, "token": STORED_ODIN},
        (PLAYWRIGHT_DEVICE, PLAYWRIGHT_ODIN),
    )
    assert choice.source == "stored"
    assert choice.device_id == STORED_DEVICE
    assert choice.odin_id == STORED_ODIN
    assert choice.playwright_pair == (PLAYWRIGHT_DEVICE, PLAYWRIGHT_ODIN)


def test_choose_identity_uses_playwright_when_row_has_no_trusted_ids():
    choice = choose_identity({"id": "guest-row", "token": ""}, (PLAYWRIGHT_DEVICE, PLAYWRIGHT_ODIN))
    assert choice.source == "playwright"
    assert choice.device_id == PLAYWRIGHT_DEVICE


def test_cookies_for_stored_identity_ignore_playwright_jar():
    original = {"ttwid": "chrome-ttwid", "sessionid": "chrome-sid", "msToken": "chrome-ms"}
    playwright = {"ttwid": "pw-ttwid", "sessionid": "chrome-sid", "msToken": "pw-ms"}
    assert cookies_for_identity("stored", original, playwright) == original
    mixed = cookies_for_identity("playwright", original, playwright)
    assert mixed["ttwid"] == "pw-ttwid"
    assert mixed["msToken"] == "pw-ms"


def test_choose_identity_raises_when_nothing_usable():
    with pytest.raises(RuntimeError, match="No device_id"):
        choose_identity({"id": "guest-row", "token": ""}, None)


def test_prefer_item_list_over_earlier_detail_request():
    detail = (
        "https://www.tiktok.com/api/challenge/detail/?device_id=1&odinId=2&challengeName=fyp"
    )
    item_list = (
        "https://www.tiktok.com/api/challenge/item_list/"
        f"?device_id={PLAYWRIGHT_DEVICE}&odinId={PLAYWRIGHT_ODIN}&challengeID=9"
    )
    assert prefer_item_list_identity([detail, item_list]) == (PLAYWRIGHT_DEVICE, PLAYWRIGHT_ODIN)
    assert identity_from_url(detail) == ("1", "2")


def test_is_logged_in_cookie_header_and_json():
    assert is_logged_in_cookie("ttwid=abc; sessionid=xyz")
    assert is_logged_in_cookie('[{"name": "sessionid", "value": "xyz"}, {"name": "ttwid", "value": "a"}]')
    assert is_logged_in_cookie("ttwid=abc; SID_TT=xyz")
    assert not is_logged_in_cookie("ttwid=abc; msToken=tok")


def test_cookie_map_drops_split_value_promoted_to_name():
    from social_crawler.spiders.tiktok.auth.cookies import cookie_map

    raw = (
        "ttwid=abc; sessionid=xyz; "
        "M.C539_BAY.junk|uuid|user@example.com|tt_csrf_token=realcsrf; "
        "tt_csrf_token=realcsrf"
    )
    names = cookie_map(raw)
    assert "sessionid" in names
    assert "tt_csrf_token" in names
    assert all("|" not in name for name in names)
    assert all(len(name) < 80 for name in names)
