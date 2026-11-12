"""clients/proxy_provider - cơ chế chung: link reset + key, xác nhận bằng IP đầu ra đo được.

Mọi HTTP đều giả lập, không dùng token/link thật."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from social_crawler.clients import proxy_provider

SETTINGS = {
    "provider_request_timeout_seconds": 10,
    "provider_min_call_interval_seconds": 0,
    "provider_max_cooldown_wait_seconds": 0,
}


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch):
    rows: dict[str, dict] = {}
    proxy_rows: dict[str, list[dict]] = {}
    monkeypatch.setattr(proxy_provider, "get_provider", lambda key: rows.get(key))
    monkeypatch.setattr(proxy_provider, "get_setting", lambda key: SETTINGS[key])
    monkeypatch.setattr(proxy_provider.time, "sleep", lambda _seconds: None)
    used: list[tuple[str, str]] = []
    monkeypatch.setattr(proxy_provider, "record_provider_use", lambda key, status: used.append((key, status)))
    monkeypatch.setattr(proxy_provider, "_provider_rows", lambda key: proxy_rows.get(key, []))
    proxy_provider._last_call_at.clear()
    rows["_used"] = used  # type: ignore[assignment]
    return rows, proxy_rows


def _reset_ok(monkeypatch: pytest.MonkeyPatch, *, status_code: int = 200, body: dict | None = None) -> MagicMock:
    """Giả lập: gọi link reset trả về OK, đo IP qua cổng trả về IP mới (lần đầu là IP cũ)."""
    calls: list[str] = []
    echo_count = {"n": 0}

    def fake_get(url, **kwargs):
        calls.append(url)
        if "ipify" in url:
            echo_count["n"] += 1
            return MagicMock(status_code=200, text="1.1.1.1" if echo_count["n"] == 1 else "9.9.9.9")
        return MagicMock(status_code=status_code, json=MagicMock(return_value=body if body is not None else {}))

    get = MagicMock(side_effect=fake_get)
    monkeypatch.setattr(proxy_provider.requests, "get", get)
    get.call_urls = calls  # type: ignore[attr-defined]
    return get


def test_reset_url_replaces_placeholders(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["src"] = {"api_url": "https://example.test/reset?token={token}&port={proxy}", "token": "abc"}
    url = proxy_provider._build_reset_url(
        rows["src"], {"url": "gw.test:40687", "username": "u", "password": "p"}
    )
    assert url == "https://example.test/reset?token=abc&port=gw.test:40687"


def test_token_placeholder_is_url_encoded(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["src"] = {"api_url": "https://example.test/reset?token={token}", "token": "a b&c"}
    url = proxy_provider._build_reset_url(rows["src"], None)
    assert url == "https://example.test/reset?token=a%20b%26c"


def test_reset_proxy_confirms_a_new_egress_ip(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["src"] = {"api_url": "https://example.test/reset", "token": "tok"}
    get = _reset_ok(monkeypatch)

    ok, ip = proxy_provider.reset_proxy("src", rows["src"], {"url": "gw.test:40687", "username": "", "password": ""})

    assert ok is True
    assert ip == "9.9.9.9"
    # 1 lần đo IP cũ + 1 lần gọi link reset + 1 lần đo IP mới
    assert get.call_count == 3


def test_reset_proxy_fails_when_the_ip_does_not_change(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["src"] = {"api_url": "https://example.test/reset", "token": "tok"}
    monkeypatch.setattr(proxy_provider, "_egress_ip", lambda proxy, timeout: "1.1.1.1")
    monkeypatch.setattr(proxy_provider, "_wait_new_ip", lambda proxy, old, deadline: None)
    monkeypatch.setattr(proxy_provider.requests, "get", lambda url, **kw: MagicMock(status_code=200, json=dict))

    ok, ip = proxy_provider.reset_proxy("src", rows["src"], {"url": "gw.test:1"})
    assert (ok, ip) == (False, None)


def test_reset_proxy_rejects_an_error_response(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["src"] = {"api_url": "https://example.test/reset", "token": "tok"}
    _reset_ok(monkeypatch, status_code=403, body={"status": "error", "message": "Token sai"})

    ok, ip = proxy_provider.reset_proxy("src", rows["src"], {"url": "gw.test:1"})
    assert (ok, ip) == (False, None)


def test_lease_returns_the_port_with_a_fresh_ip(monkeypatch, provider) -> None:
    rows, proxy_rows = provider
    rows["us"] = {"api_url": "https://example.test/reset", "token": "tok"}
    proxy_rows["us"] = [{"url": "gw.test:40687", "username": "user-1", "password": "pass"}]
    _reset_ok(monkeypatch)

    assert proxy_provider.lease_proxy("us") == {
        "host": "gw.test",
        "port": 40687,
        "username": "user-1",
        "password": "pass",
    }


def test_lease_without_any_port_is_none(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["us"] = {"api_url": "https://example.test/reset", "token": "tok"}
    get = _reset_ok(monkeypatch)

    assert proxy_provider.lease_proxy("us") is None
    get.assert_not_called()


def test_static_port_never_calls_the_reset_link(monkeypatch, provider) -> None:
    rows, proxy_rows = provider
    rows["us"] = {"api_url": "https://example.test/reset", "token": "tok"}
    proxy_rows["us"] = [{"url": "gw.test:40687", "username": "u", "password": "p"}]
    get = _reset_ok(monkeypatch)

    assert proxy_provider.static_port("us") == {"host": "gw.test", "port": 40687, "username": "u", "password": "p"}
    get.assert_not_called()


def test_static_port_falls_back_to_the_token_of_a_fixed_port_source(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["us"] = {"api_url": "https://example.test/reset", "token": "gw.test:40687:u:p"}
    get = _reset_ok(monkeypatch)

    assert proxy_provider.static_port("us") == {"host": "gw.test", "port": 40687, "username": "u", "password": "p"}
    get.assert_not_called()


def test_reset_provider_for_proxy_url_uses_the_provider_key_column(monkeypatch, provider) -> None:
    rows, _ = provider
    rows["us"] = {"api_url": "https://example.test/reset", "token": "tok"}
    monkeypatch.setattr("social_crawler.db.proxies.meta_accounts_on_proxy_url", lambda _url: [])
    monkeypatch.setattr(
        "social_crawler.db.proxies.provider_key_for_proxy_url", lambda url: "us" if url == "h:1" else None
    )
    monkeypatch.setattr(
        "social_crawler.db.proxies.proxy_row_for_url", lambda url: {"url": url, "username": "", "password": ""}
    )
    _reset_ok(monkeypatch)

    assert proxy_provider.reset_provider_for_proxy_url("other:2") is None
    assert proxy_provider.reset_provider_for_proxy_url("h:1") == "us"
