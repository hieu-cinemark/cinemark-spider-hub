"""clients/proxy_provider.get_new_proxy với request HTTP giả - gói xin-IP-mới (API trả "host:port:user:pass")
và gói cổng xoay cố định (token = PORT XOAY, api_url = LINK RESET)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from social_crawler.clients import proxy_provider

SETTINGS = {
    "provider_request_timeout_seconds": 10,
    "provider_min_get_new_interval_seconds": 0,
    "provider_max_cooldown_wait_seconds": 0,
}


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch):
    rows: dict[str, dict] = {}
    monkeypatch.setattr(proxy_provider, "get_provider", lambda key: rows.get(key))
    monkeypatch.setattr(proxy_provider, "get_setting", lambda key: SETTINGS[key])
    monkeypatch.setattr(proxy_provider.time, "sleep", lambda _seconds: None)
    proxy_provider._last_get_new_at.clear()
    return rows


def _responses(monkeypatch: pytest.MonkeyPatch, *bodies: dict) -> MagicMock:
    get = MagicMock(side_effect=[MagicMock(json=MagicMock(return_value=body)) for body in bodies])
    monkeypatch.setattr(proxy_provider.requests, "get", get)
    return get


def test_get_new_api_returns_the_leased_proxy(monkeypatch, provider) -> None:
    provider["p"] = {"api_url": "https://example.test/get_new", "token": "tok", "ip_allowlist": False}
    get = _responses(monkeypatch, {"status": "SUCCESS", "proxy": "1.2.3.4:8000:u:pw"})

    assert proxy_provider.get_new_proxy(provider_key="p") == {
        "host": "1.2.3.4",
        "port": 8000,
        "username": "u",
        "password": "pw",
    }
    assert get.call_args.kwargs["params"] == {"token": "tok"}


def test_static_port_calls_the_reset_link_and_keeps_the_port(monkeypatch, provider) -> None:
    provider["us"] = {
        "api_url": "https://example.test/reset/secret",
        "token": "gw.example.test:40687:user-1:pass",
        "ip_allowlist": False,
    }
    get = _responses(monkeypatch, {"result": "success", "statusCode": 200, "ipreal": "9.9.9.9"})

    assert proxy_provider.get_new_proxy(provider_key="us") == {
        "host": "gw.example.test",
        "port": 40687,
        "username": "user-1",
        "password": "pass",
    }
    assert get.call_args.args == ("https://example.test/reset/secret",)
    assert "params" not in get.call_args.kwargs  # link reset không nhận token


def test_static_port_waits_out_cooldown_once(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "ip_allowlist": False}
    _responses(
        monkeypatch,
        {"result": "error", "statusCode": 405, "content": "Thời gian đổi proxy còn 12 giây"},
        {"result": "success", "ipreal": "9.9.9.9"},
    )
    assert proxy_provider.get_new_proxy(provider_key="us") is not None


def test_static_port_gives_up_on_other_errors(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "ip_allowlist": False}
    _responses(monkeypatch, {"result": "error", "statusCode": 500, "content": "Không thể kết nối đến server gốc"})
    assert proxy_provider.get_new_proxy(provider_key="us") is None
