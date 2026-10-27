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
    monkeypatch.setattr(proxy_provider, "list_provider_keys", lambda: sorted(k for k in rows if k != "_used"))
    used: list[tuple[str, str]] = []
    monkeypatch.setattr(proxy_provider, "record_provider_use", lambda key, status: used.append((key, status)))
    rows["_used"] = used  # type: ignore[assignment]
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


def test_static_port_failed_reset_keeps_the_current_ip(monkeypatch, provider) -> None:
    # Reset bị từ chối không có nghĩa cổng hỏng - trả lại cổng (IP cũ) để chỗ gọi không rơi về pool
    # platform_proxies ở quốc gia khác.
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "ip_allowlist": False}
    _responses(monkeypatch, {"result": "error", "statusCode": 500, "content": "Không thể kết nối đến server gốc"})
    assert proxy_provider.get_new_proxy(provider_key="us") == {
        "host": "h",
        "port": 1,
        "username": "u",
        "password": "p",
    }


def test_rejected_token_is_recorded_for_the_dashboard(monkeypatch, provider) -> None:
    provider["p"] = {"api_url": "https://example.test/get_new", "token": "tok", "ip_allowlist": False}
    _responses(monkeypatch, {"status": "ERROR", "statusCode": 400, "error": "Token chứa ký tự không hợp lệ"})

    assert proxy_provider.get_new_proxy(provider_key="p") is None
    assert provider["_used"] == [("p", "error: Token chứa ký tự không hợp lệ")]


def test_static_port_proxy_never_calls_the_reset_link(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "ip_allowlist": False}
    get = _responses(monkeypatch)

    assert proxy_provider.static_port_proxy("us") == {"host": "h", "port": 1, "username": "u", "password": "p"}
    get.assert_not_called()
    provider["p"] = {"api_url": "https://example.test/get_new", "token": "tok", "ip_allowlist": False}
    assert proxy_provider.static_port_proxy("p") is None


def test_reset_provider_for_proxy_url_matches_the_owning_port(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "ip_allowlist": False}
    provider["p"] = {"api_url": "https://example.test/get_new", "token": "tok", "ip_allowlist": False}
    get = _responses(monkeypatch, {"result": "success", "ipreal": "9.9.9.9"})

    assert proxy_provider.reset_provider_for_proxy_url("other:2") is None
    assert proxy_provider.reset_provider_for_proxy_url("h:1") == "us"
    assert get.call_count == 1


# --- webtui: giới hạn 60 giây giữa hai lần xoay (27/10/2026) -------------------------------------------------


@pytest.fixture
def webtui(monkeypatch: pytest.MonkeyPatch, provider):
    """Một cổng webtui (proxy_id 7), mốc xoay lưu trong một "Redis" giả dùng chung, đồng hồ giả."""
    clock = {"now": 1_000_000.0, "slept": []}
    store: dict[str, object] = {}

    class FakeRedis:
        def get(self, key):
            return store.get(key)

        def set(self, key, value, ttl_seconds=None):
            store[key] = value

    import social_crawler.clients.redis as redis_module

    monkeypatch.setattr(redis_module, "RedisCache", FakeRedis)
    monkeypatch.setattr(proxy_provider.time, "time", lambda: clock["now"])
    monkeypatch.setattr(proxy_provider.time, "monotonic", lambda: clock["now"])

    def sleep(seconds):
        clock["slept"].append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(proxy_provider.time, "sleep", sleep)
    monkeypatch.setattr(
        proxy_provider, "_webtui_find", lambda *a: {"id": 7, "ip_proxy": "42.1.92.100", "proxy_port": 1052}
    )
    proxy_provider._webtui_last_rotate.clear()
    return clock, store


def _webtui_calls(monkeypatch, *rotate_bodies, new_ip="14.1.1.1"):
    bodies = list(rotate_bodies)

    def fake_get(url, params=None, timeout=None):
        if params["action"] == "rotate_ip":
            return MagicMock(json=MagicMock(return_value=bodies.pop(0)))
        return MagicMock(raise_for_status=lambda: None, json=lambda: {"success": True, "proxy": {"ip_public": new_ip}})

    monkeypatch.setattr(proxy_provider.requests, "get", fake_get)


def test_webtui_waits_out_a_recent_rotation_from_another_process(monkeypatch, webtui) -> None:
    clock, store = webtui
    store["webtui:last_rotate:7"] = clock["now"] - 20  # tiến trình khác vừa xoay 20 giây trước
    _webtui_calls(monkeypatch, {"success": True, "old_ip": "14.0.0.1"})
    status, ip = proxy_provider._rotate_webtui_ip(
        "webtui", {"api_url": "https://webtui", "token": "t"}, "42.1.92.100:1052"
    )
    assert (status, ip) == (True, "14.1.1.1")
    assert clock["slept"][0] == pytest.approx(proxy_provider._WEBTUI_MIN_ROTATE_GAP_SECONDS - 20)
    assert store["webtui:last_rotate:7"] >= 1_000_000.0  # mốc mới ghi cho mọi tiến trình


def test_webtui_retries_once_after_provider_wait_message(monkeypatch, webtui) -> None:
    clock, _ = webtui
    _webtui_calls(monkeypatch, {"success": False, "message": "Chờ 34s"}, {"success": True, "old_ip": "14.0.0.1"})
    status, _ = proxy_provider._rotate_webtui_ip(
        "webtui", {"api_url": "https://webtui", "token": "t"}, "42.1.92.100:1052"
    )
    assert status is True
    assert 36 in clock["slept"]


def test_webtui_gives_up_on_other_rejections(monkeypatch, webtui) -> None:
    _webtui_calls(monkeypatch, {"success": False, "message": "Proxy hết hạn"})
    status, ip = proxy_provider._rotate_webtui_ip(
        "webtui", {"api_url": "https://webtui", "token": "t"}, "42.1.92.100:1052"
    )
    assert (status, ip) == (False, None)
