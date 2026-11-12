"""clients/proxy_provider - registry adapter: gói cổng xoay cố định (kind=rotate_url) và
gói tĩnh (kind=static). Mọi HTTP đều giả lập, không dùng token/link thật."""

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
    monkeypatch.setattr(proxy_provider, "get_provider", lambda key: rows.get(key))
    monkeypatch.setattr(proxy_provider, "get_setting", lambda key: SETTINGS[key])
    monkeypatch.setattr(proxy_provider.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(proxy_provider, "list_provider_keys", lambda: sorted(k for k in rows if k != "_used"))
    used: list[tuple[str, str]] = []
    monkeypatch.setattr(proxy_provider, "record_provider_use", lambda key, status: used.append((key, status)))
    rows["_used"] = used  # type: ignore[assignment]
    proxy_provider._last_call_at.clear()
    return rows


def _responses(monkeypatch: pytest.MonkeyPatch, *bodies: dict) -> MagicMock:
    get = MagicMock(side_effect=[MagicMock(json=MagicMock(return_value=body)) for body in bodies])
    monkeypatch.setattr(proxy_provider.requests, "get", get)
    return get


def test_rotate_url_adapter_is_chosen_by_kind(monkeypatch, provider) -> None:
    provider["us"] = {
        "api_url": "https://example.test/reset/secret",
        "token": "gw.example.test:40687:user-1:pass",
        "kind": "rotate_url",
    }
    assert isinstance(proxy_provider.adapter_for(provider["us"]), proxy_provider.RotateUrlAdapter)


def test_legacy_kind_is_used_when_the_column_is_empty(monkeypatch, provider) -> None:
    provider["old"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "kind": ""}
    assert isinstance(proxy_provider.adapter_for(provider["old"]), proxy_provider.RotateUrlAdapter)
    # Token không phải host:port:user:pass và URL không có dấu hiệu nhà cung cấp nào.
    provider["stat"] = {"api_url": "", "token": "tok", "kind": ""}
    assert isinstance(proxy_provider.adapter_for(provider["stat"]), proxy_provider.StaticAdapter)


def test_lease_rotates_the_ip_and_keeps_the_port(monkeypatch, provider) -> None:
    provider["us"] = {
        "api_url": "https://example.test/reset/secret",
        "token": "gw.example.test:40687:user-1:pass",
        "kind": "rotate_url",
    }
    get = _responses(monkeypatch, {"result": "success", "statusCode": 200, "ipreal": "9.9.9.9"})

    assert proxy_provider.lease_proxy("us") == {
        "host": "gw.example.test",
        "port": 40687,
        "username": "user-1",
        "password": "pass",
    }
    assert get.call_args.args == ("https://example.test/reset/secret",)
    assert "params" not in get.call_args.kwargs  # link reset không nhận token


def test_lease_waits_out_cooldown_once(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "kind": "rotate_url"}
    _responses(
        monkeypatch,
        {"result": "error", "statusCode": 405, "content": "Thời gian đổi proxy còn 12 giây"},
        {"result": "success", "ipreal": "9.9.9.9"},
    )
    assert proxy_provider.lease_proxy("us") is not None


def test_lease_of_a_static_provider_is_none(monkeypatch, provider) -> None:
    provider["p"] = {"api_url": "", "token": "h:1:u:p", "kind": "static"}
    get = _responses(monkeypatch)
    assert proxy_provider.lease_proxy("p") is None
    get.assert_not_called()


def test_reset_provider_for_proxy_url_matches_the_owning_port(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "kind": "rotate_url"}
    provider["p"] = {"api_url": "", "token": "tok", "kind": "static"}
    get = _responses(monkeypatch, {"result": "success", "ipreal": "9.9.9.9"})
    # Không port nào đang chở tài khoản Meta - test không được chạm DB thật (CI không có DATABASE_URL).
    monkeypatch.setattr("social_crawler.db.proxies.meta_accounts_on_proxy_url", lambda _url: [])

    assert proxy_provider.reset_provider_for_proxy_url("other:2") is None
    assert proxy_provider.reset_provider_for_proxy_url("h:1") == "us"
    assert get.call_count == 1


def test_static_port_proxy_never_calls_the_reset_link(monkeypatch, provider) -> None:
    provider["us"] = {"api_url": "https://example.test/reset", "token": "h:1:u:p", "kind": "rotate_url"}
    get = _responses(monkeypatch)

    assert proxy_provider.static_port_proxy("us") == {"host": "h", "port": 1, "username": "u", "password": "p"}
    get.assert_not_called()
    provider["p"] = {"api_url": "", "token": "tok", "kind": "static"}
    assert proxy_provider.static_port_proxy("p") is None


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
    # _find cần danh sách my_proxies thật của tài khoản webtui - giả lập ở đây.
    monkeypatch.setattr(
        proxy_provider.WebtuiAdapter,
        "_find",
        lambda self, *a: {"id": 7, "ip_proxy": "42.1.92.100", "proxy_port": 1052},
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


def _webtui_adapter():
    return proxy_provider.WebtuiAdapter()


def test_webtui_waits_out_a_recent_rotation_from_another_process(monkeypatch, webtui) -> None:
    clock, store = webtui
    store["webtui:last_rotate:7"] = clock["now"] - 20  # tiến trình khác vừa xoay 20 giây trước
    _webtui_calls(monkeypatch, {"success": True, "old_ip": "14.0.0.1"})
    status, ip = _webtui_adapter().rotate(
        "webtui", {"api_url": "https://webtui", "token": "t", "kind": "webtui"}, "42.1.92.100:1052"
    )
    assert (status, ip) == (True, "14.1.1.1")
    assert clock["slept"][0] == pytest.approx(proxy_provider._WEBTUI_MIN_ROTATE_GAP_SECONDS - 20)
    assert store["webtui:last_rotate:7"] >= 1_000_000.0  # mốc mới ghi cho mọi tiến trình


def test_webtui_retries_once_after_provider_wait_message(monkeypatch, webtui) -> None:
    clock, _ = webtui
    _webtui_calls(monkeypatch, {"success": False, "message": "Chờ 34s"}, {"success": True, "old_ip": "14.0.0.1"})
    status, _ = _webtui_adapter().rotate(
        "webtui", {"api_url": "https://webtui", "token": "t", "kind": "webtui"}, "42.1.92.100:1052"
    )
    assert status is True
    assert 36 in clock["slept"]


def test_webtui_gives_up_on_other_rejections(monkeypatch, webtui) -> None:
    _webtui_calls(monkeypatch, {"success": False, "message": "Proxy hết hạn"})
    status, ip = _webtui_adapter().rotate(
        "webtui", {"api_url": "https://webtui", "token": "t", "kind": "webtui"}, "42.1.92.100:1052"
    )
    assert (status, ip) == (False, None)
