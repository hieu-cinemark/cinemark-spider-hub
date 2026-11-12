"""ZingProxy: mọi HTTP/Redis đều giả lập, không dùng key hay account thật."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from social_crawler.clients import proxy_provider, zingproxy
from social_crawler.runtime import ip_switch

RESET = "https://api.zingproxy.com/open/change-ip/test-secret-key"
PROXY = {"host": "gw.example.invalid", "port": 8000, "username": "u", "password": "p"}
PROVIDER = {"api_url": RESET, "token": "gw.example.invalid:8000:u:p", "kind": "zingproxy"}


class Cache:
    def __init__(self):
        self.data = {}

    def set_nx(self, key, value, ttl):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)

    def delete_if_equals(self, key, value):
        if self.data.get(key) == value:
            del self.data[key]


class Response:
    def __init__(self, status=200, *, body=None, text="", headers=None):
        self.status_code, self.body, self.text, self.headers = status, body, text, headers or {}

    def json(self):
        if self.body is None:
            raise ValueError("not JSON")
        return self.body


@pytest.fixture
def setup(monkeypatch):
    settings = {"provider_request_timeout_seconds": 1, "provider_max_cooldown_wait_seconds": 10}
    clock = {"now": 0, "sleeps": []}
    cache = Cache()

    def sleep(seconds):
        clock["sleeps"].append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(zingproxy, "get_setting", settings.__getitem__)
    monkeypatch.setattr(zingproxy, "RedisCache", lambda: cache)
    monkeypatch.setattr(zingproxy, "record_provider_use", MagicMock())
    monkeypatch.setattr(zingproxy.time, "sleep", sleep)
    monkeypatch.setattr(zingproxy.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(zingproxy, "SETTLE_SECONDS", 20)
    monkeypatch.setattr(proxy_provider, "list_provider_keys", lambda: ["zingproxy_fb"])
    monkeypatch.setattr(proxy_provider, "get_provider", lambda key: PROVIDER)
    return cache, clock


def responses(monkeypatch, *, ips=("1.1.1.1", "2.2.2.2"), replies=None):
    observed, resets, calls = iter(ips), iter(replies or [Response(body={"status": "success"})]), []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        if url == zingproxy.IP_ECHO_URL:
            result = next(observed, None)
            if isinstance(result, Exception):
                raise result
            return Response(text=result or "")
        assert url == RESET  # đúng link của cổng này, không truyền thêm token/query/header
        result = next(resets)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(zingproxy.requests, "get", get)
    return calls


def test_dispatch_rotates_matching_port_and_checks_actual_egress(monkeypatch, setup):
    calls = responses(monkeypatch, ips=("1.1.1.1", "1.1.1.1", "2.2.2.2"))
    assert proxy_provider.rotate_port_for_account_switch("gw.example.invalid:8000") == (True, "2.2.2.2")
    assert [url for url, _ in calls] == [zingproxy.IP_ECHO_URL, RESET, zingproxy.IP_ECHO_URL, zingproxy.IP_ECHO_URL]
    assert calls[0][1]["proxies"]["https"] == "http://u:p@gw.example.invalid:8000"
    assert calls[1][1] == {"timeout": 1.0, "allow_redirects": False}
    assert setup[0].data == {}  # nhả lock sau khi xác nhận


def test_other_port_never_calls_reset(monkeypatch, setup):
    get = MagicMock()
    monkeypatch.setattr(zingproxy.requests, "get", get)
    assert proxy_provider.rotate_port_for_account_switch("other.invalid:8000") == (None, None)
    get.assert_not_called()


def test_api_success_without_new_ip_is_not_success(monkeypatch, setup):
    responses(monkeypatch, ips=("1.1.1.1",) * 8)
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert setup[0].data == {}


def test_cannot_read_old_ip_does_not_reset(monkeypatch, setup):
    calls = responses(monkeypatch, ips=(requests.Timeout("proxy password"),))
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert len(calls) == 1 and calls[0][0] == zingproxy.IP_ECHO_URL


@pytest.mark.parametrize("bad_ip", ["<html>error</html>", "999.1.1.1", "127.0.0.1", "::1"])
def test_invalid_egress_never_resets(monkeypatch, setup, bad_ip):
    calls = responses(monkeypatch, ips=(bad_ip,))
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert len(calls) == 1


def test_cooldown_retries_once_then_verifies(monkeypatch, setup):
    calls = responses(
        monkeypatch,
        replies=[Response(429, headers={"Retry-After": "3"}), Response(body={"status": "success"})],
    )
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (True, "2.2.2.2")
    assert setup[1]["sleeps"][0] == 4
    assert sum(url == RESET for url, _ in calls) == 2


def test_long_cooldown_does_not_retry_early(monkeypatch, setup):
    calls = responses(monkeypatch, replies=[Response(body={"status": "error", "message": "Chờ 240 giây"})])
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert sum(url == RESET for url, _ in calls) == 1
    assert setup[1]["sleeps"] == []


def test_auth_error_does_not_retry(monkeypatch, setup):
    calls = responses(monkeypatch, replies=[Response(403, headers={"Retry-After": "1"})])
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert len(calls) == 2


def test_uncertain_reset_result_is_not_repeated_or_logged_with_secret(monkeypatch, setup):
    calls = responses(monkeypatch, replies=[requests.Timeout(RESET)])
    logger = MagicMock()
    monkeypatch.setattr(zingproxy, "logger", logger)
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert sum(url == RESET for url, _ in calls) == 1
    assert "test-secret-key" not in str(logger.mock_calls)
    assert "test-secret-key" not in str(zingproxy.record_provider_use.mock_calls)


@pytest.mark.parametrize(
    "url", ["http://api.zingproxy.com/open/change-ip/k", "https://evil.invalid/open/change-ip/k", RESET + "?x=1"]
)
def test_invalid_reset_endpoint_is_rejected(monkeypatch, setup, url):
    get = MagicMock()
    monkeypatch.setattr(zingproxy.requests, "get", get)
    assert zingproxy.rotate("z", {**PROVIDER, "api_url": url}, PROXY) == (False, None)
    get.assert_not_called()


def test_rotation_lock_is_required(monkeypatch, setup):
    monkeypatch.setattr(setup[0], "set_nx", lambda *a: False)
    get = MagicMock()
    monkeypatch.setattr(zingproxy.requests, "get", get)
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    get.assert_not_called()


def test_lost_lock_does_not_handoff_new_ip(monkeypatch, setup):
    calls = responses(monkeypatch)
    monkeypatch.setattr(setup[0], "get", lambda key: "another-owner")
    assert zingproxy.rotate("z", PROVIDER, PROXY) == (False, None)
    assert len(calls) == 2  # không đo/giao IP sau khi mất quyền xoay


def test_busy_facebook_port_does_not_rotate(monkeypatch, setup):
    class ActiveCache:
        def get(self, key):
            return "account-already-running"

    rotate = MagicMock()
    monkeypatch.setattr(proxy_provider, "rotate_port_for_account_switch", rotate)
    assert not ip_switch.claim("facebook", {"id": 7, "url": "gw.example.invalid:8000"}, "new-account", ActiveCache())
    rotate.assert_not_called()


def test_facebook_claim_uses_zingproxy_and_keeps_ip_for_same_account(monkeypatch, setup):
    calls = responses(monkeypatch)
    cache = setup[0]
    proxy = {"id": 7, "url": "gw.example.invalid:8000"}
    assert ip_switch.claim("facebook", proxy, "account-a", cache)
    assert cache.get(ip_switch.OWNER_KEY_TMPL.format(ip="2.2.2.2")) == "account-a"
    ip_switch.release("facebook", proxy, "account-a", cache)
    call_count = len(calls)
    assert ip_switch.claim("facebook", proxy, "account-a", cache)
    assert len(calls) == call_count


def test_facebook_claim_rejects_failed_zingproxy_rotation(monkeypatch, setup):
    responses(monkeypatch, ips=("1.1.1.1",) * 8)
    cache = setup[0]
    proxy = {"id": 7, "url": "gw.example.invalid:8000"}
    assert not ip_switch.claim("facebook", proxy, "account-b", cache)
    assert cache.get(ip_switch.LAST_KEY_TMPL.format(proxy_id=7)) is None
    assert cache.get(ip_switch.ACTIVE_KEY_TMPL.format(proxy_id=7)) is None
