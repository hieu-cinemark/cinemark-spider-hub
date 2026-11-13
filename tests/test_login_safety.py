"""Review đăng nhập 02/11/2026: Facebook/Threads không tự chọn đường mạng khác, một tài khoản một thao tác, mọi đường xác thực
dùng cùng trình duyệt, task nền không đăng nhập vào tài khoản đang nghỉ, mỗi POST thật đều bị đếm. Không dùng tài khoản/mạng thật."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from social_crawler.accounts import nurture
from social_crawler.db.proxies import AccountLookupError
from social_crawler.runtime import account_lease, pool
from social_crawler.spiders.common import comet_graphql_client as cgc
from social_crawler.spiders.facebook.auth import browser_interaction
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient
from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture
def no_fallback_proxy(monkeypatch):
    """Nếu code còn fallback thì nhận proxy giả này và test fail."""
    monkeypatch.setattr(pool, "claim_proxy", lambda platform: {"url": "other:1", "platform": platform})
    monkeypatch.setattr(pool, "platform_has_any_proxy", lambda platform: False)


def _lookup_fails(platform, key):
    raise AccountLookupError("db down")


@pytest.mark.parametrize("required", [True, False])
def test_db_error_never_falls_back_on_meta(monkeypatch, no_fallback_proxy, required):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", _lookup_fails)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("facebook", "a@x", required=required)


@pytest.mark.parametrize("required", [True, False])
def test_missing_account_row_never_falls_back_on_meta(monkeypatch, no_fallback_proxy, required):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda platform, key: None)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("threads", "a@x", required=required)


def test_required_never_returns_none_on_meta_even_when_db_says_no_proxy(monkeypatch, no_fallback_proxy):
    """platform_has_any_proxy trả False cả khi DB lỗi - trước đó comet phát lại cookie không proxy."""
    assert pool._unavailable("facebook", False) is None
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool._unavailable("facebook", True)
    assert pool._unavailable("tiktok", True) is None


def test_tiktok_keeps_previous_fallback(monkeypatch, no_fallback_proxy):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda platform, key: None)
    assert pool.acquire_proxy_for_account("tiktok", "a")["url"] == "other:1"


def _dead_pin(monkeypatch):
    monkeypatch.setattr(pool, "get_account_proxy_assignment", lambda platform, key: (1, 7))
    monkeypatch.setattr(pool, "get_proxy_by_id", lambda proxy_id: None)
    monkeypatch.setattr(pool, "get_proxy_raw_status", lambda proxy_id: {"enabled": False, "consecutive_failures": 0})
    monkeypatch.setattr(pool, "get_setting", lambda name: 5)
    monkeypatch.setattr(pool.ip_switch, "claim", lambda *a, **k: True)
    repinned = []

    def repin(platform, row_id):
        repinned.append(row_id)
        return {"url": "new:1", "platform": platform}

    monkeypatch.setattr(pool, "pin_account_to_free_proxy", repin)
    return repinned


def test_dead_pin_stops_instead_of_silent_repin(monkeypatch):
    repinned = _dead_pin(monkeypatch)
    monkeypatch.delenv("META_AUTO_REPIN", raising=False)
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.acquire_proxy_for_account("facebook", "a@x", required=True)
    assert repinned == []


def test_tiktok_dead_pin_is_not_repinned_either(monkeypatch):
    """Gán proxy là cố định cho mọi nền tảng (12/11/2026) - trước đó TikTok vẫn tự ghim lại, kể cả khi DB lỗi."""
    repinned = _dead_pin(monkeypatch)
    monkeypatch.delenv("META_AUTO_REPIN", raising=False)
    monkeypatch.setattr(pool, "get_proxy_raw_status", lambda proxy_id: None)  # DB lỗi / dòng mất
    assert pool.acquire_proxy_for_account("tiktok", "dev1") is None
    assert repinned == []


def test_auto_repin_is_opt_in(monkeypatch):
    repinned = _dead_pin(monkeypatch)
    monkeypatch.setenv("META_AUTO_REPIN", "true")
    assert pool.acquire_proxy_for_account("facebook", "a@x", required=True)["url"] == "new:1"
    assert repinned == [1]


class _Resp:
    def __init__(self, status_code=200, text="1.2.3.4\n"):
        self.status_code, self.text = status_code, text


class _Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def set_nx(self, key, value, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def delete_if_equals(self, key, value):
        if self.data.get(key) == value:
            del self.data[key]
            return True
        return False

    def expire_if_equals(self, key, value, ttl_seconds):
        return self.data.get(key) == value


PROXY = {"server": "http://gw:1", "username": "u", "password": "p"}


def test_egress_recorded_and_failure_blocks(monkeypatch):
    seen = {}

    def get(url, proxies, timeout):
        seen.update(proxies)
        return _Resp()

    monkeypatch.setattr(pool.curl_requests, "get", get)
    cache = _Cache()
    assert pool.check_login_egress("facebook", "A@x", PROXY, cache) == "1.2.3.4"
    assert seen["https"] == "http://u:p@gw:1"
    assert cache.data["proxy_ip:account_egress:a@x"]["ip"] == "1.2.3.4"
    monkeypatch.setattr(pool.curl_requests, "get", lambda url, proxies, timeout: _Resp(502, ""))
    with pytest.raises(pool.ProxyPoolExhaustedError):
        pool.check_login_egress("facebook", "a@x", PROXY, cache)


@pytest.fixture
def lease_clean():
    account_lease._held.clear()
    yield
    for held in account_lease._held.values():
        held.stop.set()
    account_lease._held.clear()


def _other_process():
    """Tiến trình khác: cùng Redis, không cùng bộ nhớ."""
    saved = dict(account_lease._held)
    account_lease._held.clear()
    return saved


def test_lease_excludes_other_process_and_is_reentrant(lease_clean):
    cache = _Cache()
    account_lease.acquire("facebook", "A@x", "manual_login", cache)
    account_lease.acquire("facebook", "a@x", "bootstrap", cache)  # cùng tiến trình
    saved = _other_process()
    with pytest.raises(account_lease.AccountBusyError, match="manual_login"):
        account_lease.acquire("facebook", "a@x", "crawl", cache)
    account_lease._held.update(saved)
    account_lease.release("facebook", "a@x")
    assert account_lease.holder("facebook", "a@x", cache)  # còn một tham chiếu
    account_lease.release("facebook", "a@x")
    assert account_lease.holder("facebook", "a@x", cache) is None
    account_lease.acquire("facebook", "a@x", "crawl", cache)
    account_lease.release("facebook", "a@x")


def test_release_does_not_delete_a_lease_taken_over(lease_clean):
    cache = _Cache()
    account_lease.acquire("facebook", "a@x", "crawl", cache)
    key = "account_lease:facebook:a@x"
    cache.data[key] = {"token": "someone-else"}  # lease đã hết hạn, người khác nhận
    account_lease.release("facebook", "a@x")
    assert cache.data[key] == {"token": "someone-else"}


def test_redis_failure_counts_as_busy(lease_clean):
    class Down(_Cache):
        def set_nx(self, key, value, ttl_seconds):
            return False  # RedisCache.set_nx trả False khi Redis lỗi

        def get(self, key):
            raise pool.redis.RedisError("down")

    with pytest.raises(account_lease.AccountBusyError):
        account_lease.acquire("facebook", "a@x", "crawl", Down())


def test_lost_lease_is_detected_by_renewal(lease_clean, monkeypatch):
    monkeypatch.setattr(account_lease, "RENEW_EVERY_SECONDS", 0.01)
    cache = _Cache()
    account_lease.acquire("facebook", "a@x", "crawl", cache)
    cache.data.clear()
    held = account_lease._held[("facebook", "a@x")]
    for _ in range(200):
        if held.lost:
            break
        held.stop.wait(0.01)
    assert account_lease.lost("facebook", "a@x")
    with pytest.raises(account_lease.AccountBusyError):
        account_lease.acquire("facebook", "a@x", "crawl", cache)


def test_busy_is_requeued_without_penalty():
    assert issubclass(account_lease.AccountBusyError, pool.ProxyPoolExhaustedError)
    with pytest.raises(SystemExit) as exit_info:
        pool.abort_spider_for_network_error(account_lease.AccountBusyError("busy"))
    assert exit_info.value.code == pool.PROXY_EXHAUSTED_EXIT_CODE


def test_tiktok_and_default_slot_have_no_lease(lease_clean):
    cache = _Cache()
    account_lease.acquire("tiktok", "a", "crawl", cache)
    account_lease.acquire("facebook", "default", "crawl", cache)
    assert cache.data == {} and account_lease._held == {}


def _imports_of(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}


@pytest.mark.parametrize(
    "path",
    [
        "social_crawler/spiders/facebook/auth/bootstrap.py",
        "social_crawler/spiders/threads/auth/bootstrap.py",
        "social_crawler/accounts/nurture.py",
    ],
)
def test_every_auth_path_uses_patchright(path):
    imports = _imports_of(REPO / path)
    assert "patchright" in imports and "playwright" not in imports


def test_browser_launch_failure_is_not_hidden_by_fallback(monkeypatch):
    monkeypatch.setattr(browser_interaction, "CHROMIUM_CHANNEL", "chromium")
    calls = []

    class Chromium:
        def launch(self, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("Executable doesn't exist")

    class PW:
        chromium = Chromium()

    with pytest.raises(RuntimeError):
        browser_interaction.launch_chromium(PW(), headless=True)
    assert len(calls) == 1 and calls[0]["channel"] == "chromium"


def test_every_retry_post_is_throttled(monkeypatch):
    """Transient 503 x4 then 200 counts five physical POSTs toward throttle/budget."""
    monkeypatch.setattr(cgc.time, "sleep", lambda s: None)
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a, **k: None)

    class Session:
        posts = 0

        def post(self, *a, **k):
            Session.posts += 1
            return _Resp(503 if Session.posts < 5 else 200)

    client = cgc.CometGraphQLClient.__new__(cgc.CometGraphQLClient)
    client.PLATFORM = "facebook"
    client.GRAPHQL_URL = "https://example.invalid/"
    client.MAX_RETRIES = 5
    client.RETRY_BACKOFF_BASE_SECONDS = 0.0
    client.RETRY_BACKOFF_JITTER_SECONDS = 0.0
    client._proxy_cfg = None
    client._session = Session()
    client._adjust_interval = lambda stressed: None
    client._record_proxy_outcome_once = lambda success: None
    throttled = []
    client._throttle = lambda: throttled.append(1)
    assert client._post_with_retry({}, {}, {}).status_code == 200
    assert Session.posts == 5 and len(throttled) == 5


def test_lost_lease_stops_every_send_path(monkeypatch):
    """Kiểm tra nằm trong _throttle: POST GraphQL, GET Threads và GET HTML Facebook đều đi qua đó."""
    monkeypatch.setattr(cgc.account_lease, "lost", lambda platform, account: True)
    monkeypatch.setattr(cgc.ip_switch, "touch", lambda *a, **k: None)

    class Session:
        def post(self, *a, **k):
            raise AssertionError("không được gửi khi đã mất lease")

        get = post

    def client_of(cls):
        client = cls.__new__(cls)
        client.PLATFORM = "facebook"
        client.MAX_RETRIES = 3
        client._proxy_cfg = None
        client._account = "a@x"
        client._session = Session()
        client._cache = {"cookies": {}}
        client.GRAPHQL_URL = "https://example.invalid/"
        return client

    with pytest.raises(cgc.NetworkError) as err:
        client_of(cgc.CometGraphQLClient)._post_with_retry({}, {}, {})
    assert isinstance(err.value.__cause__, pool.ProxyPoolExhaustedError)
    with pytest.raises(cgc.NetworkError):
        client_of(ThreadsGraphQLClient)._get_with_retry("https://example.invalid/", {})
    fb = client_of(FacebookGraphQLClient)
    fb._html_headers = dict
    with pytest.raises(cgc.NetworkError):
        fb.resolve_page_id("https://example.invalid/page")


def test_threads_retry_get_is_throttled_each_time(monkeypatch):
    monkeypatch.setattr(cgc.time, "sleep", lambda s: None)

    class Session:
        gets = 0

        def get(self, *a, **k):
            Session.gets += 1
            return _Resp(429 if Session.gets < 3 else 200)

    client = ThreadsGraphQLClient.__new__(ThreadsGraphQLClient)
    client.MAX_RETRIES = 5
    client.RETRY_BACKOFF_BASE_SECONDS = 0.0
    client.RETRY_BACKOFF_JITTER_SECONDS = 0.0
    client._proxy_cfg = None
    client._session = Session()
    client._cache = {"cookies": {}}
    client._adjust_interval = lambda stressed: None
    client._record_proxy_outcome_once = lambda success: None
    throttled = []
    client._throttle = lambda: throttled.append(1)
    assert client._get_with_retry("https://example.invalid/", {}).status_code == 200
    assert Session.gets == 3 and len(throttled) == 3


class _Playwright:
    def __enter__(self):
        return object()

    def __exit__(self, *exc):
        return False


@pytest.fixture
def nurture_main(monkeypatch, lease_clean):
    """main() thật của nurture với một tài khoản, không trình duyệt, không ngủ."""
    cache = _Cache()
    calls = []
    monkeypatch.setattr(nurture, "RedisCache", lambda: cache)
    monkeypatch.setattr(nurture, "sync_playwright", _Playwright)
    monkeypatch.setattr(nurture.time, "sleep", lambda s: None)
    monkeypatch.setattr(nurture, "_iter_jobs", lambda *a, **k: [("threads", {"id": "1", "email": "a@x"})])
    monkeypatch.setattr(sys, "argv", ["nurture", "--platform", "threads"])
    return cache, calls


def test_nurture_retry_does_not_open_account_held_by_another_process(monkeypatch, nurture_main):
    cache, calls = nurture_main
    cache.data["account_lease:threads:1"] = {"token": "other", "operation": "manual_login"}

    def fake_one(pw, **kwargs):
        calls.append(kwargs["account"]["id"])
        return "ok"

    monkeypatch.setattr(nurture, "nurture_one", fake_one)
    assert nurture.main() == 0
    assert calls == []  # bận ở lượt đầu và lượt thử lại


def test_nurture_retry_runs_under_lease(monkeypatch, nurture_main):
    cache, calls = nurture_main
    results = iter(["skipped", "ok"])  # lượt đầu cổng proxy bận

    def fake_one(pw, **kwargs):
        calls.append(cache.data["account_lease:threads:1"]["operation"])
        return next(results)

    monkeypatch.setattr(nurture, "nurture_one", fake_one)
    assert nurture.main() == 0
    assert calls == ["nurture", "nurture"]
    assert "account_lease:threads:1" not in cache.data  # đã nhả sau cả hai lượt


def _lost_run(monkeypatch, operation, fail=None):
    """Chạy _renew_loop đồng bộ: lần gia hạn đầu đã thấy mất quyền (hoặc Redis lỗi)."""
    exits = []
    monkeypatch.setattr(account_lease, "_exit", exits.append)
    monkeypatch.setattr(account_lease, "_EXIT_FLUSH_SECONDS", 0)
    monkeypatch.setattr(account_lease, "RENEW_EVERY_SECONDS", 0)

    class Cache(_Cache):
        def expire_if_equals(self, key, value, ttl_seconds):
            if fail:
                raise fail
            return False

    held = account_lease._Held(cache=Cache(), key="k", value={"operation": operation})
    return held, exits


@pytest.mark.parametrize("operation", ["bootstrap", "nurture", "manual_login", "cookie_check", "auto_login"])
def test_lost_lease_ends_process_of_browser_operations(monkeypatch, operation):
    held, exits = _lost_run(monkeypatch, operation)
    account_lease._renew_loop(held)
    assert held.lost and exits == [pool.PROXY_EXHAUSTED_EXIT_CODE]


def test_lost_lease_on_http_crawl_only_sets_the_flag(monkeypatch):
    held, exits = _lost_run(monkeypatch, "crawl")
    account_lease._renew_loop(held)
    assert held.lost and exits == []


def test_redis_outage_stops_before_the_lease_can_expire(monkeypatch):
    """Không gia hạn được: phải dừng trước TTL, để không có lúc tiến trình khác nhận tài khoản khi browser này còn chạy."""
    held, exits = _lost_run(monkeypatch, "nurture", fail=pool.redis.ConnectionError("down"))
    monkeypatch.setattr(account_lease, "RENEW_EVERY_SECONDS", 60)
    now = [0.0]

    class Stop:
        def wait(self, seconds):
            now[0] += seconds
            return False

    held.stop = Stop()
    monkeypatch.setattr(account_lease.time, "monotonic", lambda: now[0])
    account_lease._renew_loop(held)
    assert exits == [pool.PROXY_EXHAUSTED_EXIT_CODE]
    assert now[0] < account_lease.LEASE_TTL_SECONDS


def test_lost_lease_really_ends_the_process():
    code = (
        "import time\n"
        "from social_crawler.runtime import account_lease as al\n"
        "al.RENEW_EVERY_SECONDS, al._EXIT_FLUSH_SECONDS = 0.05, 0\n"
        "class C:\n"
        "    def set_nx(self, *a): return True\n"
        "    def expire_if_equals(self, *a): return False\n"
        "    def delete_if_equals(self, *a): return False\n"
        "al.acquire('facebook', 'a@x', 'nurture', C())\n"
        "time.sleep(30)\n"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True, timeout=25, check=False)
    assert result.returncode == pool.PROXY_EXHAUSTED_EXIT_CODE, result.stderr[-500:]


def test_checkpoint_is_not_counted_as_proxy_failure(monkeypatch):
    outcomes, proxy_results = [], []
    monkeypatch.setattr(nurture, "record_account_outcome", lambda *a, **k: outcomes.append(k))
    monkeypatch.setattr(nurture.pool, "release_proxy", lambda row, success: proxy_results.append(success))

    class Page:
        url = "https://www.facebook.com/checkpoint/1"

    assert nurture._handle_checkpoint(Page(), "facebook", {"id": "1"}, "a@x", "load", {"url": "p"}) == "failed"
    assert outcomes[0]["hard_failure"] is True  # tài khoản vẫn bị đánh dấu
    assert proxy_results == [True]
