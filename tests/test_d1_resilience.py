"""d1.query: câu đọc được thử lại, câu ghi chỉ thử lại khi chắc chắn chưa thực thi, lỗi liên tiếp mở ngắt mạch."""

import httpx
import pytest

from social_crawler.db import d1

OK = {"success": True, "result": [{"results": [{"n": 1}]}]}


@pytest.fixture
def env(monkeypatch):
    for name, value in (
        ("CLOUDFLARE_ACCOUNT_ID", "acc"),
        ("CLOUDFLARE_API_TOKEN", "tok"),
        ("CLOUDFLARE_D1_CONFIG_DATABASE_ID", "db"),
    ):
        monkeypatch.setenv(name, value)
    sleeps: list[float] = []
    monkeypatch.setattr(d1, "_sleep", sleeps.append)
    d1.reset_breaker()
    yield sleeps
    d1.reset_breaker()


def _serve(monkeypatch, *outcomes):
    """Mỗi lần gọi trả (hoặc ném) phần tử kế tiếp; trả về danh sách các request đã nhận."""
    seen: list[httpx.Request] = []
    queue = list(outcomes)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        outcome = queue.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return (
            httpx.Response(outcome, json=OK)
            if isinstance(outcome, int) and outcome == 200
            else httpx.Response(outcome, text="x")
        )

    monkeypatch.setattr(d1, "_client", httpx.Client(transport=httpx.MockTransport(handler)))
    return seen


def test_read_is_retried_after_server_error_then_succeeds(monkeypatch, env):
    seen = _serve(monkeypatch, 503, 502, 200)
    assert d1.query("SELECT 1") == [{"n": 1}]
    assert len(seen) == 3 and len(env) == 2


def test_read_gives_up_after_the_last_attempt(monkeypatch, env):
    seen = _serve(monkeypatch, 503, 503, 503)
    with pytest.raises(d1.D1Error, match="HTTP 503"):
        d1.query("SELECT 1")
    assert len(seen) == 3


def test_read_retries_network_timeouts(monkeypatch, env):
    seen = _serve(monkeypatch, httpx.ReadTimeout("slow"), 200)
    assert d1.query("SELECT 1") == [{"n": 1}]
    assert len(seen) == 2


def test_write_is_not_retried_after_read_timeout_or_server_error(monkeypatch, env):
    """Có thể đã thực thi: thử lại sẽ nhận tài khoản/proxy hai lần."""
    seen = _serve(monkeypatch, httpx.ReadTimeout("slow"))
    with pytest.raises(d1.D1Error):
        d1.query("UPDATE platform_accounts SET enabled = 0")
    assert len(seen) == 1
    seen = _serve(monkeypatch, 500)
    with pytest.raises(d1.D1Error, match="HTTP 500"):
        d1.query("UPDATE platform_accounts SET enabled = 0")
    assert len(seen) == 1


def test_write_is_retried_when_it_was_certainly_not_executed(monkeypatch, env):
    seen = _serve(monkeypatch, httpx.ConnectError("no route"), 429, 200)
    assert d1.query("UPDATE platform_accounts SET enabled = 0 RETURNING 1") == [{"n": 1}]
    assert len(seen) == 3


def test_sql_errors_and_client_errors_are_not_retried(monkeypatch, env):
    seen = _serve(monkeypatch, 403)
    with pytest.raises(d1.D1Error, match="HTTP 403"):
        d1.query("SELECT 1")
    assert len(seen) == 1


def test_breaker_opens_after_repeated_failures_and_fails_fast(monkeypatch, env):
    seen = _serve(monkeypatch, *([httpx.ConnectError("down")] * 20))
    with pytest.raises(d1.D1Error):
        d1.query("SELECT 1")  # 3 lần thử
    with pytest.raises(d1.D1Error):
        d1.query("SELECT 1")  # lần thứ 5 mở mạch giữa chừng
    sent = len(seen)
    with pytest.raises(d1.D1Error, match="circuit open"):
        d1.query("SELECT 1")
    assert len(seen) == sent  # không gọi mạng khi mạch mở


def test_breaker_recovers_after_the_open_window(monkeypatch, env):
    _serve(monkeypatch, *([httpx.ConnectError("down")] * 6), 200)
    for _ in range(2):
        with pytest.raises(d1.D1Error):
            d1.query("SELECT 1")
    monkeypatch.setattr(d1, "_open_until", 0.0)  # hết thời gian mở
    assert d1.query("SELECT 1") == [{"n": 1}]


def test_success_resets_the_failure_streak(monkeypatch, env):
    _serve(monkeypatch, 503, 200, 503, 200, 503, 200)
    for _ in range(3):
        d1.query("SELECT 1")
    assert d1._consecutive_failures == 0
