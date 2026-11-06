import pytest


@pytest.fixture(autouse=True)
def _no_db_for_new_account_ramp(monkeypatch):
    """search_budget.ramp_factor tra danh sách tài khoản mới trong DB thật ở mỗi request - test không được chạm DB.
    Test của phần tăng tải tự đặt lại _new_account_keys."""
    from social_crawler.runtime import search_budget

    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: set())


@pytest.fixture(autouse=True)
def _no_db_for_session_policy(monkeypatch, request):
    """Dòng thời gian và chính sách IP đều đọc/ghi DB ở mỗi lần mở phiên - test không được chạm DB. Test của
    chính các phần này đặt marker real_session_policy."""
    if request.node.get_closest_marker("real_session_policy"):
        return
    from social_crawler.db import account_events
    from social_crawler.runtime import lanes, pool

    monkeypatch.setattr(account_events, "record", lambda *a, **k: None)
    monkeypatch.setattr(pool, "meta_proxy_violation", lambda *a, **k: None)
    monkeypatch.setattr(pool, "watch_exit_ip", lambda *a, **k: True)
    monkeypatch.setattr(lanes, "facebook_accounts_blocked_by_proxy_policy", set)


@pytest.fixture(autouse=True)
def _no_real_redis(monkeypatch):
    """Không test nào được mở kết nối Redis thật: máy dev có sẵn Redis nên một test quên mock vẫn "pass" ở local rồi hỏng
    ở CI (hoặc âm thầm đọc/ghi state thật). Chặn ở tầng connect() để lộ ra ngay, kèm gợi ý mock. Test cần Redis giả thì dùng
    fakeredis hoặc monkeypatch lớp client - không bao giờ cần socket thật."""
    import redis.connection

    def _blocked(*args, **kwargs):
        raise RuntimeError(
            "Test mở kết nối Redis thật - mock get_redis_client / RedisCache (hoặc hàm gọi nó) trong test này."
        )

    monkeypatch.setattr(redis.connection.AbstractConnection, "connect", _blocked, raising=False)
    try:
        import redis.asyncio.connection as _aioconn

        monkeypatch.setattr(_aioconn.AbstractConnection, "connect", _blocked, raising=False)
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def _no_restart_ramp(monkeypatch):
    """Giai đoạn tăng dần có test riêng (test_restart_ramp.py); các test hạn mức khác tính trên đủ hạn mức."""
    from social_crawler.runtime import search_budget

    monkeypatch.setattr(search_budget, "RESTART_DAILY_CAPS", [])


@pytest.fixture(autouse=True)
def _no_real_run_rest(monkeypatch):
    """Budget tests opt in explicitly; other tests must never sleep for a production rest."""
    from social_crawler.runtime import facebook_run_budget

    monkeypatch.setattr(facebook_run_budget, "MAX_REQUESTS", 0)
    monkeypatch.setattr(facebook_run_budget, "MAX_SECONDS", 0)
