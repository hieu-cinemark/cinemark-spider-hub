import pytest


@pytest.fixture(autouse=True)
def _no_db_for_new_account_ramp(monkeypatch):
    """search_budget.ramp_factor tra danh sách tài khoản mới trong DB thật ở mỗi request - test không được chạm DB.
    Test của phần tăng tải tự đặt lại _new_account_keys."""
    from social_crawler.runtime import search_budget

    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: set())


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
