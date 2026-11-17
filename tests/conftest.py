import pytest


@pytest.fixture(autouse=True)
def _no_real_postgres(monkeypatch):
    """Test phải tự cung cấp DB giả, kể cả test chính sách phiên."""
    import psycopg

    def blocked(*args, **kwargs):
        raise AssertionError("Test attempted a real Postgres connection; mock the database first")

    monkeypatch.setattr(psycopg, "connect", blocked)


@pytest.fixture(autouse=True)
def _no_real_telegram(monkeypatch):
    """Không test nào được gửi tin Telegram thật: `.env` của máy dev có token thật nên log `telegram=True` trong test từng bắn
    cảnh báo vào nhóm vận hành (17/11/2026 gặp 429 vì bị gửi dồn). Tắt ở tầng cấu hình của client - `telegram_enabled()` thành False."""
    from social_crawler.clients import telegram

    monkeypatch.setattr(telegram, "TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr(telegram, "TELEGRAM_CHAT_ID", "")


@pytest.fixture(autouse=True)
def _no_db_for_new_account_ramp(monkeypatch):
    """search_budget.ramp_factor tra danh sách tài khoản mới trong DB thật ở mỗi request - test không được chạm DB.
    Test của phần tăng tải tự đặt lại _new_account_keys."""
    from social_crawler.runtime import search_budget

    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: set())


@pytest.fixture(autouse=True)
def _no_db_for_session_policy(monkeypatch, request):
    """Dòng thời gian tài khoản đọc/ghi DB ở mỗi lần mở phiên - test không được chạm DB. Test của
    chính các phần này đặt marker real_session_policy."""
    if request.node.get_closest_marker("real_session_policy"):
        return
    from social_crawler.db import account_events

    monkeypatch.setattr(account_events, "record", lambda *a, **k: None)


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
    """Test ngân sách phải bật rõ; test khác không bao giờ được ngủ theo thời gian nghỉ thật."""
    from social_crawler.runtime import facebook_run_budget

    monkeypatch.setattr(facebook_run_budget, "MAX_REQUESTS", 0)
    monkeypatch.setattr(facebook_run_budget, "MAX_SECONDS", 0)


@pytest.fixture(autouse=True)
def _default_proxy_settings(monkeypatch):
    """Cấu hình cào (proxy_settings) đọc từ Postgres: trong test dùng bản chụp giá trị mặc định của cinemark-api
    (tests/fixtures/proxy_settings.json - chạy lại lệnh trong tests/README.md khi cinemark-api thêm khoá), không chạm DB."""
    import json
    from pathlib import Path

    from social_crawler.db import proxy_settings

    defaults = json.loads((Path(__file__).parent / "fixtures" / "proxy_settings.json").read_text())
    monkeypatch.setattr(proxy_settings, "_load_settings", lambda: dict(defaults))
    monkeypatch.setattr(proxy_settings, "_settings_cache", None)


@pytest.fixture
def set_setting(monkeypatch):
    """Đặt giá trị một khoá proxy_settings cho một test: `set_setting("fb_public_pages_max_pace", 8)`. Khoá khác giữ bản chụp mặc định."""
    import json
    from pathlib import Path

    from social_crawler.db import proxy_settings

    values = json.loads((Path(__file__).parent / "fixtures" / "proxy_settings.json").read_text())
    monkeypatch.setattr(proxy_settings, "get_proxy_settings", lambda: values)

    def _set(key, value):
        values[key] = value

    return _set
