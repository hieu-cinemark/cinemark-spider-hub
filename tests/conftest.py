import pytest


@pytest.fixture(autouse=True)
def _no_real_database(monkeypatch):
    """Test phải tự cung cấp DB giả (patch d1.query), kể cả test chính sách phiên: không bao giờ gọi D1 thật."""
    import httpx

    from social_crawler.db import d1

    def blocked(request):
        raise AssertionError("Test attempted a real D1 request; mock the database first (patch d1.query)")

    monkeypatch.setattr(d1, "_client", httpx.Client(transport=httpx.MockTransport(blocked)))


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
def _budget_defaults(_default_proxy_settings):
    """Giai đoạn tăng dần có test riêng (test_restart_ramp.py); các test hạn mức khác tính trên đủ hạn mức. Test ngân sách chạy liên
    tục phải bật rõ; test khác không bao giờ được ngủ theo thời gian nghỉ thật."""
    from tests.support import cfg

    cfg("fb_restart_daily_caps", [])
    cfg("fb_run_max_requests", 0)
    cfg("fb_run_max_seconds", 0)


@pytest.fixture(autouse=True)
def _default_proxy_settings(monkeypatch):
    """Cấu hình cào (proxy_settings) đọc từ D1: trong test dùng bản chụp giá trị mặc định của cinemark-api
    (tests/fixtures/proxy_settings.json - chạy lại lệnh trong tests/README.md khi cinemark-api thêm khoá), không chạm DB.
    Đổi một khoá trong test: `from tests.support import cfg; cfg("fb_daily_request_cap", 0)`."""
    import json
    from pathlib import Path

    from social_crawler.db import proxy_settings

    values = json.loads((Path(__file__).parent / "fixtures" / "proxy_settings.json").read_text())
    monkeypatch.setattr(proxy_settings, "_test_values", values, raising=False)
    monkeypatch.setattr(proxy_settings, "load_settings", lambda: dict(values))
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
