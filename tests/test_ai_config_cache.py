"""db/config.py: cấu hình AI được cache trong tiến trình (không đọc DB cho từng lời gọi AI) và đường đọc không chạy DDL."""

from social_crawler.db import config


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _Conn:
    def __init__(self, calls, row):
        self.calls, self.row = calls, row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.calls.append(sql)
        return _Result(self.row)


def _patch(monkeypatch, row):
    calls: list[str] = []
    monkeypatch.setattr(config, "connect", lambda: _Conn(calls, row))
    monkeypatch.setattr(config, "_cache", {})
    return calls


def test_ai_settings_read_once_and_without_ddl(monkeypatch):
    calls = _patch(
        monkeypatch, {"enabled": True, "active_provider": " Bee ", "model": "", "prompts": {"a": "x", "b": " "}}
    )
    for _ in range(4):
        got = config.get_ai_settings()
    assert got == {"enabled": True, "active_provider": "bee", "model": "", "prompts": {"a": "x"}}
    assert len(calls) == 1
    assert calls[0].lstrip().upper().startswith("SELECT") and "SELECT *" not in calls[0]


def test_ai_settings_expires(monkeypatch):
    calls = _patch(monkeypatch, {"enabled": False, "active_provider": "", "model": "", "prompts": {}})
    config.get_ai_settings()
    key = "ai_settings"
    t, value = config._cache[key]
    config._cache[key] = (t - config._CACHE_TTL_SECONDS - 1, value)
    config.get_ai_settings()
    assert len(calls) == 2


def test_missing_provider_not_cached(monkeypatch):
    calls = _patch(monkeypatch, None)
    assert config.get_ai_provider("bee") is None
    assert config.get_ai_provider("bee") is None
    assert len(calls) == 2  # không nhớ "chưa có" để lúc người dùng vừa thêm provider thì thấy ngay


def test_provider_cached(monkeypatch):
    calls = _patch(monkeypatch, {"key": "bee", "api_key": "k"})
    config.get_ai_provider("bee")
    config.get_ai_provider("bee")
    assert len(calls) == 1
