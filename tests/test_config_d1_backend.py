"""CONFIG_DB_BACKEND=d1: ai_settings, ai_providers và proxy_settings đọc từ D1 (không chạm Postgres), cột JSON lưu dạng TEXT."""

import json

import httpx
import pytest

from social_crawler.db import config, d1, proxy_settings


def _no_postgres(*_args, **_kwargs):
    raise AssertionError("không được chạm Postgres khi CONFIG_DB_BACKEND=d1")


@pytest.fixture
def d1_backend(monkeypatch):
    monkeypatch.setenv("CONFIG_DB_BACKEND", "d1")
    monkeypatch.setattr(config, "connect", _no_postgres)
    monkeypatch.setattr(config, "_cache", {})
    monkeypatch.setattr(proxy_settings, "_settings_cache", None)
    monkeypatch.setattr("social_crawler.db.connection.connect", _no_postgres)
    calls: list[tuple[str, list]] = []
    answers: dict[str, list[dict]] = {}

    def fake_query(sql, params=None):
        calls.append((sql, params or []))
        for table, rows in answers.items():
            if f"FROM {table}" in sql:
                return rows
        return []

    monkeypatch.setattr(d1, "query", fake_query)
    return calls, answers


def test_backend_flag_defaults_to_postgres(monkeypatch):
    monkeypatch.delenv("CONFIG_DB_BACKEND", raising=False)
    assert d1.enabled() is False
    monkeypatch.setenv("CONFIG_DB_BACKEND", " D1 ")
    assert d1.enabled() is True


def test_ai_settings_parse_json_text_and_cache(d1_backend):
    calls, answers = d1_backend
    answers["ai_settings"] = [
        {"enabled": 1, "active_provider": " Bee ", "model": "", "prompts": json.dumps({"a": "x", "b": " "})}
    ]
    for _ in range(3):
        got = config.get_ai_settings()
    assert got == {"enabled": True, "active_provider": "bee", "model": "", "prompts": {"a": "x"}}
    assert len(calls) == 1


def test_ai_settings_d1_failure_returns_disabled(d1_backend, monkeypatch):
    def boom(sql, params=None):
        raise d1.D1Error("HTTP 500")

    monkeypatch.setattr(d1, "query", boom)
    assert config.get_ai_settings() == {"enabled": False, "model": "", "prompts": {}}


def test_ai_provider_row_and_missing(d1_backend):
    calls, answers = d1_backend
    answers["ai_providers"] = [
        {"key": "bee", "base_url": "https://x", "api_key": "sk", "model": "haiku", "reasoning_effort": ""}
    ]
    assert config.get_ai_provider("bee")["api_key"] == "sk"
    assert calls[0][1] == ["bee"] and "= ?" in calls[0][0]
    answers["ai_providers"] = []
    assert config.get_ai_provider("khac") is None


def test_ai_provider_d1_failure_is_none(d1_backend, monkeypatch):
    monkeypatch.setattr(d1, "query", lambda *a, **k: (_ for _ in ()).throw(d1.D1Error("down")))
    assert config.get_ai_provider("bee") is None


def test_proxy_settings_from_d1(d1_backend):
    # conftest thay proxy_settings._load_settings bằng bản chụp mặc định: nạp một bản module gốc để chạy đường đọc D1 thật.
    import importlib.util

    spec = importlib.util.spec_from_file_location("proxy_settings_real", proxy_settings.__file__)
    real = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(real)
    _, answers = d1_backend
    answers["proxy_settings"] = [{"settings": json.dumps({"fb_daily_request_cap": 200})}]
    assert real._load_settings() == {"fb_daily_request_cap": 200}
    answers["proxy_settings"] = []
    assert real._load_settings() is None


def test_d1_query_requires_env(monkeypatch):
    for name in ("CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_D1_CONFIG_DATABASE_ID"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(d1.D1Error, match="CLOUDFLARE_ACCOUNT_ID"):
        d1.query("SELECT 1")


def test_d1_query_parses_cloudflare_response(monkeypatch):
    for name, value in (
        ("CLOUDFLARE_ACCOUNT_ID", "acc"),
        ("CLOUDFLARE_API_TOKEN", "tok"),
        ("CLOUDFLARE_D1_CONFIG_DATABASE_ID", "db"),
    ):
        monkeypatch.setenv(name, value)
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"], seen["auth"], seen["body"] = (
            str(request.url),
            request.headers["authorization"],
            json.loads(request.content),
        )
        return httpx.Response(200, json={"success": True, "result": [{"results": [{"n": 1}]}]})

    monkeypatch.setattr(d1, "_client", httpx.Client(transport=httpx.MockTransport(handler)))
    assert d1.query("SELECT ? AS n", [1]) == [{"n": 1}]
    assert seen["url"].endswith("/accounts/acc/d1/database/db/query") and seen["auth"] == "Bearer tok"
    assert seen["body"] == {"sql": "SELECT ? AS n", "params": [1]}


def test_d1_query_errors(monkeypatch):
    for name in ("CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_D1_CONFIG_DATABASE_ID"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setattr(
        d1, "_client", httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500, text="boom")))
    )
    with pytest.raises(d1.D1Error, match="HTTP 500"):
        d1.query("SELECT 1")
    monkeypatch.setattr(
        d1,
        "_client",
        httpx.Client(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"success": False, "errors": [{"message": "no such table"}]})
            )
        ),
    )
    with pytest.raises(d1.D1Error, match="no such table"):
        d1.query("SELECT 1")


def test_script_connect_translates_psycopg_style_sql(monkeypatch):
    from social_crawler.db import script_connect

    seen = []
    monkeypatch.setattr(d1, "query", lambda sql, params=None: seen.append((sql, params)) or [{"id": 1}])
    monkeypatch.setenv("CONFIG_DB_BACKEND", "d1")
    with script_connect.connect() as conn:
        one = conn.execute(
            "SELECT id FROM platform_proxies WHERE id = %s AND enabled = true AND note = 'true'", (7,)
        ).fetchone()
        every = conn.execute("UPDATE platform_proxies SET ip_type = %s WHERE id = %s", ("static", 7))
    assert one == {"id": 1} and every.rowcount == 1
    assert seen[0] == ("SELECT id FROM platform_proxies WHERE id = ? AND enabled = 1 AND note = 'true'", [7])
    assert seen[1][1] == ["static", 7]


def test_script_connect_uses_postgres_by_default(monkeypatch):
    from social_crawler.db import script_connect

    sentinel = object()
    monkeypatch.delenv("CONFIG_DB_BACKEND", raising=False)
    monkeypatch.setattr(script_connect, "_postgres_connect", lambda: sentinel)
    assert script_connect.connect() is sentinel
