"""scripts/migrate_proxy_providers.py - kế hoạch điền provider_key/provider_ref, dry-run không ghi.

DB giả trả dữ liệu theo câu SELECT; không kết nối Postgres thật."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "migrate_proxy_providers.py"
_spec = importlib.util.spec_from_file_location("migrate_proxy_providers", _PATH)
migrate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migrate)  # type: ignore[union-attr]


class FakeConn:
    def __init__(self, providers, proxies, columns=("id", "proxy_url", "provider_key", "provider_ref")):
        self.providers = providers
        self.proxies = proxies
        self.columns = set(columns)
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if sql.startswith("SELECT key FROM proxy_providers"):
            rows = [{"key": p["key"]} for p in self.providers]
        elif sql.startswith("SELECT key, token, last_status"):
            rows = self.providers
        elif "information_schema.columns" in sql:
            rows = [{"column_name": c} for c in self.columns]
        elif sql.startswith("SELECT id, proxy_url"):
            rows = [{"provider_key": None, "provider_ref": None, **row} for row in self.proxies]
        else:
            rows = []
        return type("Cur", (), {"fetchall": lambda self_: rows, "fetchone": lambda self_: rows[0] if rows else None})()

    def rollback(self):
        self.statements.append("ROLLBACK")

    def commit(self):
        self.statements.append("COMMIT")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_fixed_port_source_gets_key_and_ref_but_status_owner_only_key():
    conn = FakeConn(
        providers=[
            {"key": "us", "token": "gw.test:4000:u:p", "last_status": None},
            {"key": "webtui", "token": "apitoken", "last_status": "ok: 10.0.0.1:3128 1.1.1.1 -> 2.2.2.2"},
        ],
        proxies=[
            {"id": 1, "proxy_url": "gw.test:4000"},
            {"id": 2, "proxy_url": "10.0.0.1:3128"},
            {"id": 3, "proxy_url": "manual.test:80"},
        ],
    )
    plan = migrate.build_plan(conn)
    assert plan == [(1, None, "us", None, "gw.test:4000"), (2, None, "webtui", None, None)]


def test_rows_with_a_source_are_never_overwritten():
    conn = FakeConn(
        providers=[{"key": "us", "token": "gw.test:4000:u:p", "last_status": None}],
        proxies=[{"id": 1, "proxy_url": "gw.test:4000", "provider_key": "manual_pick", "provider_ref": "42"}],
    )
    assert migrate.build_plan(conn) == []


def test_operator_ref_is_kept_when_filling_the_key():
    conn = FakeConn(
        providers=[{"key": "webtui", "token": "t", "last_status": "ok: 10.0.0.1:3128 a -> b"}],
        proxies=[{"id": 2, "proxy_url": "10.0.0.1:3128", "provider_ref": "17"}],
    )
    assert migrate.build_plan(conn) == [(2, None, "webtui", "17", "17")]


def test_plan_reads_a_database_without_the_new_columns():
    conn = FakeConn(
        providers=[{"key": "us", "token": "gw.test:4000:u:p", "last_status": None}],
        proxies=[{"id": 1, "proxy_url": "gw.test:4000"}],
        columns=("id", "proxy_url"),
    )
    assert migrate.build_plan(conn) == [(1, None, "us", None, "gw.test:4000")]
    assert any("NULL::text AS provider_ref" in s for s in conn.statements)


def test_rename_conflict_is_reported():
    conn = FakeConn(providers=[{"key": "proxiestrust_default"}, {"key": "default"}], proxies=[])
    plan, conflicts = migrate.build_rename_plan(conn)
    assert plan == [] and conflicts


def test_dry_run_runs_no_ddl_and_no_write(monkeypatch, capsys):
    conn = FakeConn(providers=[{"key": "us", "token": "gw.test:4000:u:p", "last_status": None}], proxies=[])
    monkeypatch.setattr(migrate, "connect", lambda: conn)
    assert migrate.main([]) == 0
    written = [s for s in conn.statements if s.split()[0].upper() in {"ALTER", "CREATE", "UPDATE", "INSERT", "DELETE"}]
    assert written == []
    assert "COMMIT" not in conn.statements


def test_drop_is_a_separate_command():
    with pytest.raises(SystemExit):
        migrate.main(["--apply", "--drop-ip-allowlist"])
