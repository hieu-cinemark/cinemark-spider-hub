"""Enabled Facebook accounts do not need warm-up, nurture flags or a client declaration."""

import asyncio
import json
import sqlite3
from contextlib import contextmanager
from unittest.mock import AsyncMock

import pytest

from social_crawler.accounts import warmup
from social_crawler.db import accounts


@pytest.fixture
def account_db(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.create_function("now", 0, lambda: "2026-11-04 00:00:00")
    db.executescript("""
        CREATE TABLE platform_accounts (
            id INTEGER PRIMARY KEY, platform TEXT, account_id TEXT, email TEXT,
            password TEXT, totp_secret TEXT, cookie TEXT, token TEXT, email_password TEXT,
            enabled BOOLEAN, status TEXT, cooldown_until TEXT, last_used_at TEXT,
            nurture_only BOOLEAN, session_client TEXT, last_check_status TEXT
        );
        INSERT INTO platform_accounts (id,platform,account_id,enabled,status,nurture_only,session_client)
        VALUES (1,'facebook','123',true,'active',true,'manual'),
               (2,'facebook','456',true,'active',false,NULL),
               (3,'facebook','789',false,'active',false,NULL),
               (4,'facebook','999',true,'checkpoint',false,NULL),
               (5,'threads','threads-test',true,'active',true,NULL);
        INSERT INTO platform_accounts (id,platform,account_id,enabled,status,nurture_only,cooldown_until)
        VALUES (6,'facebook','cooling',true,'active',false,'2026-11-05 00:00:00');
    """)

    class Connection:
        def execute(self, sql, params=()):
            # Only eligibility is under test; SQLite cannot exercise PostgreSQL row locks.
            sql = sql.replace("FOR UPDATE SKIP LOCKED ", "")
            sql = sql.replace("account_id = ANY(%s)", "account_id IN (SELECT value FROM json_each(%s))")
            params = tuple(json.dumps(value) if isinstance(value, list) else value for value in params)
            return db.execute(sql.replace("%s", "?"), params)

    @contextmanager
    def connect():
        yield Connection()

    monkeypatch.setattr(accounts, "connect", connect)
    monkeypatch.setattr(accounts, "_ensure_nurture_only_column", lambda conn: None)
    yield db
    db.close()


def test_enabled_facebook_accounts_are_selected_despite_legacy_manual_and_nurture_flags(account_db):
    assert {row["id"] for row in accounts.get_accounts("facebook")} == {"123", "456"}
    assert accounts.is_account_usable("facebook", "123")
    assert accounts.is_account_usable("facebook", "456")
    assert not accounts.is_account_usable("facebook", "789")
    assert not accounts.is_account_usable("facebook", "999")
    assert not accounts.is_account_usable("threads", "threads-test")
    assert accounts.get_accounts("threads") == []


def test_pool_claims_enabled_facebook_accounts_without_legacy_handover(account_db):
    assert accounts.claim_account("facebook")["id"] == "123"
    assert accounts.claim_account("facebook", exclude={"123"})["id"] == "456"
    assert accounts.claim_account("facebook", exclude={"123", "456"}) is None
    assert accounts.claim_account("threads") is None


def test_facebook_never_waits_for_nurture_days_or_reads_their_state(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Facebook must not read nurture eligibility")

    monkeypatch.setattr(warmup, "nurture_only_accounts", unexpected)
    monkeypatch.setattr(warmup, "accounts_created_since", unexpected)
    monkeypatch.setattr(warmup, "nurture_days", unexpected)
    assert warmup.unwarmed_account_ids("facebook") == set()
    assert not warmup.is_warming_key("facebook", "test@example.test")


def test_queued_facebook_nurture_job_is_consumed_without_starting_a_browser(monkeypatch):
    from social_crawler.consumer.jobs import accounts as jobs

    run = AsyncMock(side_effect=AssertionError("obsolete nurture job must not start a subprocess"))
    monkeypatch.setattr(jobs, "_run_subprocess", run)
    asyncio.run(jobs._nurture_accounts({"type": "nurture", "platform": "facebook", "account": "123"}))
    run.assert_not_awaited()


def test_facebook_cookie_import_does_not_bootstrap_a_browser_or_wait_for_warmup(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from social_crawler.consumer.jobs import accounts as jobs

    run = AsyncMock(return_value=0)
    monkeypatch.setattr(jobs, "_run_subprocess", run)
    monkeypatch.setattr(jobs, "RedisCache", lambda: SimpleNamespace(get=lambda key: None))
    monkeypatch.setattr(jobs, "_mark_refresh_result", Mock())
    monkeypatch.setattr(jobs, "is_warming_key", Mock(side_effect=AssertionError("Facebook must not wait for warmup")))
    asyncio.run(
        jobs._import_cookies(
            {
                "platform": "facebook",
                "account_key": "123",
                "cookies": '{"c_user":"123"}',
            }
        )
    )
    run.assert_awaited_once()
    assert "--cookies-file" in run.await_args.args[0]
    assert "--query" not in run.await_args.args[0]
