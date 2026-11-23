"""Tài khoản/proxy/sự kiện đọc ghi qua D1 - chạy SQL thật trên SQLite trong bộ nhớ thay cho D1."""

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from social_crawler.db import account_events, accounts, d1, d1_pool, proxies

SCHEMA = """
CREATE TABLE platform_accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT NOT NULL, account_id TEXT NOT NULL,
  password TEXT NOT NULL DEFAULT '', totp_secret TEXT NOT NULL DEFAULT '', cookie TEXT NOT NULL DEFAULT '',
  token TEXT NOT NULL DEFAULT '', email TEXT NOT NULL DEFAULT '', email_password TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  last_checked_at TEXT, last_check_status TEXT, last_check_note TEXT, status TEXT NOT NULL DEFAULT 'active',
  cooldown_until TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0, last_used_at TEXT, nurture_only INTEGER NOT NULL DEFAULT 0);
CREATE TABLE platform_proxies (
  id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT NOT NULL DEFAULT 'all', proxy_url TEXT NOT NULL,
  username TEXT NOT NULL DEFAULT '', password TEXT NOT NULL DEFAULT '', login_use_proxy INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
  cooldown_until TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0, last_used_at TEXT,
  ip_type TEXT NOT NULL DEFAULT 'unknown', provider_key TEXT, provider_ref TEXT);
CREATE TABLE proxy_providers (key TEXT PRIMARY KEY, api_url TEXT NOT NULL DEFAULT '', token TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL, last_used_at TEXT, last_status TEXT);
CREATE TABLE account_events (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, platform TEXT NOT NULL,
  account_key TEXT NOT NULL, event TEXT NOT NULL, details TEXT NOT NULL DEFAULT '{}');
"""


@pytest.fixture
def db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)

    def fake_query(sql, params=None):
        cursor = conn.execute(sql, params or [])
        rows = [] if cursor.description is None else [dict(r) for r in cursor.fetchall()]
        conn.commit()
        return rows

    monkeypatch.setattr(d1, "query", fake_query)
    # conftest thay account_events.record bằng hàm rỗng: nạp một bản module gốc để ghi sự kiện thật vào D1 giả.
    import importlib.util

    spec = importlib.util.spec_from_file_location("account_events_real", account_events.__file__)
    real_events = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(real_events)
    monkeypatch.setattr(account_events, "record", real_events.record)
    monkeypatch.setattr("social_crawler.clients.ai.diagnose_account_failure", lambda reason: f"chẩn đoán: {reason}")
    monkeypatch.setattr(accounts, "diagnose_account_failure", lambda reason: f"chẩn đoán: {reason}")
    return conn


def _iso(delta_minutes: int = 0) -> str:
    return d1.iso(datetime.now(UTC) + timedelta(minutes=delta_minutes))


def add_account(db, account_id, platform="facebook", **extra):
    values = {
        "platform": platform,
        "account_id": account_id,
        "email": f"{account_id}@x.com",
        "created_at": _iso(),
        "updated_at": _iso(),
        **extra,
    }
    db.execute(
        f"INSERT INTO platform_accounts ({', '.join(values)}) VALUES ({', '.join('?' * len(values))})",
        list(values.values()),
    )
    db.commit()


def add_proxy(db, url, platform="facebook", **extra):
    values = {"platform": platform, "proxy_url": url, "created_at": _iso(), "updated_at": _iso(), **extra}
    db.execute(
        f"INSERT INTO platform_proxies ({', '.join(values)}) VALUES ({', '.join('?' * len(values))})",
        list(values.values()),
    )
    db.commit()


def row(db, table, **where):
    clause = " AND ".join(f"{k} = ?" for k in where)
    return dict(db.execute(f"SELECT * FROM {table} WHERE {clause}", list(where.values())).fetchone())


def test_get_accounts_filters_and_orders_lru(db):
    add_account(db, "never")
    add_account(db, "old", last_used_at=_iso(-60))
    add_account(db, "recent", last_used_at=_iso(-5))
    add_account(db, "off", enabled=0)
    add_account(db, "cooling", cooldown_until=_iso(30))
    add_account(db, "expired-cooldown", cooldown_until=_iso(-30), last_used_at=_iso(-120))
    add_account(db, "checkpoint", status="checkpoint")
    add_account(db, "nurture", platform="threads", nurture_only=1)
    ids = [a["id"] for a in accounts.get_accounts("facebook")]
    assert ids == ["never", "expired-cooldown", "old", "recent"]
    assert accounts.get_accounts("threads") == []
    assert accounts.nurture_only_accounts("threads") == {"nurture": "nurture@x.com"}
    assert accounts.has_enabled_accounts("facebook") is True and accounts.has_enabled_accounts("tiktok") is False


def test_claim_account_is_lru_atomic_and_respects_exclude_and_dead(db):
    add_account(db, "a", last_used_at=_iso(-30))
    add_account(db, "b", last_used_at=_iso(-20))
    add_account(db, "dead", last_check_status="dead")
    # mỗi lần claim ghi last_used_at nên lần sau không lấy lại cùng dòng: a (cũ nhất) -> b -> a ...
    assert [accounts.claim_account("facebook")["id"] for _ in range(3)] == ["a", "b", "a"]
    # tài khoản có lần kiểm tra cookie 'dead' xếp cuối: chỉ được chọn khi không còn tài khoản sống
    assert accounts.claim_account("facebook", frozenset({"a", "b"}))["id"] == "dead"
    assert accounts.claim_account("facebook", frozenset({"a", "b", "dead"})) is None
    assert row(db, "platform_accounts", account_id="dead")["last_used_at"] is not None


def test_check_status_guard_and_session_blocked(db):
    add_account(db, "dead-now", last_check_status="dead", last_checked_at=_iso(), updated_at=_iso(-10))
    add_account(db, "dead-but-new-cookie", last_check_status="dead", last_checked_at=_iso(-10), updated_at=_iso())
    assert [a["id"] for a in accounts.get_accounts_by_check_status("facebook", "dead")] == ["dead-now"]
    assert accounts.session_setup_blocked("facebook", "DEAD-NOW@x.com") is True
    assert accounts.session_setup_blocked("facebook", "dead-but-new-cookie") is False
    assert accounts.session_setup_blocked("facebook", "khong-co") is False


def test_is_account_usable_rules(db):
    add_account(db, "ok")
    add_account(db, "off", enabled=0)
    add_account(db, "t", platform="threads", nurture_only=1)
    assert accounts.is_account_usable("facebook", "OK@x.com") is True
    assert accounts.is_account_usable("facebook", "off") is False
    assert accounts.is_account_usable("threads", "t") is False
    assert accounts.is_account_usable("facebook", "default") is True
    assert accounts.is_account_usable("facebook", "da-xoa") is False


def test_rest_reactivate_cookie_and_lookups(db):
    add_account(db, "acc", cooldown_until=None, status="checkpoint", enabled=0, consecutive_failures=3)
    until = datetime.now(UTC) + timedelta(hours=6)
    assert accounts.rest_account_until("facebook", "ACC@x.com", until) is True
    assert row(db, "platform_accounts", account_id="acc")["cooldown_until"] == d1.iso(until)
    assert accounts.rest_account_until("facebook", "", until) is False
    assert db.execute("SELECT event FROM account_events").fetchall()[0][0] == "rested"

    accounts.reactivate_account("facebook", "acc")
    reactivated = row(db, "platform_accounts", account_id="acc")
    assert (
        reactivated["enabled"],
        reactivated["status"],
        reactivated["consecutive_failures"],
        reactivated["cooldown_until"],
    ) == (1, "active", 0, None)

    pk = accounts.get_account_pk("facebook", "acc@x.com")
    assert (
        accounts.update_account_cookie("facebook", pk, "c_user=9") is True
        and accounts.update_account_cookie("facebook", 999, "x") is False
    )
    assert accounts.get_account_by_key("facebook", "ACC")["cookie"] == "c_user=9"
    assert accounts.get_account_by_row_id("facebook", pk)["id"] == "acc"
    assert accounts.get_account_by_key("facebook", " ") is None


def test_outcomes_success_soft_hard_and_cap(db):
    add_account(db, "acc", consecutive_failures=2, cooldown_until=_iso(30), last_check_note="cũ")
    accounts.record_account_outcome("facebook", "acc", success=True)
    ok = row(db, "platform_accounts", account_id="acc")
    assert (ok["consecutive_failures"], ok["cooldown_until"], ok["last_check_note"]) == (0, None, None)

    before = datetime.now(UTC)
    accounts.record_account_outcome("facebook", "acc", success=False)
    first = row(db, "platform_accounts", account_id="acc")
    assert first["consecutive_failures"] == 1
    wait = datetime.fromisoformat(first["cooldown_until"]) - before
    assert timedelta(minutes=9) < wait < timedelta(minutes=11)  # 5 * 2^1 phút

    db.execute("UPDATE platform_accounts SET consecutive_failures = 40")
    db.commit()
    accounts.record_account_outcome("facebook", "acc", success=False)
    capped = datetime.fromisoformat(row(db, "platform_accounts", account_id="acc")["cooldown_until"]) - datetime.now(
        UTC
    )
    assert timedelta(minutes=119) < capped <= timedelta(minutes=120)  # trần 2 giờ, không tràn

    accounts.record_account_outcome("facebook", "acc", success=False, hard_failure=True, reason="checkpoint")
    hard = row(db, "platform_accounts", account_id="acc")
    assert (hard["status"], hard["enabled"], hard["last_check_note"]) == ("checkpoint", 0, "chẩn đoán: checkpoint")


def test_cookie_check_and_disable_write_events(db):
    add_account(db, "acc")
    accounts.record_cookie_check("facebook", "acc", status="dead", note="bị từ chối")
    assert row(db, "platform_accounts", account_id="acc")["last_check_status"] == "dead"
    assert accounts.disable_account("facebook", "acc", "checkpoint") is True
    assert row(db, "platform_accounts", account_id="acc")["enabled"] == 0
    events = [r[0] for r in db.execute("SELECT event FROM account_events ORDER BY id").fetchall()]
    assert events == ["session_marked_dead", "account_disabled"]


def test_tiktok_identity_keeps_existing_email(db):
    add_account(db, "pending", platform="tiktok", email="")
    pk = accounts.get_account_pk("tiktok", "pending")
    assert (
        accounts.update_tiktok_identity(pk, device_id="dev1", odin_id="odin", cookie="ck", lookup_key="me@x.com")
        is True
    )
    got = row(db, "platform_accounts", id=pk)
    assert (got["account_id"], got["token"], got["cookie"], got["email"]) == ("dev1", "odin", "ck", "me@x.com")
    accounts.update_tiktok_identity(pk, device_id="dev2", odin_id="o2", cookie="c2", lookup_key="other@x.com")
    assert row(db, "platform_accounts", id=pk)["email"] == "me@x.com"


def test_proxy_selection_and_circuit_breaker(db, set_setting):
    add_proxy(db, "1.1.1.1:80", last_used_at=_iso(-30))
    add_proxy(db, "2.2.2.2:80")
    add_proxy(db, "3.3.3.3:80", cooldown_until=_iso(60))
    add_proxy(db, "4.4.4.4:80", enabled=0)
    add_proxy(db, "5.5.5.5:80", platform="public", provider_key="webtui", ip_type="mobile")
    assert [p["url"] for p in proxies.list_proxies("facebook")] == ["1.1.1.1:80", "2.2.2.2:80", "3.3.3.3:80"]
    assert proxies.claim_proxy("facebook")["url"] == "2.2.2.2:80"  # chưa dùng bao giờ đứng đầu
    assert proxies.claim_proxy("facebook")["url"] == "1.1.1.1:80"
    assert proxies.claim_proxy("facebook")["url"] == "2.2.2.2:80"  # 3.3.3.3 đang cooldown nên không bao giờ được chọn
    assert proxies.platform_has_any_proxy("facebook") and not proxies.platform_has_any_proxy("tiktok")
    public = proxies.list_public_page_proxies()[0]
    assert public["provider_key"] == "webtui" and public["login_use_proxy"] is False
    assert proxies.provider_key_for_proxy_url("5.5.5.5:80") == "webtui"
    assert proxies.proxy_row_for_url("5.5.5.5:80")["ip_type"] == "mobile"
    assert [p["url"] for p in proxies.list_proxies_by_provider("webtui")] == ["5.5.5.5:80"]

    set_setting("cooldown_base_minutes", 2)
    set_setting("cooldown_max_minutes", 30)
    proxies.record_proxy_outcome("facebook", "1.1.1.1:80", success=False)
    bad = row(db, "platform_proxies", proxy_url="1.1.1.1:80")
    assert (bad["status"], bad["consecutive_failures"]) == ("degraded", 1)
    wait = datetime.fromisoformat(bad["cooldown_until"]) - datetime.now(UTC)
    assert timedelta(minutes=3) < wait < timedelta(minutes=5)  # 2 phút * 2^1
    db.execute("UPDATE platform_proxies SET consecutive_failures = 50 WHERE proxy_url = '1.1.1.1:80'")
    db.commit()
    proxies.record_proxy_outcome("facebook", "1.1.1.1:80", success=False)
    capped = datetime.fromisoformat(
        row(db, "platform_proxies", proxy_url="1.1.1.1:80")["cooldown_until"]
    ) - datetime.now(UTC)
    assert timedelta(minutes=29) < capped <= timedelta(minutes=30)
    proxies.record_proxy_outcome("facebook", "1.1.1.1:80", success=True)
    healed = row(db, "platform_proxies", proxy_url="1.1.1.1:80")
    assert (healed["status"], healed["consecutive_failures"], healed["cooldown_until"]) == ("active", 0, None)


def test_proxies_without_meta_accounts_skip_shared_endpoints(db):
    add_proxy(db, "9.9.9.9:1", platform="facebook")
    add_proxy(db, "9.9.9.9:1", platform="tiktok")  # cùng cổng với proxy Meta -> bị loại
    add_proxy(db, "8.8.8.8:1", platform="tiktok")
    assert [p["url"] for p in proxies.list_proxies_without_meta_accounts()] == ["8.8.8.8:1"]


def test_lease_candidates_and_public_proxy(db, monkeypatch):
    add_proxy(db, "7.7.7.7:1", login_use_proxy=1)
    add_proxy(db, "6.6.6.6:1", platform="public", username="u", password="p")
    assert d1_pool.lease_candidates("facebook")[0]["login_use_proxy"] is True
    d1_pool.mark_proxy_used(1)
    assert row(db, "platform_proxies", id=1)["last_used_at"] is not None
    found, meta = d1_pool.public_proxy_row(2)
    assert found["proxy_url"] == "6.6.6.6:1" and meta is False
    assert d1_pool.public_proxy_row(99) == (None, False)


def test_provider_use_and_events_timeline(db):
    db.execute(
        "INSERT INTO proxy_providers (key, api_url, token, updated_at) VALUES ('p', 'https://x', 'tok', ?)", [_iso()]
    )
    db.commit()
    assert d1_pool.get_provider_row("p")["token"] == "tok" and d1_pool.get_provider_row("khac") is None
    d1_pool.record_provider_use("p", "ok")
    assert row(db, "proxy_providers", key="p")["last_status"] == "ok"

    account_events.record("facebook", "Acc@X.com", "one", note="a")
    account_events.record("facebook", "acc@x.com", "two")
    account_events.record("facebook", "", "bo-qua")
    account_events.record("facebook", "khac", "other")
    got = account_events.timeline("facebook", ["ACC@x.com", ""])
    assert [e["event"] for e in got] == ["one", "two"] and got[0]["details"] == {"note": "a"}


def test_d1_errors_are_swallowed(db, monkeypatch):
    def boom(sql, params=None):
        raise d1.D1Error("HTTP 500")

    monkeypatch.setattr(d1, "query", boom)
    assert accounts.get_accounts("facebook") == [] and accounts.claim_account("facebook") is None
    assert accounts.has_enabled_accounts("facebook") is False and accounts.is_account_usable("facebook", "x") is True
    assert accounts.session_setup_blocked("facebook", "x") is True
    assert accounts.disable_account("facebook", "acc", "r") is False
    accounts.record_account_outcome("facebook", "acc", success=False)
    proxies.record_proxy_outcome("facebook", "1.1.1.1:80", success=True)
    assert proxies.list_proxies("facebook") == [] and proxies.claim_proxy("facebook") is None
    account_events.record("facebook", "acc", "event")
