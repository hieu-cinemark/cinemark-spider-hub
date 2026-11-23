"""Bản D1 (cinemark-config-db) của db/accounts.py, db/proxies.py, db/account_events.py và các câu hỏi proxy_providers: cùng tên hàm,
cùng giá trị trả về và cùng tên sự kiện log với bản Postgres - chỉ khác SQL (SQLite) và việc không có transaction nhiều câu.
Chọn bằng CONFIG_DB_BACKEND=d1 (xem db/d1.py); các module cũ gọi vào đây ở đầu mỗi hàm.

Điểm khác với bản Postgres:
- Thời gian là chuỗi ISO UTC độ rộng cố định (d1.iso) nên so sánh chuỗi đúng thứ tự; "now()" thành tham số do Python tính.
- Chọn-và-giữ chỗ (claim_account/claim_proxy) là MỘT câu UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING: D1 chạy mỗi câu
  nguyên tử, nên hai lời gọi đồng thời không thể lấy cùng một dòng (thay cho FOR UPDATE SKIP LOCKED).
- Cooldown luỹ thừa tính trong SQL bằng strftime('now', '+N minutes') với số mũ bị chặn (1 << 20) như bản Postgres.
Schema do cinemark-api tạo (app/services/accounts/config_db/d1_accounts.py); module này chỉ đọc/ghi dữ liệu."""

from __future__ import annotations

import json
from typing import Any

from social_crawler.core.logger import get_logger
from social_crawler.db import d1
from social_crawler.db.d1 import D1Error

logger = get_logger(__name__)

_ACCOUNT_FIELDS = "account_id, password, totp_secret, cookie, token, email, email_password"
_PROXY_FIELDS = "id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref"
# strftime cho cooldown tính trong SQL: cùng độ rộng với d1.iso (SS.mmm + 000 = microsecond).
_SQL_NOW_PLUS = "strftime('%Y-%m-%dT%H:%M:%f000+00:00', 'now', '+' || {minutes} || ' minutes')"
_COOLDOWN_MAX_EXPONENT = 20
_KEY_MATCH = "(lower(coalesce(email, '')) = lower(?) OR lower(account_id) = lower(?))"


def _acct(row: dict[str, Any]) -> dict[str, str]:
    from social_crawler.db.accounts import account_dict

    return account_dict(row)


def _proxy(row: dict[str, Any]) -> dict[str, Any]:
    from social_crawler.db.proxies import _proxy_row

    return _proxy_row({**row, "login_use_proxy": bool(row.get("login_use_proxy"))})


# --- tài khoản: đọc ------------------------------------------------------------------------------------------------


def nurture_only_accounts(platform: str) -> dict[str, str | None]:
    try:
        rows = d1.query(
            "SELECT account_id, email FROM platform_accounts WHERE platform = ? AND enabled = 1 AND nurture_only = 1",
            [platform],
        )
    except D1Error as exc:
        logger.error("db_nurture_only_accounts_failed", platform=platform, error=str(exc))
        return {}
    return {row["account_id"]: row["email"] for row in rows}


def get_accounts(platform: str) -> list[dict[str, str]]:
    try:
        rows = d1.query(
            f"SELECT {_ACCOUNT_FIELDS} FROM platform_accounts WHERE platform = ? AND enabled = 1 "
            "AND (platform = 'facebook' OR nurture_only = 0) AND status != 'checkpoint' "
            "AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY last_used_at ASC, id ASC",
            [platform, d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_get_accounts_failed", platform=platform, error=str(exc))
        return []
    return [_acct(row) for row in rows]


def is_account_usable(platform: str, key: str) -> bool:
    try:
        rows = d1.query(
            "SELECT enabled, status, nurture_only FROM platform_accounts WHERE platform = ? "
            "AND (lower(account_id) = ? OR lower(email) = ?) LIMIT 1",
            [platform, key, key],
        )
    except D1Error as exc:
        logger.error("db_is_account_usable_failed", platform=platform, account=key, error=str(exc))
        return True
    if not rows:
        return key == "default"
    row = rows[0]
    return (
        bool(row["enabled"]) and row["status"] != "checkpoint" and (platform == "facebook" or not row["nurture_only"])
    )


def list_enabled_accounts(platform: str, *, include_cooldown: bool) -> list[dict[str, str]]:
    try:
        rows = d1.query(
            f"SELECT {_ACCOUNT_FIELDS} FROM platform_accounts WHERE platform = ? AND enabled = 1 "
            "AND status != 'checkpoint' AND (? OR cooldown_until IS NULL OR cooldown_until <= ?) "
            "ORDER BY last_used_at ASC, id ASC",
            [platform, int(include_cooldown), d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_list_enabled_accounts_failed", platform=platform, error=str(exc))
        return []
    return [_acct(row) for row in rows]


def get_accounts_by_check_status(platform: str, status: str) -> list[dict[str, str]]:
    guard = (
        " AND (updated_at IS NULL OR last_checked_at IS NULL OR last_checked_at >= updated_at)"
        if status == "dead"
        else ""
    )
    try:
        rows = d1.query(
            f"SELECT {_ACCOUNT_FIELDS} FROM platform_accounts WHERE platform = ? AND enabled = 1 AND last_check_status = ?"
            + guard
            + " ORDER BY id ASC",
            [platform, status],
        )
    except D1Error as exc:
        logger.error("db_get_accounts_by_check_status_failed", platform=platform, status=status, error=str(exc))
        return []
    return [_acct(row) for row in rows]


def accounts_created_since(platform: str, since: str) -> dict[str, str | None]:
    try:
        rows = d1.query(
            "SELECT account_id, email FROM platform_accounts WHERE platform = ? AND enabled = 1 AND created_at >= ?",
            [platform, d1.iso(since)],
        )
    except D1Error as exc:
        logger.error("db_accounts_created_since_failed", platform=platform, error=str(exc))
        return {}
    return {row["account_id"]: row["email"] for row in rows}


def claim_account(platform: str, exclude: set[str] | frozenset[str]) -> dict[str, str] | None:
    """LRU + giữ chỗ trong MỘT câu UPDATE (xem docstring module)."""
    try:
        rows = d1.query(
            "UPDATE platform_accounts SET last_used_at = ? WHERE id = ("
            "SELECT id FROM platform_accounts WHERE platform = ? AND enabled = 1 "
            "AND (platform = 'facebook' OR nurture_only = 0) AND status != 'checkpoint' "
            "AND (cooldown_until IS NULL OR cooldown_until <= ?) "
            "AND account_id NOT IN (SELECT value FROM json_each(?)) "
            "ORDER BY (last_check_status IS 'dead') ASC, last_used_at ASC, id ASC LIMIT 1) "
            f"RETURNING {_ACCOUNT_FIELDS}",
            [d1.iso(), platform, d1.iso(), json.dumps(sorted(exclude))],
        )
    except D1Error as exc:
        logger.error("db_claim_account_failed", platform=platform, error=str(exc))
        return None
    return _acct(rows[0]) if rows else None


def get_account_by_row_id(platform: str, row_id: int) -> dict[str, str] | None:
    try:
        rows = d1.query(
            f"SELECT {_ACCOUNT_FIELDS} FROM platform_accounts WHERE platform = ? AND id = ?", [platform, row_id]
        )
    except D1Error as exc:
        logger.error("db_get_account_by_row_id_failed", platform=platform, row_id=row_id, error=str(exc))
        return None
    return _acct(rows[0]) if rows else None


def get_account_by_key(platform: str, needle: str) -> dict[str, str] | None:
    try:
        rows = d1.query(
            f"SELECT {_ACCOUNT_FIELDS} FROM platform_accounts WHERE platform = ? AND {_KEY_MATCH} ORDER BY id ASC LIMIT 1",
            [platform, needle, needle],
        )
    except D1Error as exc:
        logger.error("db_get_account_by_key_failed", platform=platform, error=str(exc))
        return None
    return _acct(rows[0]) if rows else None


def get_account_pk(platform: str, needle: str) -> int | None:
    try:
        rows = d1.query(
            f"SELECT id FROM platform_accounts WHERE platform = ? AND {_KEY_MATCH} ORDER BY id ASC LIMIT 1",
            [platform, needle, needle],
        )
    except D1Error as exc:
        logger.error("db_get_account_pk_failed", platform=platform, error=str(exc))
        return None
    return int(rows[0]["id"]) if rows else None


def session_setup_blocked(platform: str, needle: str) -> bool:
    try:
        rows = d1.query(
            f"SELECT 1 AS blocked FROM platform_accounts WHERE platform = ? AND {_KEY_MATCH} "
            "AND (cooldown_until > ? OR (last_check_status = 'dead' "
            "AND (updated_at IS NULL OR last_checked_at IS NULL OR last_checked_at >= updated_at))) LIMIT 1",
            [platform, needle, needle, d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_session_setup_blocked_failed", platform=platform, error=str(exc))
        return True
    return bool(rows)


def has_enabled_accounts(platform: str) -> bool:
    try:
        rows = d1.query(
            "SELECT 1 AS found FROM platform_accounts WHERE platform = ? AND enabled = 1 LIMIT 1", [platform]
        )
    except D1Error as exc:
        logger.error("db_has_enabled_accounts_failed", platform=platform, error=str(exc))
        return False
    return bool(rows)


# --- tài khoản: ghi ------------------------------------------------------------------------------------------------


def rest_account_until(platform: str, needle: str, until: Any) -> bool:
    try:
        rows = d1.query(
            f"UPDATE platform_accounts SET cooldown_until = ? WHERE platform = ? AND {_KEY_MATCH} RETURNING id",
            [d1.iso(until), platform, needle, needle],
        )
    except D1Error as exc:
        logger.error("db_rest_account_failed", platform=platform, error=str(exc))
        return False
    return bool(rows)


def update_account_cookie(platform: str, row_id: int, cookie: str) -> bool:
    try:
        rows = d1.query(
            "UPDATE platform_accounts SET cookie = ? WHERE platform = ? AND id = ? RETURNING id",
            [cookie, platform, row_id],
        )
    except D1Error as exc:
        logger.error("db_update_account_cookie_failed", platform=platform, row_id=row_id, error=str(exc))
        return False
    return bool(rows)


def reactivate_account(platform: str, account_id: str) -> bool | None:
    """True/False = có/không có dòng khớp; None = lỗi D1 (đã log)."""
    try:
        rows = d1.query(
            "UPDATE platform_accounts SET enabled = 1, status = 'active', consecutive_failures = 0, cooldown_until = NULL, "
            "last_check_note = NULL, last_checked_at = ? WHERE platform = ? AND account_id = ? RETURNING id",
            [d1.iso(), platform, account_id],
        )
    except D1Error as exc:
        logger.error("db_reactivate_account_failed", platform=platform, account_id=account_id, error=str(exc))
        return None
    return bool(rows)


def update_tiktok_identity(row_id: int, *, device_id: str, odin_id: str, cookie: str, lookup_key: str | None) -> bool:
    try:
        if lookup_key:
            d1.query(
                "UPDATE platform_accounts SET account_id = ?, token = ?, cookie = ?, "
                "email = CASE WHEN coalesce(email, '') = '' THEN ? ELSE email END WHERE platform = 'tiktok' AND id = ?",
                [device_id, odin_id, cookie, lookup_key, row_id],
            )
        else:
            d1.query(
                "UPDATE platform_accounts SET account_id = ?, token = ?, cookie = ? WHERE platform = 'tiktok' AND id = ?",
                [device_id, odin_id, cookie, row_id],
            )
    except D1Error as exc:
        logger.error("db_update_tiktok_identity_failed", row_id=row_id, error=str(exc))
        return False
    return True


def record_account_outcome(
    platform: str, account_id: str, *, success: bool, hard_failure: bool, note: str | None
) -> dict[str, Any] | None:
    """Trả dòng {consecutive_failures, cooldown_until} của nhánh lỗi nhẹ (để chỗ gọi log), còn lại None. `note` là chẩn đoán đã tính
    sẵn cho nhánh hard_failure. Lỗi D1 được log rồi nuốt như bản Postgres."""
    try:
        if success:
            d1.query(
                "UPDATE platform_accounts SET status = 'active', consecutive_failures = 0, cooldown_until = NULL, "
                "last_check_note = NULL, last_checked_at = ? WHERE platform = ? AND account_id = ?",
                [d1.iso(), platform, account_id],
            )
        elif hard_failure:
            d1.query(
                "UPDATE platform_accounts SET status = 'checkpoint', enabled = 0, consecutive_failures = consecutive_failures + 1, "
                "last_check_note = ?, last_checked_at = ? WHERE platform = ? AND account_id = ?",
                [note, d1.iso(), platform, account_id],
            )
        else:
            minutes = f"MIN(5 * (1 << MIN(consecutive_failures + 1, {_COOLDOWN_MAX_EXPONENT})), 120)"
            rows = d1.query(
                "UPDATE platform_accounts SET consecutive_failures = consecutive_failures + 1, "
                f"cooldown_until = {_SQL_NOW_PLUS.format(minutes=minutes)} "
                "WHERE platform = ? AND account_id = ? RETURNING consecutive_failures, cooldown_until",
                [platform, account_id],
            )
            return rows[0] if rows else None
    except D1Error as exc:
        logger.error(
            "db_record_account_outcome_failed",
            platform=platform,
            account_id=account_id,
            success=success,
            hard_failure=hard_failure,
            error=str(exc),
        )
    return None


def record_cookie_check(platform: str, account_id: str, *, status: str, note: str | None) -> bool:
    try:
        d1.query(
            "UPDATE platform_accounts SET last_checked_at = ?, last_check_status = ?, last_check_note = ? "
            "WHERE platform = ? AND account_id = ?",
            [d1.iso(), status, note, platform, account_id],
        )
    except D1Error as exc:
        logger.error(
            "db_record_cookie_check_failed", platform=platform, account_id=account_id, status=status, error=str(exc)
        )
        return False
    return True


def disable_account(platform: str, account_id: str, reason: str, note: str | None) -> bool:
    try:
        d1.query(
            "UPDATE platform_accounts SET enabled = 0, last_check_note = ?, last_checked_at = ? WHERE platform = ? AND account_id = ?",
            [note, d1.iso(), platform, account_id],
        )
    except D1Error as exc:
        logger.error(
            "db_disable_account_failed", platform=platform, account_id=account_id, reason=reason, error=str(exc)
        )
        return False
    return True


# --- proxy ---------------------------------------------------------------------------------------------------------


def list_proxies(platform: str) -> list[dict[str, Any]]:
    try:
        rows = d1.query(
            "SELECT id, platform, proxy_url, username, password, login_use_proxy, provider_key, provider_ref "
            "FROM platform_proxies WHERE enabled = 1 AND platform = ? ORDER BY id ASC",
            [platform],
        )
    except D1Error as exc:
        logger.error("db_list_proxies_failed", platform=platform, error=str(exc))
        return []
    return [_proxy(row) for row in rows]


def claim_proxy(platform: str) -> dict[str, Any] | None:
    try:
        rows = d1.query(
            "UPDATE platform_proxies SET last_used_at = ? WHERE id = ("
            "SELECT id FROM platform_proxies WHERE enabled = 1 AND platform = ? "
            "AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY last_used_at ASC LIMIT 1) "
            f"RETURNING {_PROXY_FIELDS}",
            [d1.iso(), platform, d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_claim_proxy_failed", platform=platform, error=str(exc))
        return None
    return _proxy(rows[0]) if rows else None


def list_proxies_without_meta_accounts() -> list[dict[str, Any]]:
    try:
        rows = d1.query(
            "SELECT pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.provider_key "
            "FROM platform_proxies pp WHERE pp.enabled = 1 AND pp.platform NOT IN ('facebook', 'threads') "
            "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= ?) "
            "AND NOT EXISTS (SELECT 1 FROM platform_proxies meta WHERE meta.platform IN ('facebook', 'threads') "
            "AND meta.proxy_url = pp.proxy_url) ORDER BY pp.id",
            [d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_list_proxies_without_meta_accounts_failed", error=str(exc))
        return []
    return [_proxy(row) for row in rows]


def list_public_page_proxies() -> list[dict[str, Any]]:
    try:
        rows = d1.query(
            "SELECT id, platform, proxy_url, username, password, login_use_proxy, provider_key, ip_type "
            "FROM platform_proxies WHERE enabled = 1 AND platform = 'public' "
            "AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY id",
            [d1.iso()],
        )
    except D1Error as exc:
        logger.error("db_list_public_page_proxies_failed", error=str(exc))
        return []
    return [_proxy(row) for row in rows]


def provider_key_for_proxy_url(proxy_url: str) -> str | None:
    try:
        rows = d1.query(
            "SELECT provider_key FROM platform_proxies WHERE proxy_url = ? AND provider_key IS NOT NULL ORDER BY id LIMIT 1",
            [proxy_url],
        )
    except D1Error as exc:
        logger.error("db_provider_key_for_proxy_url_failed", proxy_url=proxy_url, error=str(exc))
        return None
    return rows[0]["provider_key"] if rows else None


def proxy_row_for_url(proxy_url: str) -> dict[str, Any] | None:
    try:
        rows = d1.query(
            f"SELECT {_PROXY_FIELDS} FROM platform_proxies WHERE proxy_url = ? ORDER BY id LIMIT 1", [proxy_url]
        )
    except D1Error as exc:
        logger.error("db_proxy_row_for_url_failed", proxy_url=proxy_url, error=str(exc))
        return None
    return _proxy(rows[0]) if rows else None


def list_proxies_by_provider(provider_key: str) -> list[dict[str, Any]]:
    try:
        rows = d1.query(
            f"SELECT {_PROXY_FIELDS} FROM platform_proxies WHERE provider_key = ? ORDER BY id", [provider_key]
        )
    except D1Error as exc:
        logger.error("db_list_proxies_by_provider_failed", provider_key=provider_key, error=str(exc))
        return []
    return [_proxy(row) for row in rows]


def platform_has_any_proxy(platform: str) -> bool:
    try:
        rows = d1.query("SELECT 1 AS found FROM platform_proxies WHERE platform = ? LIMIT 1", [platform])
    except D1Error as exc:
        logger.error("db_platform_has_any_proxy_failed", platform=platform, error=str(exc))
        return False
    return bool(rows)


def proxy_failures(platform: str, proxy_url: str) -> int | None:
    """consecutive_failures hiện tại của một proxy (None = không có dòng) - để log "proxy_recovered"."""
    rows = d1.query(
        "SELECT consecutive_failures FROM platform_proxies WHERE platform = ? AND proxy_url = ?", [platform, proxy_url]
    )
    return int(rows[0]["consecutive_failures"]) if rows else None


def record_proxy_success(platform: str, proxy_url: str) -> None:
    d1.query(
        "UPDATE platform_proxies SET status = 'active', consecutive_failures = 0, cooldown_until = NULL "
        "WHERE platform = ? AND proxy_url = ?",
        [platform, proxy_url],
    )


def record_proxy_failure(
    platform: str, proxy_url: str, base_minutes: float, max_minutes: float
) -> dict[str, Any] | None:
    minutes = f"MIN((1 << MIN(consecutive_failures + 1, {_COOLDOWN_MAX_EXPONENT})) * ?, ?)"
    rows = d1.query(
        "UPDATE platform_proxies SET status = 'degraded', consecutive_failures = consecutive_failures + 1, "
        f"cooldown_until = {_SQL_NOW_PLUS.format(minutes=minutes)} WHERE platform = ? AND proxy_url = ? "
        "RETURNING id, consecutive_failures, cooldown_until",
        [base_minutes, max_minutes, platform, proxy_url],
    )
    return rows[0] if rows else None


def mark_proxy_used(proxy_id: int) -> None:
    d1.query("UPDATE platform_proxies SET last_used_at = ? WHERE id = ?", [d1.iso(), proxy_id])


def lease_candidates(platform: str) -> list[dict[str, Any]]:
    rows = d1.query(
        f"SELECT {_PROXY_FIELDS} FROM platform_proxies WHERE enabled = 1 AND platform = ? "
        "AND (cooldown_until IS NULL OR cooldown_until <= ?) ORDER BY last_used_at ASC",
        [platform, d1.iso()],
    )
    return [{**row, "login_use_proxy": bool(row["login_use_proxy"])} for row in rows]


def public_proxy_row(proxy_id: int) -> tuple[dict[str, Any] | None, bool]:
    """(dòng proxy đang bật hoặc None, có phải đường ra của tài khoản Meta không)."""
    rows = d1.query("SELECT * FROM platform_proxies WHERE id = ? AND enabled = 1", [proxy_id])
    if not rows:
        return None, False
    meta = d1.query(
        "SELECT 1 AS meta FROM platform_proxies WHERE platform IN ('facebook', 'threads') AND proxy_url = ? LIMIT 1",
        [rows[0]["proxy_url"]],
    )
    return rows[0], bool(meta)


# --- nguồn proxy ---------------------------------------------------------------------------------------------------


def get_provider_row(key: str) -> dict[str, Any] | None:
    rows = d1.query("SELECT key, api_url, token FROM proxy_providers WHERE key = ?", [key])
    return rows[0] if rows else None


def record_provider_use(key: str, status: str) -> None:
    d1.query(
        "UPDATE proxy_providers SET last_used_at = ?, last_status = ? WHERE key = ?", [d1.iso(), status[:300], key]
    )


# --- sự kiện tài khoản ---------------------------------------------------------------------------------------------


def record_event(platform: str, key: str, event: str, details: dict[str, Any]) -> None:
    d1.query(
        "INSERT INTO account_events (at, platform, account_key, event, details) VALUES (?, ?, ?, ?, ?)",
        [d1.iso(), platform, key, event, json.dumps(details, default=str, ensure_ascii=False)],
    )


def timeline(platform: str, keys: list[str], limit: int) -> list[dict[str, Any]]:
    rows = d1.query(
        "SELECT at, account_key, event, details FROM account_events "
        "WHERE platform = ? AND account_key IN (SELECT value FROM json_each(?)) ORDER BY at DESC, id DESC LIMIT ?",
        [platform, json.dumps([key.strip().lower() for key in keys if key]), limit],
    )
    return list(reversed([{**row, "details": d1.json_value(row["details"]) or {}} for row in rows]))
