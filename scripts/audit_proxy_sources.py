"""Khảo sát CHỈ ĐỌC các nguồn proxy trên Postgres (Supabase).

In ra (không bao giờ in token / user / mật khẩu / IP thật):
  - các dòng proxy_providers (nguồn proxy): có link reset không, có key không, lần dùng
    cuối và kết quả;
  - số dòng platform_proxies đang bật, theo trạng thái pool / nền tảng / ip_type;
  - số proxy đang bật thuộc mỗi nguồn (theo cột `provider_key` nếu DB đã có, nếu không thì
    theo cổng host:port trong token của nguồn, hoặc theo cổng mà nguồn tự ghi trong
    last_status - cùng luật mà clients/proxy_provider.reset_provider_for_proxy_url dùng);
  - proxy đang bật không khớp nguồn nào (= nhập tay / tĩnh);
  - số tài khoản đang ghim vào mỗi proxy (để kiểm tra nguyên tắc "một proxy, một tài
    khoản");
  - các setting liên quan, kèm giá trị mặc định hiệu lực (đọc từ
    social_crawler.db.proxy_settings.DEFAULTS) khi DB chưa lưu.

Chạy:
    PYTHONPATH=. .venv/bin/python scripts/audit_proxy_sources

Script chỉ SELECT. Nếu không kết nối được thì in lỗi và thoát với mã 1 (không raise
stack trace có thể lộ DSN).
"""

from __future__ import annotations

import re
import sys

from social_crawler.db.connection import connect
from social_crawler.db.proxy_settings import DEFAULTS

_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_HOSTPORT = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b|\b[a-z0-9.-]+:\d+\b", re.IGNORECASE)


def _redact(text: str) -> str:
    """Bỏ mọi IP/host:port khỏi chuỗi in ra - repo public, log ra ngoài không được lộ
    địa chỉ proxy thật."""
    text = _HOSTPORT.sub("<host:port>", text)
    return _IPV4.sub("<ip>", text)


def _port_of(token: str) -> str | None:
    """host:port của nguồn "cổng xoay cố định" (token dạng host:port:user:pass), None nếu
    token không phải dạng đó."""
    parts = (token or "").split(":")
    if len(parts) != 4 or not parts[0] or not parts[1].isdigit():
        return None
    return f"{parts[0]}:{parts[1]}"


def _ports_in_last_status(status: str | None) -> set[str]:
    """Các cổng host:port mà nguồn tự ghi trong last_status (ví dụ "ok: <cổng> <ip cũ> ->
    <ip mới>") - dùng để gán proxy về đúng nguồn mà không cần gọi API ngoài."""
    if not status:
        return set()
    return {m.group(0) for m in _HOSTPORT.finditer(status)}


def main() -> int:
    try:
        with connect() as conn:
            providers = conn.execute(
                "SELECT key, api_url, token, last_used_at, last_status FROM proxy_providers ORDER BY key"
            ).fetchall()
            proxies = conn.execute(
                "SELECT id, platform, proxy_url, enabled, status, ip_type, provider_key, "
                "(SELECT count(*) FROM platform_accounts pa WHERE pa.assigned_proxy_id = pp.id "
                " AND pa.enabled = true AND pa.status != 'checkpoint') AS assigned_accounts "
                "FROM platform_proxies pp ORDER BY id"
            ).fetchall()
            settings_row = conn.execute("SELECT settings FROM proxy_settings WHERE id = 1").fetchone()
    except Exception as exc:  # noqa: BLE001 - script khảo sát: in lỗi gọn, không lộ DSN
        print(f"KHONG_DOC_DUOC_DU_LIEU: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    stored = settings_row.get("settings") if settings_row else {}
    stored = stored if isinstance(stored, dict) else {}

    print("=" * 72)
    print("1) proxy_providers - nguon proxy (link reset + key)")
    print("=" * 72)
    provider_port: dict[str, str] = {}
    provider_status_ports: dict[str, set[str]] = {}
    for row in providers:
        port = _port_of(row.get("token") or "")
        if port:
            provider_port[row["key"]] = port
        provider_status_ports[row["key"]] = _ports_in_last_status(row.get("last_status"))
    if not providers:
        print("(khong co dong nao)")
    for row in providers:
        status = _redact((row.get("last_status") or "")[:80])
        print(
            f"  {row['key']:32s} link_reset={'co' if row.get('api_url') else 'khong'} "
            f"key={'co' if row.get('token') else 'khong'} "
            f"lan_dung_cuoi={row.get('last_used_at')} ket_qua={status or '-'}"
        )

    print()
    print("=" * 72)
    print("2) platform_proxies")
    print("=" * 72)
    total = len(proxies)
    enabled = [p for p in proxies if p.get("enabled")]
    by_status: dict[str, int] = {}
    for p in enabled:
        by_status[p.get("status") or "unknown"] = by_status.get(p.get("status") or "unknown", 0) + 1
    print(f"  tong so dong              : {total}")
    print(f"  dang bat                  : {len(enabled)}")
    print(f"  tat                       : {total - len(enabled)}")
    for status, count in sorted(by_status.items()):
        print(f"    trang thai pool = {status:12s}: {count}")
    by_platform: dict[str, int] = {}
    for p in enabled:
        by_platform[p.get("platform") or "?"] = by_platform.get(p.get("platform") or "?", 0) + 1
    for platform, count in sorted(by_platform.items()):
        print(f"    nen tang {platform:16s}: {count}")
    by_ip_type: dict[str, int] = {}
    for p in enabled:
        by_ip_type[p.get("ip_type") or "unknown"] = by_ip_type.get(p.get("ip_type") or "unknown", 0) + 1
    for ip_type, count in sorted(by_ip_type.items()):
        print(f"    ip_type {ip_type:20s}: {count}")

    print()
    print("=" * 72)
    print("3) proxy dang bat thuoc nguon nao")
    print("=" * 72)
    owner: dict[str, list[int]] = {}
    unmatched: list[int] = []
    for p in enabled:
        url = p.get("proxy_url") or ""
        matched = p.get("provider_key")
        if matched is None:
            matched = next((key for key, port in provider_port.items() if port == url), None)
        if matched is None:
            matched = next((key for key, ports in provider_status_ports.items() if url in ports), None)
        if matched:
            owner.setdefault(matched, []).append(p["id"])
        else:
            unmatched.append(p["id"])
    for row in providers:
        print(f"  {row['key']:32s} so proxy dang bat: {len(owner.get(row['key'], []))}")
    print(f"  khong khop nguon nao (nhap tay/tinh): {len(unmatched)}  (id: {unmatched})")

    print()
    print("=" * 72)
    print("4) nguyen tac 'mot proxy, mot tai khoan'")
    print("=" * 72)
    shared = [p for p in proxies if int(p.get("assigned_accounts") or 0) > 1]
    print(f"  proxy bi gan nhieu tai khoan: {len(shared)}  (id: {[p['id'] for p in shared]})")
    for p in shared:
        print(f"    proxy id {p['id']}: {int(p['assigned_accounts'])} tai khoan")

    print()
    print("=" * 72)
    print("5) setting lien quan (gia tri hieu luc = DB tran DEFAULTS)")
    print("=" * 72)
    for key in (
        "tiktok_synthetic_provider",
        "provider_min_call_interval_seconds",
        "provider_max_cooldown_wait_seconds",
        "provider_request_timeout_seconds",
    ):
        effective = stored.get(key, DEFAULTS.get(key))
        source = "DB" if key in stored else "mac dinh"
        print(f"  {key:42s} = {effective!r} ({source})")
    synthetic = stored.get("tiktok_synthetic_provider", DEFAULTS.get("tiktok_synthetic_provider"))
    if synthetic:
        row = next((r for r in providers if r["key"] == synthetic), None)
        if row is None:
            print(f"    -> KHONG co dong nguon '{synthetic}' trong proxy_providers")
        else:
            print(f"    -> nguon ton tai, so proxy dang bat: {len(owner.get(synthetic, []))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
