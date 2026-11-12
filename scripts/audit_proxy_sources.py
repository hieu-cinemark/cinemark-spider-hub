"""Khảo sát CHỈ ĐỌC các nguồn proxy trên Postgres (Supabase) - giai đoạn 0 của việc thiết kế
lại proxy nhiều nguồn.

In ra (không bao giờ in token / user / mật khẩu / IP thật):
  - số dòng proxy_providers theo loại: cột `kind` nếu DB đã có, nếu không thì đoán bằng
    luật cũ (đoán theo URL/token, xem clients/proxy_provider.py) - webtui / zingproxy /
    rotate_url (token dạng host:port:user:pass) / static;
  - số dòng platform_proxies đang bật, theo trạng thái pool / nền tảng / ip_type;
  - số proxy đang bật thuộc mỗi nhà cung cấp (khớp theo cột `provider_key` nếu DB đã
    có, nếu không thì theo cổng host:port trong token của provider, hoặc theo cổng mà
    nhà cung cấp tự ghi trong last_status - cùng luật mà
    clients/proxy_provider.reset_provider_for_proxy_url dùng);
  - proxy đang bật không khớp provider nào (= nhập tay / tĩnh);
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
from urllib.parse import urlsplit

from social_crawler.db.connection import connect
from social_crawler.db.proxy_settings import DEFAULTS

# Chuỗi nhận diện loại nhà cung cấp - chỉ dùng để đoán khi cột `kind` còn rỗng.
_WEBTUI_MARKERS = ("webtui",)
_ZINGPROXY_HOSTS = {"api.zingproxy.com"}

_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_HOSTPORT = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b|\b[a-z0-9.-]+:\d+\b", re.IGNORECASE)


def _redact(text: str) -> str:
    """Bỏ mọi IP/host:port khỏi chuỗi in ra - repo public, log ra ngoài không được lộ
    địa chỉ proxy thật."""
    text = _HOSTPORT.sub("<host:port>", text)
    return _IPV4.sub("<ip>", text)


def _classify_provider(api_url: str, token: str, kind: str = "") -> str:
    """Loại của một dòng proxy_providers: cột `kind` nếu có, nếu không đoán bằng luật cũ."""
    if (kind or "").strip():
        return kind.strip()
    host = ""
    try:
        host = (urlsplit(api_url or "").hostname or "").lower()
    except ValueError:
        host = ""
    if any(marker in (api_url or "").lower() for marker in _WEBTUI_MARKERS):
        return "webtui"
    if host in _ZINGPROXY_HOSTS:
        return "zingproxy"
    parts = (token or "").split(":")
    if len(parts) == 4 and parts[0] and parts[1].isdigit():
        return "rotate_url"
    return "static"


def _port_of(token: str) -> str | None:
    """host:port của gói "cổng xoay cố định" (token dạng host:port:user:pass), None nếu
    token không phải dạng đó."""
    parts = (token or "").split(":")
    if len(parts) != 4 or not parts[0] or not parts[1].isdigit():
        return None
    return f"{parts[0]}:{parts[1]}"


def _ports_in_last_status(status: str | None) -> set[str]:
    """Các cổng host:port mà nhà cung cấp tự ghi trong last_status (ví dụ "ok: <cổng>
    <ip cũ> -> <ip mới>") - dùng để gán proxy webtui về đúng nhà cung cấp mà không cần
    gọi API ngoài."""
    if not status:
        return set()
    return {m.group(0) for m in _HOSTPORT.finditer(status)}


def main() -> int:
    try:
        with connect() as conn:
            providers = conn.execute(
                "SELECT key, api_url, token, kind, last_used_at, last_status FROM proxy_providers ORDER BY key"
            ).fetchall()
            proxies = conn.execute(
                "SELECT id, platform, proxy_url, enabled, status, ip_type, provider_key FROM platform_proxies ORDER BY id"
            ).fetchall()
            settings_row = conn.execute("SELECT settings FROM proxy_settings WHERE id = 1").fetchone()
    except Exception as exc:  # noqa: BLE001 - script khảo sát: in lỗi gọn, không lộ DSN
        print(f"KHONG_DOC_DUOC_DU_LIEU: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    stored = settings_row.get("settings") if settings_row else {}
    stored = stored if isinstance(stored, dict) else {}

    print("=" * 72)
    print("1) proxy_providers theo loai (cot 'kind', chua co thi doan theo URL/token)")
    print("=" * 72)
    by_kind: dict[str, list[str]] = {}
    provider_port: dict[str, str] = {}
    provider_status_ports: dict[str, set[str]] = {}
    for row in providers:
        kind = _classify_provider(row.get("api_url") or "", row.get("token") or "", row.get("kind") or "")
        by_kind.setdefault(kind, []).append(row["key"])
        port = _port_of(row.get("token") or "")
        if port:
            provider_port[row["key"]] = port
        provider_status_ports[row["key"]] = _ports_in_last_status(row.get("last_status"))
    if not providers:
        print("(khong co dong nao)")
    for kind in sorted(by_kind):
        print(f"  {kind:24s} {len(by_kind[kind]):3d}  {', '.join(by_kind[kind])}")

    print()
    print("=" * 72)
    print("2) proxy_providers - trang thai su dung gan nhat (khong in token / IP)")
    print("=" * 72)
    for row in providers:
        kind = _classify_provider(row.get("api_url") or "", row.get("token") or "", row.get("kind") or "")
        last_used = row.get("last_used_at")
        status = _redact((row.get("last_status") or "")[:80])
        print(
            f"  {row['key']:32s} kind={kind:24s} token={'co' if row.get('token') else 'khong'} "
            f"lan_dung_cuoi={last_used} ket_qua={status or '-'}"
        )

    print()
    print("=" * 72)
    print("3) platform_proxies")
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
    print("4) proxy dang bat thuoc nha cung cap nao")
    print("=" * 72)
    owner: dict[str, list[int]] = {}
    unmatched: list[int] = []
    for p in enabled:
        url = p.get("proxy_url") or ""
        matched = p.get("provider_key")
        if matched is None:
            matched = next(
                (key for key, port in provider_port.items() if port == url),
                None,
            )
        if matched is None:
            matched = next(
                (key for key, ports in provider_status_ports.items() if url in ports),
                None,
            )
        if matched:
            owner.setdefault(matched, []).append(p["id"])
        else:
            unmatched.append(p["id"])
    for key in sorted(by_kind and {k for ks in by_kind.values() for k in ks}):
        print(f"  {key:32s} so proxy dang bat: {len(owner.get(key, []))}")
    print(f"  khong khop provider nao (nhap tay/tinh): {len(unmatched)}  (id: {unmatched})")

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
            print(f"    -> KHONG co dong provider '{synthetic}' trong proxy_providers")
        else:
            kind = _classify_provider(row.get("api_url") or "", row.get("token") or "", row.get("kind") or "")
            print(f"    -> provider ton tai, loai hien tai: {kind}")

    print()
    print("=" * 72)
    print("6) CANH BAO - goi xoay IP dang co proxy bat")
    print("=" * 72)
    warned = False
    for kind, keys in by_kind.items():
        if kind not in {"rotate_url", "zingproxy", "webtui"}:
            continue
        for key in keys:
            used = owner.get(key, [])
            row = next(r for r in providers if r["key"] == key)
            print(
                f"  provider {key} (loai {kind}): proxy dang bat = {len(used)}, "
                f"lan dung cuoi = {row.get('last_used_at')}"
            )
            if used:
                warned = True
                print(f"    -> DANG BAT: {len(used)} proxy. Can quyet dinh noi dich truoc khi xoa.")
    if not warned:
        print("  Khong co goi xoay IP nao dang co proxy bat.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
