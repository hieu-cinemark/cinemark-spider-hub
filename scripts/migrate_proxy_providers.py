"""Migration: điền `platform_proxies.provider_key` (và `provider_ref`) cho cơ chế nguồn
proxy MỘT KIỂU - mỗi nguồn là một link reset + một key.

Vì sao cần: trước đó không có quan hệ nào giữa một dòng platform_proxies và nguồn proxy
sở hữu nó - code phải đoán bằng cách so `proxy_url` với `host:port` trong token của từng
gói (clients/proxy_provider.py), và loại của gói thì đoán theo URL. Migration này ghi kết
quả đoán ấy vào cột, để từ đây có một nguồn sự thật.

Luật đoán (giữ nguyên hành vi cũ), CHỈ cho dòng còn provider_key NULL:
  - gói nào có host:port trong token khớp proxy_url của dòng đó -> provider_key = gói đó,
    provider_ref = host:port (gói "cổng xoay cố định": link reset không cần mã cổng)
  - nếu không: gói nào đang giữ cổng đó (nhận ra qua last_status mà gói đó tự ghi khi xoay
    - "ok: <cổng> <ip cũ> -> <ip mới>") -> provider_key = gói đó, provider_ref ĐỂ NGUYÊN:
    webtui cần proxy_id dạng số, zingproxy cần key riêng của cổng - script không biết các
    mã đó, người vận hành nhập trên dashboard (và sửa api_url thành link có {token}/{ref}).
  - nếu vẫn không: giữ NULL (proxy nhập tay/tĩnh)

CÁCH CHẠY (luôn dry-run trước - dry-run KHÔNG ghi gì, kể cả DDL):
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py            # chỉ in kế hoạch
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py --apply    # ghi vào DB
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py --revert   # hoàn tác

Idempotent: chạy lại `--apply` không bao giờ ghi đè dòng đã có provider_key, và không đụng
provider_ref đã có giá trị (kể cả giá trị người vận hành nhập tay).

`--revert` trả lại provider_key/provider_ref và tên gói mà các lần `--apply` chưa hoàn tác đã
đổi, theo thứ tự ngược (lần mới nhất trước), dựa trên bảng `proxy_provider_migration_log`.

Ngoài ra script còn đổi TÊN các gói còn mang tên nhà cung cấp cũ (proxiestrust_*) thành
tên trung tính (default / tiktok_us), và cập nhật mọi tham chiếu tới tên đó:
platform_proxies.provider_key và giá trị setting tiktok_synthetic_provider. Tên mới đã có
sẵn trong proxy_providers thì dừng, không ghi gì (báo trong kế hoạch).

THỨ TỰ TRIỂN KHAI: chạy `--apply` TRƯỚC khi deploy code mới - code mới SELECT cột
provider_ref, cột chưa có thì mọi câu đọc proxy lỗi. Code cũ vẫn chạy được sau khi apply.

Cột `proxy_providers.ip_allowlist` và `proxy_providers.kind` không còn dùng. Code cũ (trước
lần deploy này) của cả hai repo còn đọc/ghi ip_allowlist, nên xoá là một lệnh RIÊNG, chỉ chạy
khi cả spider-hub và cinemark-api đã chạy bản mới: `--drop-ip-allowlist` (không kèm --apply).
"""

from __future__ import annotations

import argparse
import json
import re
import sys

from social_crawler.db.connection import connect

_HOSTPORT = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b|\b[a-z0-9.-]+:\d+\b", re.IGNORECASE)

LOG_TABLE = """
CREATE TABLE IF NOT EXISTS proxy_provider_migration_log (
    id bigserial PRIMARY KEY,
    ran_at timestamptz NOT NULL DEFAULT now(),
    provider_key text NOT NULL DEFAULT '',
    proxy_id int,
    old_provider_key text,
    new_provider_key text,
    old_provider_ref text,
    new_provider_ref text,
    old_key text,
    new_key text,
    reverted_at timestamptz
)
"""

# Tên gói còn mang tên nhà cung cấp cũ -> tên trung tính. Tên gói chỉ là id, không ảnh
# hưởng hành vi, nhưng để tên nhà cung cấp trong DB thì kiến trúc mới lại bị kéo về một
# nhà cung cấp cụ thể.
VENDOR_KEY_PREFIX = "proxiestrust_"


def token_port(token: str) -> str | None:
    """host:port trong token của gói cổng xoay cố định, None nếu token không phải dạng đó."""
    parts = (token or "").split(":")
    if len(parts) != 4 or not parts[0] or not parts[1].isdigit():
        return None
    return f"{parts[0]}:{parts[1]}"


def status_ports(last_status: str | None) -> set[str]:
    """Các cổng host:port mà gói tự ghi trong last_status."""
    if not last_status:
        return set()
    return {m.group(0) for m in _HOSTPORT.finditer(last_status)}


def existing_columns(conn, table: str) -> set[str]:
    """Cột hiện có của bảng (chỉ đọc information_schema - dùng được trong dry-run)."""
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    ).fetchall()
    return {row["column_name"] for row in rows}


def ensure_columns(conn) -> None:
    """Thêm cột mới nếu chưa có (ADD COLUMN IF NOT EXISTS - idempotent). Chỉ gọi khi --apply."""
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_key text")
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_ref text")
    conn.execute(LOG_TABLE)


def build_rename_plan(conn) -> tuple[list[tuple[str, str]], list[str]]:
    """((tên cũ, tên mới) cho các gói còn mang tên nhà cung cấp, [xung đột]). Xung đột = tên mới
    đã có sẵn - đổi tên sẽ vi phạm khoá chính, nên cả lần apply bị chặn."""
    keys = [row["key"] for row in conn.execute("SELECT key FROM proxy_providers ORDER BY key").fetchall()]
    plan: list[tuple[str, str]] = []
    conflicts: list[str] = []
    for key in keys:
        if key.startswith(VENDOR_KEY_PREFIX):
            new = key[len(VENDOR_KEY_PREFIX) :]
            if new in keys:
                conflicts.append(f"{key} -> {new}: tên mới đã có trong proxy_providers")
            else:
                plan.append((key, new))
    return plan, conflicts


def build_plan(
    conn, renames: list[tuple[str, str]] | None = None
) -> list[tuple[int, str | None, str | None, str | None, str | None]]:
    """[(proxy id, provider_key cũ, provider_key mới, provider_ref cũ, provider_ref mới)].

    Chỉ dòng còn provider_key NULL. Kế hoạch tính trên tên MỚI của gói để khớp trạng thái DB
    sau khi đổi tên. Cột chưa có (DB chưa migrate) thì coi như NULL."""
    rename_map = dict(renames or [])
    providers = conn.execute("SELECT key, token, last_status FROM proxy_providers ORDER BY key").fetchall()
    columns = existing_columns(conn, "platform_proxies")
    key_col = "provider_key" if "provider_key" in columns else "NULL::text AS provider_key"
    ref_col = "provider_ref" if "provider_ref" in columns else "NULL::text AS provider_ref"
    proxies = conn.execute(f"SELECT id, proxy_url, {key_col}, {ref_col} FROM platform_proxies ORDER BY id").fetchall()

    by_token_port: dict[str, str] = {}
    status_owner: dict[str, str] = {}
    for p in providers:
        key = rename_map.get(p["key"], p["key"])
        port = token_port(p["token"] or "")
        if port:
            by_token_port.setdefault(port, key)
        for candidate in status_ports(p["last_status"]):
            status_owner.setdefault(candidate, key)

    changes: list[tuple[int, str | None, str | None, str | None, str | None]] = []
    for row in proxies:
        if row["provider_key"] is not None:
            continue  # đã có nguồn (migration trước hoặc người vận hành) - không bao giờ ghi đè
        url = row["proxy_url"] or ""
        old_ref = row["provider_ref"]
        if url in by_token_port:
            owner, ref = by_token_port[url], old_ref or url
        elif url in status_owner:
            owner, ref = status_owner[url], old_ref
        else:
            continue
        changes.append((row["id"], None, owner, old_ref, ref))
    return changes


def apply_renames(conn, renames: list[tuple[str, str]]) -> None:
    """Đổi tên gói và mọi tham chiếu tới nó (provider_key của proxy, giá trị setting
    tiktok_synthetic_provider). Ghi vào bảng log để `--revert` trả lại được."""
    for old, new in renames:
        conn.execute("UPDATE proxy_providers SET key = %s WHERE key = %s", (new, old))
        conn.execute("UPDATE platform_proxies SET provider_key = %s WHERE provider_key = %s", (new, old))
        conn.execute(
            "UPDATE proxy_settings SET settings = jsonb_set(settings, '{tiktok_synthetic_provider}', %s::jsonb) "
            "WHERE id = 1 AND settings->>'tiktok_synthetic_provider' = %s",
            (json.dumps(new), old),
        )
        conn.execute(
            "INSERT INTO proxy_provider_migration_log (ran_at, provider_key, old_key, new_key) VALUES (now(), %s, %s, %s)",
            (new, old, new),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="ghi thay đổi vào DB (mặc định chỉ in)")
    parser.add_argument("--revert", action="store_true", help="hoàn tác các lần --apply chưa hoàn tác")
    parser.add_argument(
        "--drop-ip-allowlist",
        action="store_true",
        help="CHỈ xoá cột proxy_providers.ip_allowlist và kind (không dùng nữa) - chạy riêng, sau khi cả hai repo đã deploy",
    )
    args = parser.parse_args(argv)
    if sum((args.apply, args.revert, args.drop_ip_allowlist)) > 1:
        parser.error("--apply, --revert và --drop-ip-allowlist là ba lệnh riêng, chạy từng cái một")

    if args.drop_ip_allowlist:
        return _drop_old_columns()
    if args.revert:
        return _revert()

    try:
        with connect() as conn:
            renames, conflicts = build_rename_plan(conn)
            proxy_changes = build_plan(conn, renames)
            conn.rollback()  # dry-run và bước lập kế hoạch không ghi gì
    except Exception as exc:  # noqa: BLE001 - in lỗi gọn, không lộ DSN
        print(f"KHONG_DOC_DUOC_DU_LIEU: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print("=" * 72)
    print("Ke hoach: doi ten goi (bo ten nha cung cap cu)")
    print("=" * 72)
    for old, new in renames:
        print(f"  {old:32s} -> {new}")
    for conflict in conflicts:
        print(f"  XUNG DOT: {conflict}")
    if not renames and not conflicts:
        print("  (khong co goi nao mang ten nha cung cap)")

    print()
    print("=" * 72)
    print("Ke hoach: platform_proxies.provider_key + provider_ref (chi dong con NULL)")
    print("=" * 72)
    for proxy_id, _old, new, old_ref, ref in proxy_changes:
        note = "" if ref else "  <- can nhap provider_ref (ma cong cua nha cung cap) tren dashboard"
        print(
            f"  proxy id {proxy_id:<4} (NULL) -> {new}   provider_ref: {old_ref or '(NULL)'} -> {ref or '(NULL)'}{note}"
        )
    if not proxy_changes:
        print("  (khong co thay doi)")

    if conflicts:
        print()
        print("DUNG: co xung dot ten goi - xu ly tay roi chay lai. Khong ghi gi.")
        return 1
    if not args.apply:
        print()
        print("DRY-RUN: chua ghi gi vao DB (ke ca DDL). Them --apply de ghi.")
        return 0

    try:
        with connect() as conn:
            ensure_columns(conn)
            # Lập lại kế hoạch trong cùng transaction ghi - DB có thể đã đổi từ lúc in.
            renames, conflicts = build_rename_plan(conn)
            if conflicts:
                conn.rollback()
                print("DUNG: xung dot ten goi xuat hien luc ghi. Khong ghi gi.", file=sys.stderr)
                return 1
            proxy_changes = build_plan(conn, renames)
            apply_renames(conn, renames)
            for proxy_id, old, new, old_ref, ref in proxy_changes:
                conn.execute(
                    "UPDATE platform_proxies SET provider_key = %s, provider_ref = %s "
                    "WHERE id = %s AND provider_key IS NULL",
                    (new, ref, proxy_id),
                )
                conn.execute(
                    "INSERT INTO proxy_provider_migration_log (ran_at, proxy_id, old_provider_key, new_provider_key, "
                    "old_provider_ref, new_provider_ref) VALUES (now(), %s, %s, %s, %s, %s)",
                    (proxy_id, old, new, old_ref, ref),
                )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"LOI_KHI_GHI: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print()
    print(f"Da doi ten {len(renames)} goi, ghi {len(proxy_changes)} dong proxy.")
    print("Hoan tac: --revert")
    return 0


def _drop_old_columns() -> int:
    try:
        with connect() as conn:
            conn.execute("ALTER TABLE proxy_providers DROP COLUMN IF EXISTS ip_allowlist")
            conn.execute("ALTER TABLE proxy_providers DROP COLUMN IF EXISTS kind")
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"LOI_KHI_GHI: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print("Da xoa cot proxy_providers.ip_allowlist va proxy_providers.kind.")
    return 0


def _revert() -> int:
    """Trả lại mọi thay đổi chưa hoàn tác, lần mới nhất trước (id giảm dần): một dòng proxy bị
    đổi nhiều lần sẽ về đúng giá trị trước lần đầu. Đổi tên gói hoàn tác SAU CÙNG vì các bản ghi
    provider_key tham chiếu tới tên MỚI của gói."""
    try:
        with connect() as conn:
            if "id" not in existing_columns(conn, "proxy_provider_migration_log"):
                print("Khong co gi de hoan tac (chua tung --apply).")
                return 0
            rows = conn.execute(
                "SELECT id, proxy_id, old_provider_key, old_provider_ref, old_key, new_key "
                "FROM proxy_provider_migration_log WHERE reverted_at IS NULL ORDER BY id DESC"
            ).fetchall()
            if not rows:
                print("Khong co gi de hoan tac.")
                return 0
            for row in rows:
                if row["old_key"] is None and row["proxy_id"] is not None:
                    conn.execute(
                        "UPDATE platform_proxies SET provider_key = %s, provider_ref = %s WHERE id = %s",
                        (row["old_provider_key"], row["old_provider_ref"], row["proxy_id"]),
                    )
            for row in rows:
                if row["old_key"] is None:
                    continue
                conn.execute("UPDATE proxy_providers SET key = %s WHERE key = %s", (row["old_key"], row["new_key"]))
                conn.execute(
                    "UPDATE platform_proxies SET provider_key = %s WHERE provider_key = %s",
                    (row["old_key"], row["new_key"]),
                )
                conn.execute(
                    "UPDATE proxy_settings SET settings = jsonb_set(settings, '{tiktok_synthetic_provider}', %s::jsonb) "
                    "WHERE id = 1 AND settings->>'tiktok_synthetic_provider' = %s",
                    (json.dumps(row["old_key"]), row["new_key"]),
                )
            conn.execute(
                "UPDATE proxy_provider_migration_log SET reverted_at = now() WHERE id = ANY(%s)",
                ([row["id"] for row in rows],),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"LOI_KHI_GHI: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(f"Da hoan tac {len(rows)} dong.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
