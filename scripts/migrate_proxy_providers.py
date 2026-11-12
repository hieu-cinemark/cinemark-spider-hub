"""Migration: điền `platform_proxies.provider_key` (và `provider_ref`) cho cơ chế nguồn
proxy MỘT KIỂU - mỗi nguồn là một link reset + một key.

Vì sao cần: trước đó không có quan hệ nào giữa một dòng platform_proxies và nguồn proxy
sở hữu nó - code phải đoán bằng cách so `proxy_url` với `host:port` trong token của từng
gói (clients/proxy_provider.py), và loại của gói thì đoán theo URL. Migration này ghi kết
quả đoán ấy vào cột, để từ đây có một nguồn sự thật.

Luật đoán (giữ nguyên hành vi cũ):
  provider_key của một dòng platform_proxies:
    - gói nào có host:port trong token khớp proxy_url của dòng đó
    - nếu không gói nào khớp: gói nào đang giữ cổng đó (nhận ra qua last_status mà gói đó
      tự ghi khi xoay thành công - "ok: <cổng> <ip cũ> -> <ip mới>")
    - nếu vẫn không: NULL (proxy nhập tay/tĩnh)

CÁCH CHẠY (luôn dry-run trước):
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py            # chỉ in kế hoạch
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py --apply    # ghi vào DB
    PYTHONPATH=. .venv/bin/python scripts/migrate_proxy_providers.py --revert   # hoàn tác

`--revert` trả lại provider_key/provider_ref mà lần điền gần nhất đã đặt. Script ghi lại
những gì nó đã đổi vào bảng `proxy_provider_migration_log` (tự tạo) để lần `--revert` sau
biết đúng chỗ cần trả lại.

Idempotent: chạy lại `--apply` không ghi đè dòng đã có provider_key.

Ngoài ra script còn đổi TÊN các gói còn mang tên nhà cung cấp cũ (proxiestrust_*) thành
tên trung tính (default / tiktok_us), và cập nhật mọi tham chiếu tới tên đó:
platform_proxies.provider_key và giá trị setting tiktok_synthetic_provider. Tên gói là
tùy ý (chỉ là id), nên đổi không ảnh hưởng hành vi - nhưng giữ tên nhà cung cấp trong DB
thì mọi thứ sau này lại phải dính vào một nhà cung cấp cụ thể.

THỨ TỰ TRIỂN KHAI: chạy `--apply` (đổi tên + provider_key) TRƯỚC khi deploy code mới, vì
code mới mặc định tìm gói tên trung tính. Nếu deploy trước, tiktok_synthetic_provider sẽ
không tìm thấy gói nào -> rơi về pool proxy thường.

Cột `proxy_providers.ip_allowlist` (chế độ "cấp quyền theo IP nguồn") đã bị bỏ cùng API
lease của nhà cung cấp cũ, nhưng phía ghi (cinemark-api) còn ghi cột đó - nên chỉ xoá
khi đã deploy cả hai phía: `--apply --drop-ip-allowlist`.

Cột `proxy_providers.kind` (loại của gói) cũng không còn dùng - code mới không phân loại
gói nữa, mọi nguồn dùng chung một cơ chế. Cột được xoá kèm lúc xoá ip_allowlist.
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


def ensure_columns(conn) -> None:
    """Thêm cột mới nếu chưa có (ADD COLUMN IF NOT EXISTS - idempotent, chạy lại vô
    hại). Cùng câu DDL với db/proxy_settings._ensure_tables, để script này chạy được
    ngay trên một DB chưa migrate."""
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_key text")
    conn.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_ref text")
    conn.execute(LOG_TABLE)
    conn.commit()


def build_rename_plan(conn) -> list[tuple[str, str]]:
    """(tên cũ, tên mới) cho các gói còn mang tên nhà cung cấp."""
    rows = conn.execute("SELECT key FROM proxy_providers ORDER BY key").fetchall()
    plan: list[tuple[str, str]] = []
    for row in rows:
        key = row["key"]
        if key.startswith(VENDOR_KEY_PREFIX):
            plan.append((key, key[len(VENDOR_KEY_PREFIX) :]))
    return plan


def build_plan(
    conn, renames: list[tuple[str, str]] | None = None
) -> tuple[list[tuple[int, str | None, str | None, str | None]], list[tuple[str, str | None]]]:
    """(thay đổi provider_key/provider_ref của proxy, thay đổi provider_ref của gói).

    `renames` là danh sách (tên cũ, tên mới) sẽ được ghi - kế hoạch được tính trên tên
    MỚI để nó khớp với trạng thái DB sau khi đổi tên."""
    rename_map = dict(renames or [])
    providers = conn.execute("SELECT key, token, last_status FROM proxy_providers ORDER BY key").fetchall()
    proxies = conn.execute("SELECT id, proxy_url, provider_key, provider_ref FROM platform_proxies ORDER BY id").fetchall()

    by_token_port: dict[str, str] = {}
    status_owner: dict[str, str] = {}
    for p in providers:
        key = rename_map.get(p["key"], p["key"])
        port = token_port(p["token"] or "")
        if port:
            by_token_port.setdefault(port, key)
        for candidate in status_ports(p["last_status"]):
            status_owner.setdefault(candidate, key)

    proxy_changes: list[tuple[int, str | None, str | None, str | None]] = []
    for row in proxies:
        url = row["proxy_url"] or ""
        owner = by_token_port.get(url) or status_owner.get(url)
        current = row["provider_key"]
        if current is not None:
            current = rename_map.get(current, current)
        # provider_ref: mã riêng của cổng trong hệ thống nhà cung cấp. Với gói cổng-xoay-cố-định
        # thì mã đó CHÍNH LÀ cổng (host:port) - đủ để nhà cung cấp biết đổi IP cổng nào.
        ref = url if owner else None
        if current != owner or row["provider_ref"] != ref:
            proxy_changes.append((row["id"], current, owner, ref))
    return proxy_changes, []


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="ghi thay đổi vào DB (mặc định chỉ in)")
    parser.add_argument("--revert", action="store_true", help="hoàn tác lần điền gần nhất")
    parser.add_argument(
        "--drop-ip-allowlist",
        action="store_true",
        help="xoa cot proxy_providers.ip_allowlist va proxy_providers.kind (khong con dung)",
    )
    args = parser.parse_args(argv)

    try:
        with connect() as conn:
            ensure_columns(conn)
            if args.revert:
                return _revert(conn)
            renames = build_rename_plan(conn)
            proxy_changes, _ = build_plan(conn, renames)
    except Exception as exc:  # noqa: BLE001 - in lỗi gọn, không lộ DSN
        print(f"KHONG_DOC_DUOC_DU_LIEU: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print("=" * 72)
    print("Ke hoach: doi ten goi (bo ten nha cung cap cu)")
    print("=" * 72)
    for old, new in renames:
        print(f"  {old:32s} -> {new}")
    if not renames:
        print("  (khong co goi nao mang ten nha cung cap)")

    print()
    print("=" * 72)
    print("Ke hoach: platform_proxies.provider_key + provider_ref")
    print("=" * 72)
    for proxy_id, old, new, ref in proxy_changes:
        print(f"  proxy id {proxy_id:<4} {old or '(NULL)':<24} -> {new or '(NULL - nhap tay/tinh)'}")
        print(f"            provider_ref: {old and ''}{ref or '(NULL)'}")
    if not proxy_changes:
        print("  (khong co thay doi)")

    if not args.apply:
        print()
        print("DRY-RUN: chua ghi gi vao DB. Them --apply de ghi.")
        return 0

    try:
        with connect() as conn:
            if args.drop_ip_allowlist:
                conn.execute("ALTER TABLE proxy_providers DROP COLUMN IF EXISTS ip_allowlist")
                conn.execute("ALTER TABLE proxy_providers DROP COLUMN IF EXISTS kind")
            apply_renames(conn, renames)
            for proxy_id, old, new, ref in proxy_changes:
                conn.execute(
                    "UPDATE platform_proxies SET provider_key = %s, provider_ref = %s WHERE id = %s",
                    (new, ref, proxy_id),
                )
                conn.execute(
                    "INSERT INTO proxy_provider_migration_log (ran_at, proxy_id, old_provider_key, new_provider_key, "
                    "old_provider_ref, new_provider_ref) VALUES (now(), %s, %s, %s, %s, %s)",
                    (proxy_id, old, new, None, ref),
                )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"LOI_KHI_GHI: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print()
    print(f"Da doi ten {len(renames)} goi, ghi {len(proxy_changes)} dong proxy.")
    if args.drop_ip_allowlist:
        print("Da xoa cot proxy_providers.ip_allowlist va proxy_providers.kind.")
    print("Hoan tac: --revert")
    return 0


def _revert(conn) -> int:
    """Trả lại các dòng mà lần điền gần nhất (chưa revert) đã đổi.

    Đổi tên gói được hoàn tác SAU cùng các thay đổi khác, vì các bản ghi provider_key
    tham chiếu tới tên MỚI của gói."""
    rows = conn.execute(
        "SELECT id, provider_key, proxy_id, old_provider_key, new_provider_key, "
        "old_provider_ref, new_provider_ref, old_key, new_key "
        "FROM proxy_provider_migration_log WHERE reverted_at IS NULL ORDER BY id"
    ).fetchall()
    if not rows:
        print("Khong co gi de hoan tac.")
        return 0
    renames = [row for row in rows if row["old_key"]]
    for row in rows:
        if row["old_key"]:
            continue
        if row["proxy_id"] is not None:
            conn.execute(
                "UPDATE platform_proxies SET provider_key = %s, provider_ref = %s WHERE id = %s",
                (row["old_provider_key"], row["old_provider_ref"], row["proxy_id"]),
            )
    for row in renames:
        conn.execute("UPDATE proxy_providers SET key = %s WHERE key = %s", (row["old_key"], row["new_key"]))
        conn.execute(
            "UPDATE platform_proxies SET provider_key = %s WHERE provider_key = %s", (row["old_key"], row["new_key"])
        )
        conn.execute(
            "UPDATE proxy_settings SET settings = jsonb_set(settings, '{tiktok_synthetic_provider}', %s::jsonb) "
            "WHERE id = 1 AND settings->>'tiktok_synthetic_provider' = %s",
            (json.dumps(row["old_key"]), row["new_key"]),
        )
    for row in rows:
        conn.execute("UPDATE proxy_provider_migration_log SET reverted_at = now() WHERE id = %s", (row["id"],))
    conn.commit()
    print(f"Da hoan tac {len(rows)} dong.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
