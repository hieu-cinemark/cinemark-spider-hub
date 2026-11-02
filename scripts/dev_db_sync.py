"""Đồng bộ schema + dữ liệu từ Postgres production (Supabase) sang Postgres local.

Mục đích: chạy spider-hub (và cinemark-api) ở local mà không đụng vào dữ liệu prod.
Local Postgres (docker-compose service "postgres", db spider_hub_dev) chỉ có vài bảng
do scripts/dev_db_schema.sql tạo - thiếu ai_providers / ai_settings / proxy_settings /
proxy_providers và thiếu mấy cột của platform_accounts (needs_manual_login,
last_relogin_at, last_relogin_status). Script này:

  1. Introspect schema của prod, tạo bảng/cột còn thiếu ở local (idempotent).
  2. Copy dữ liệu sang local (INSERT ... ON CONFLICT DO UPDATE).

Chạy:
    cd /Users/mypc/cinemark-workspace/spider-hub
    source .venv/bin/activate
    python -m scripts.dev_db_sync            # schema + copy data
    python -m scripts.dev_db_sync --schema-only
    python -m scripts.dev_db_sync --no-copy

Cần: psycopg (đã có trong requirements), DATABASE_URL (prod) + LOCAL_DATABASE_URL (local).
KHÔNG ghi đè dữ liệu local đã tồn tại khác prod - copy là upsert theo khoá chính, nên
chạy lại nhiều lần vẫn an toàn.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

# (bảng, khoá chính) - copy theo thứ tự này để không vi phạm FK.
TABLES: list[tuple[str, tuple[str, ...]]] = [
    ("platform_accounts", ("id",)),
    ("platform_proxies", ("id",)),
    ("filter_keywords", ("id",)),
    ("ai_providers", ("key",)),
    ("ai_settings", ("id",)),
    ("proxy_settings", ("id",)),
    ("proxy_providers", ("key",)),
    ("crawl_schedules", ("platform",)),
    ("comment_crawl_schedules", ("platform",)),
]

# Cột tự tăng: prod dùng sequence, local tạo bằng serial -> bỏ default khi copy.
def _is_serial(default: str | None) -> bool:
    return bool(default) and "nextval(" in default


def _local_type(col: dict[str, Any]) -> str:
    """Chuyển kiểu của prod sang kiểu dùng được ở local (serial thay identity)."""
    dtype = col["data_type"]
    default = col["column_default"]
    if dtype == "user-defined" and col.get("udt_name"):
        return col["udt_name"]
    if dtype == "ARRAY":
        return col.get("array_type") or "text[]"
    if dtype == "character varying":
        return "text"
    if dtype == "timestamp with time zone":
        return "timestamptz"
    if dtype == "timestamp without time zone":
        return "timestamp"
    if dtype == "integer" and _is_serial(default):
        return "serial"
    if dtype == "bigint" and _is_serial(default):
        return "bigserial"
    if dtype == "smallint" and _is_serial(default):
        return "smallserial"
    return dtype


def _column_sql(col: dict[str, Any]) -> str:
    parts = [f'"{col["column_name"]}"', _local_type(col)]
    if col["is_nullable"] == "NO" and not _is_serial(col["column_default"]):
        parts.append("NOT NULL")
    default = col["column_default"]
    if default and not _is_serial(default):
        parts.append(f"DEFAULT {default}")
    return " ".join(parts)


def prod_conn() -> psycopg.Connection:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL chưa được set (prod Supabase) - xem .env.example")
    return psycopg.connect(url, row_factory=dict_row)


def local_conn() -> psycopg.Connection:
    url = os.environ.get("LOCAL_DATABASE_URL")
    if not url:
        raise SystemExit("LOCAL_DATABASE_URL chưa được set - xem .env.example")
    return psycopg.connect(url, row_factory=dict_row)


def sync_schema(prod: psycopg.Connection, local: psycopg.Connection) -> None:
    for table, _ in TABLES:
        cols = prod.execute(
            "SELECT column_name, data_type, udt_name, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
            (table,),
        ).fetchall()
        if not cols:
            print(f"  - {table}: không có ở prod, bỏ qua")
            continue

        exists = local.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table,)
        ).fetchone()
        if not exists:
            body = ",\n  ".join(_column_sql(c) for c in cols)
            local.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (\n  {body}\n)')
            print(f"  + tạo bảng {table} ({len(cols)} cột)")
        else:
            local_cols = {
                r["column_name"]
                for r in local.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                    (table,),
                ).fetchall()
            }
            for c in cols:
                if c["column_name"] in local_cols:
                    continue
                local.execute(f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS {_column_sql(c)}')
                print(f"  + thêm cột {table}.{c['column_name']}")

        # Bảng không có cột id/khoá text mà copy cần -> tự tạo khoá cho bảng text-key.
        keys = [k for k in dict(TABLES)[table] if k in {c["column_name"] for c in cols}]
        if keys:
            local.execute(
                f'DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '
                f'\'{table}_pkey\') THEN ALTER TABLE "{table}" ADD PRIMARY KEY ({", ".join(keys)}); '
                f"END IF; END $$;"
            )
    local.commit()


def copy_data(prod: psycopg.Connection, local: psycopg.Connection) -> None:
    for table, keys in TABLES:
        pk = ", ".join(keys)
        cols = [
            r["column_name"]
            for r in local.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position",
                (table,),
            ).fetchall()
        ]
        if not cols:
            continue
        rows = prod.execute(f'SELECT * FROM "{table}"').fetchall()
        if not rows:
            print(f"  - {table}: prod rỗng")
            continue
        cols = [c for c in cols if c in rows[0]]
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in keys)
        conflict = f'ON CONFLICT ({pk}) DO UPDATE SET {updates}' if updates else f'ON CONFLICT ({pk}) DO NOTHING'
        sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) {conflict}'
        with local.cursor() as cur:
            cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
        print(f"  + {table}: {len(rows)} dòng")
    local.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--schema-only", action="store_true", help="chỉ tạo bảng/cột, không copy dữ liệu")
    ap.add_argument("--no-copy", action="store_true", help="chỉ copy dữ liệu, không đụng schema")
    args = ap.parse_args()

    with prod_conn() as prod, local_conn() as local:
        if not args.no_copy:
            print("[1/2] Đồng bộ schema -> local")
            sync_schema(prod, local)
        if not args.schema_only:
            print("[2/2] Copy dữ liệu prod -> local")
            copy_data(prod, local)
    print("Xong. Để spider-hub dùng DB local, .env cần có APP_ENV=development")


if __name__ == "__main__":
    main()
