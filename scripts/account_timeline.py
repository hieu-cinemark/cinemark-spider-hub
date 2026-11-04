"""In dòng thời gian của một tài khoản: phiên, client, IP thoát, tín hiệu từ Facebook, nghỉ, tắt. Không gửi gì lên Facebook.

.venv/bin/python -m scripts.account_timeline <UID>
.venv/bin/python -m scripts.account_timeline ten@mail.com --limit 50
"""

from __future__ import annotations

import argparse
import json

from social_crawler.core import env  # noqa: F401 - nạp .env
from social_crawler.db.account_events import timeline
from social_crawler.db.connection import connect


def account_keys(platform: str, key: str) -> list[str]:
    """Sự kiện ghi theo email hoặc account_id; dòng đã xoá thì chỉ còn key được truyền vào."""
    with connect() as conn:
        row = conn.execute(
            "SELECT account_id, email FROM platform_accounts WHERE platform = %s "
            "AND (lower(account_id) = lower(%s) OR lower(coalesce(email, '')) = lower(%s))",
            (platform, key, key),
        ).fetchone()
    return [key, *([row["account_id"], row["email"]] if row else [])]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("account")
    parser.add_argument("--platform", default="facebook")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)
    rows = timeline(args.platform, account_keys(args.platform, args.account), limit=args.limit)
    if not rows:
        print("Chưa có sự kiện nào cho tài khoản này.")
    for row in rows:
        print(f"{row['at']:%Y-%m-%d %H:%M:%S%z}  {row['event']:<28} {json.dumps(row['details'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
