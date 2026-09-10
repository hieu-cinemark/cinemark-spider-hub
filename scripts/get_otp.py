"""Manual helper: prints the current TOTP code for one platform_accounts row.
Usage: python -m scripts.get_otp <row_id>
Run from the spider-hub repo root with the same venv/.env as bootstrap.py."""

from __future__ import annotations

import sys

import pyotp

from social_crawler.services.db import _connect


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python -m scripts.get_otp <row_id>")
        sys.exit(1)
    row_id = int(sys.argv[1])

    with _connect() as conn:
        row = conn.execute(
            "SELECT platform, account_id, totp_secret, enabled FROM platform_accounts WHERE id = %s",
            (row_id,),
        ).fetchone()

    if row is None:
        print(f"No platform_accounts row with id={row_id}")
        sys.exit(1)

    print(f"platform: {row['platform']}")
    print(f"account_id: {row['account_id']}")
    print(f"enabled: {row['enabled']}")

    secret = row["totp_secret"]
    if not secret:
        print("This row has no totp_secret set - no OTP to generate.")
        sys.exit(1)

    secret = secret.split("|", 1)[0]
    print(f"OTP: {pyotp.TOTP(secret).now()}")


if __name__ == "__main__":
    main()
