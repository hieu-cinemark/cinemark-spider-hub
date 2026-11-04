"""Mở Chrome thật cho từng tài khoản qua đúng proxy đã gán, để người vận hành tự nuôi bằng tay. Không tự thao tác gì trên trang.

    .venv/bin/python -m scripts.open_account_browser              # mọi tài khoản trong DEFAULT_ACCOUNTS
    .venv/bin/python -m scripts.open_account_browser 126 6        # theo id dòng platform_accounts
    .venv/bin/python -m scripts.open_account_browser --dry-run
    .venv/bin/python -m scripts.open_account_browser --show-proxy-auth
    .venv/bin/python -m scripts.open_account_browser 129 --proxy-port 8159 --login

Mỗi tài khoản một hồ sơ riêng trong ~/fb-profiles. Hai tài khoản cùng proxy không mở cùng lúc: chạy lại sau khi đóng cái trước.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from curl_cffi import requests as cr

from social_crawler.core import env  # noqa: F401 - nạp .env
from social_crawler.db.connection import connect
from social_crawler.runtime.pool import build_proxy_url

DEFAULT_ACCOUNTS = (63, 126, 127, 6, 55, 62)
HOME_URL = {"facebook": "https://www.facebook.com/", "threads": "https://www.threads.com/"}
PROFILE_ROOT = Path.home() / "fb-profiles"
# Phiên Facebook phải tạo trên bản curl_cffi giả được (hiện tới 152), nếu không crawl sẽ từ chối vì TLS lệch UA.
FACEBOOK_CHROME_APP = (
    PROFILE_ROOT / "tools/chrome/chrome/mac_arm-152.0.7977.82/chrome-mac-arm64/Google Chrome for Testing.app"
)


def profile_dir(platform: str, account_id: int) -> Path:
    # Hồ sơ Facebook đã có sẵn theo id; Threads thêm tiền tố cho dễ nhận.
    return PROFILE_ROOT / (str(account_id) if platform == "facebook" else f"{platform}-{account_id}")


def chrome_args(
    profile: Path, proxy_url: str, check_url: str, *, login_url: str | None = None, chrome_app: str = "Google Chrome"
) -> list[str]:
    args = [
        "open",
        "-na",
        chrome_app,
        "--args",
        f"--user-data-dir={profile}",
        f"--proxy-server=http://{proxy_url}",
        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
        "--no-first-run",
        check_url,
    ]
    if login_url:
        args.append(login_url)
    return args


def exit_ip(row: dict) -> str | None:
    url = build_proxy_url({"url": row["proxy_url"], "username": row["username"], "password": row["password"]})
    try:
        return cr.get("https://api.ipify.org", proxies={"http": url, "https": url}, timeout=15).text.strip()
    except cr.RequestsError:
        return None


def running_chrome_commands() -> list[str]:
    output = subprocess.run(["ps", "ax", "-o", "command"], capture_output=True, text=True, check=True).stdout
    return [line for line in output.splitlines() if "--user-data-dir=" in line and "--type=" not in line]


def busy_reason(profile: Path, proxy_url: str, running: list[str]) -> str | None:
    for line in running:
        if f"--user-data-dir={profile}" in line:
            return "hồ sơ này đang mở"
        if f"--proxy-server=http://{proxy_url}" in line:
            return "một tài khoản khác đang mở qua cùng proxy, đóng nó trước"
    return None


def chrome_app_for(platform: str, override: str | None) -> str:
    if override:
        return override
    return str(FACEBOOK_CHROME_APP) if platform == "facebook" else "Google Chrome"


def policy_refusal(row: dict) -> str | None:
    """Facebook: hệ thống đang giữ phiên thì không mở thêm Chrome trên đó; proxy tự xoay thì không đăng nhập qua nó."""
    if row["platform"] != "facebook":
        return None
    if row.get("session_client") in ("browser", "http"):
        return f"phiên đang do client {row['session_client']} giữ, đổi về manual trên dashboard trước"
    return "proxy tự xoay IP" if row.get("ip_type") == "rotating" else None


def load_accounts(ids: list[int]) -> list[dict]:
    with connect() as conn:
        return conn.execute(
            "SELECT a.id, a.platform, a.account_id, a.enabled, a.status, a.assigned_proxy_id, a.session_client, "
            "p.ip_type, p.proxy_url, p.username, p.password FROM platform_accounts a "
            "LEFT JOIN platform_proxies p ON p.id = a.assigned_proxy_id WHERE a.id = ANY(%s) ORDER BY a.platform, a.id",
            (ids,),
        ).fetchall()


def load_proxy_for_account(port: int, account_row_id: int) -> dict:
    """Explicit one-off proxy override. Does not pin the account or reset a rotating endpoint."""
    if not 1 <= port <= 65535:
        raise ValueError("Cổng proxy phải nằm trong khoảng 1–65535")
    with connect() as conn:
        rows = conn.execute(
            "SELECT p.id, p.ip_type, p.proxy_url, p.username, p.password FROM platform_proxies p "
            "WHERE p.enabled = true AND p.proxy_url LIKE %s "
            "AND NOT EXISTS (SELECT 1 FROM platform_accounts a JOIN platform_proxies same "
            "ON same.id = a.assigned_proxy_id WHERE same.proxy_url = p.proxy_url AND a.id != %s)",
            (f"%:{port}", account_row_id),
        ).fetchall()
    if len(rows) != 1:
        raise ValueError(f"Cổng {port}: cần đúng một proxy đang bật, không gán cho tài khoản khác")
    return rows[0]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ids", nargs="*", type=int, help="id dòng platform_accounts")
    parser.add_argument("--dry-run", action="store_true", help="chỉ đo IP và in kế hoạch, không mở Chrome")
    parser.add_argument("--show-proxy-auth", action="store_true", help="in user/mật khẩu proxy ra terminal này")
    parser.add_argument("--proxy-port", type=int, help="chọn proxy trong DB theo cổng cho một tài khoản, không sửa DB")
    parser.add_argument("--login", action="store_true", help="mở thêm tab đăng nhập sau tab kiểm tra IP")
    parser.add_argument(
        "--chrome-app",
        help="tên hoặc đường dẫn app Chrome; mặc định Facebook dùng Chrome 152, nền tảng khác dùng Google Chrome",
    )
    args = parser.parse_args(argv)
    if args.proxy_port is not None and len(args.ids) != 1:
        parser.error("--proxy-port chỉ dùng khi chọn đúng một id tài khoản")

    rows = load_accounts(args.ids or list(DEFAULT_ACCOUNTS))
    missing = set(args.ids) - {row["id"] for row in rows}
    if missing:
        parser.error(f"Không tìm thấy id tài khoản: {', '.join(map(str, sorted(missing)))}")
    if args.proxy_port is not None:
        try:
            proxy = load_proxy_for_account(args.proxy_port, args.ids[0])
        except ValueError as exc:
            parser.error(str(exc))
        rows = [
            {
                **row,
                "assigned_proxy_id": proxy["id"],
                **{k: proxy[k] for k in ("ip_type", "proxy_url", "username", "password")},
            }
            for row in rows
        ]

    running = running_chrome_commands()
    for row in rows:
        label = f"{row['id']} {row['platform']} {row['account_id']}"
        if row["platform"] not in HOME_URL:
            print(f"BỎ QUA {label}: chưa hỗ trợ nền tảng này")
            continue
        if not row["assigned_proxy_id"] or not row["proxy_url"]:
            print(f"BỎ QUA {label}: chưa gán proxy")
            continue
        refusal = policy_refusal(row)
        if refusal:
            print(f"BỎ QUA {label}: {refusal}")
            continue
        profile = profile_dir(row["platform"], row["id"])
        reason = busy_reason(profile, row["proxy_url"], running)
        if reason:
            print(f"BỎ QUA {label}: {reason}")
            continue
        ip = exit_ip(row)
        if not ip:
            print(f"BỎ QUA {label}: proxy #{row['assigned_proxy_id']} không kết nối được")
            continue
        print(
            f"{label}: proxy #{row['assigned_proxy_id']}, IP đo được {ip}; tab đầu kiểm tra IP, rồi vào {HOME_URL[row['platform']]}"
        )
        if args.proxy_port is not None:
            print("    proxy được chọn cho lần mở này; không sửa gán proxy trong DB")
        print(f"    mã 2FA: .venv/bin/python -m scripts.get_otp {row['id']}")
        if row.get("username"):
            print("    Chrome hỏi xác thực proxy: dùng --show-proxy-auth để xem thông tin trong terminal riêng")
        if args.show_proxy_auth:
            print(f"    proxy user={row['username']} pass={row['password']}")
        if args.dry_run:
            continue
        login_url = None
        if args.login:
            login_url = (
                "https://www.facebook.com/login/" if row["platform"] == "facebook" else "https://www.threads.com/login/"
            )
        subprocess.run(
            chrome_args(
                profile,
                row["proxy_url"],
                "https://api.ipify.org",
                login_url=login_url,
                chrome_app=chrome_app_for(row["platform"], args.chrome_app),
            ),
            check=True,
        )
        running.append(f"--user-data-dir={profile} --proxy-server=http://{row['proxy_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
