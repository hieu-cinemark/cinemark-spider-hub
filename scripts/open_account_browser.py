"""Mở Chrome thật cho từng tài khoản qua một proxy của nền tảng để người vận hành đăng nhập/cập nhật phiên.
Không tự thao tác gì trên trang.

Không còn ghim proxy (13/11/2026; từ 18/11/2026 proxy tự đổi IP mỗi 60 giây): script lấy một proxy đang trống trong danh sách của
nền tảng và GIỮ CHỖ endpoint đó cho tài khoản (runtime/proxy_lease.py, cùng khoá với job cào) suốt lúc Chrome mở - job cào của
tài khoản khác không đi qua cùng cổng. Nhấn Ctrl+C ở terminal này khi đăng nhập xong để nhả chỗ.

    .venv/bin/python -m scripts.open_account_browser              # mọi tài khoản trong DEFAULT_ACCOUNTS
    .venv/bin/python -m scripts.open_account_browser 126 6        # theo id dòng platform_accounts
    .venv/bin/python -m scripts.open_account_browser --dry-run
    .venv/bin/python -m scripts.open_account_browser --show-proxy-auth
    .venv/bin/python -m scripts.open_account_browser 129 --proxy-port 8159 --login

Mỗi tài khoản một hồ sơ riêng trong ~/fb-profiles. Hai tài khoản không bao giờ mở qua cùng một cổng proxy cùng lúc.
"""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

from curl_cffi import requests as cr

from social_crawler.clients.redis import RedisCache
from social_crawler.core import env  # noqa: F401 - nạp .env
from social_crawler.db.proxies import _proxy_row
from social_crawler.db.script_connect import connect
from social_crawler.runtime import proxy_lease
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


def load_accounts(ids: list[int]) -> list[dict]:
    with connect() as conn:
        return conn.execute(
            "SELECT id, platform, account_id, email, enabled, status FROM platform_accounts "
            "WHERE id = ANY(%s) ORDER BY platform, id",
            (ids,),
        ).fetchall()


def lease_owner(row: dict) -> str:
    """Khoá giữ chỗ proxy của tài khoản - đúng khoá job cào dùng (email, không có thì id tài khoản trên nền tảng)."""
    return str(row.get("email") or row["account_id"]).strip().lower()


def proxy_by_port(platform: str, port: int) -> dict:
    """Ghi đè: dùng đúng proxy có cổng này trong danh sách của nền tảng."""
    if not 1 <= port <= 65535:
        raise ValueError("Cổng proxy phải nằm trong khoảng 1–65535")
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM platform_proxies WHERE enabled = true AND platform = %s AND proxy_url LIKE %s",
            (platform, f"%:{port}"),
        ).fetchall()
    if len(rows) != 1:
        raise ValueError(f"Cổng {port}: cần đúng một proxy {platform} đang bật")
    return _proxy_row(rows[0])


def claim_proxy_for(row: dict, port: int | None, cache: RedisCache) -> dict | None:
    """Proxy cho lần mở này, đã giữ chỗ endpoint cho tài khoản; None khi mọi proxy của nền tảng đang bận / cooldown."""
    owner = lease_owner(row)
    if port is None:
        return proxy_lease.claim(row["platform"], owner, cache)
    proxy = proxy_by_port(row["platform"], port)
    key = proxy_lease.LEASE_KEY_TMPL.format(endpoint=proxy_lease.endpoint(proxy["url"]))
    holder = cache.get(key)
    if holder not in (None, owner) or (holder is None and not cache.set_nx(key, owner, proxy_lease.LEASE_SECONDS)):
        raise ValueError(f"Cổng {port} đang được tài khoản khác dùng")
    return proxy


def hold_until_interrupted(held: list[tuple[dict, str]], cache: RedisCache) -> None:
    """Gia hạn chỗ giữ mỗi phút tới khi người vận hành nhấn Ctrl+C, rồi nhả."""
    print("\nĐang giữ chỗ proxy cho các tài khoản trên. Đăng nhập xong thì nhấn Ctrl+C ở đây để nhả.")
    try:
        while True:
            time.sleep(60)
            for proxy, owner in held:
                proxy_lease.touch(proxy, owner, cache)
    except KeyboardInterrupt:
        pass
    finally:
        for proxy, owner in held:
            proxy_lease.release(proxy, owner, cache)
        print("Đã nhả chỗ giữ proxy.")


def main(argv: list[str] | None = None) -> int:
    """Điểm vào dòng lệnh: chọn tài khoản (theo id hoặc `DEFAULT_ACCOUNTS`), giữ chỗ một proxy đang trống của nền tảng (hoặc
    `--proxy-port`), đo IP và mở Chrome thật với hồ sơ riêng trong ~/fb-profiles. `--dry-run` chỉ in kế hoạch (không giữ chỗ),
    `--login` mở thêm tab đăng nhập; trả mã thoát 0 nếu mở xong, khác 0 nếu tham số/proxy không hợp lệ."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ids", nargs="*", type=int, help="id dòng platform_accounts")
    parser.add_argument(
        "--dry-run", action="store_true", help="chỉ đo IP và in kế hoạch, không mở Chrome, không giữ chỗ"
    )
    parser.add_argument("--show-proxy-auth", action="store_true", help="in user/mật khẩu proxy ra terminal này")
    parser.add_argument("--proxy-port", type=int, help="dùng đúng proxy có cổng này của nền tảng (một tài khoản)")
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

    cache = RedisCache()
    held: list[tuple[dict, str]] = []
    running = running_chrome_commands()
    for row in rows:
        label = f"{row['id']} {row['platform']} {row['account_id']}"
        if row["platform"] not in HOME_URL:
            print(f"BỎ QUA {label}: chưa hỗ trợ nền tảng này")
            continue
        try:
            proxy = claim_proxy_for(row, args.proxy_port, cache)
        except ValueError as exc:
            parser.error(str(exc))
        if proxy is None:
            print(f"BỎ QUA {label}: mọi proxy {row['platform']} đang bận hoặc cooldown")
            continue
        owner = lease_owner(row)
        if args.dry_run:
            proxy_lease.release(proxy, owner, cache)  # chỉ xem kế hoạch: không giữ chỗ
        profile = profile_dir(row["platform"], row["id"])
        reason = busy_reason(profile, proxy["url"], running)
        if reason:
            print(f"BỎ QUA {label}: {reason}")
            proxy_lease.release(proxy, owner, cache)
            continue
        ip = exit_ip({"proxy_url": proxy["url"], "username": proxy["username"], "password": proxy["password"]})
        if not ip:
            print(f"BỎ QUA {label}: proxy #{proxy['id']} không kết nối được")
            proxy_lease.release(proxy, owner, cache)
            continue
        print(
            f"{label}: proxy #{proxy['id']}, IP đo được {ip}; tab đầu kiểm tra IP, rồi vào {HOME_URL[row['platform']]}"
        )
        print("    proxy tự đổi IP mỗi 60 giây - IP ở tab đầu sẽ khác dần, đó là bình thường")
        print(f"    mã 2FA: .venv/bin/python -m scripts.get_otp {row['id']}")
        if proxy.get("username"):
            print("    Chrome hỏi xác thực proxy: dùng --show-proxy-auth để xem thông tin trong terminal riêng")
        if args.show_proxy_auth:
            print(f"    proxy user={proxy['username']} pass={proxy['password']}")
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
                proxy["url"],
                "https://api.ipify.org",
                login_url=login_url,
                chrome_app=chrome_app_for(row["platform"], args.chrome_app),
            ),
            check=True,
        )
        running.append(f"--user-data-dir={profile} --proxy-server=http://{proxy['url']}")
        held.append((proxy, owner))
    if held:
        hold_until_interrupted(held, cache)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
