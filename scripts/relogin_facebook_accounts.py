"""Lượt đăng nhập lại tuần tự, chạy một lần, cho các tài khoản facebook có cookie đã cache
bị chết (xem scripts/check_facebook_cookies.py) - mỗi tài khoản đi qua relogin_one
trong social_crawler/auto_login/facebook.py (điền form đăng nhập, giải 2FA TOTP, ghi
cookie mới lại vào dòng đó và cache storage_state trong Redis của nó).

Cố ý chạy tuần tự, từng tài khoản một (không bao giờ song song trong script này) - mỗi
tài khoản đăng nhập qua proxy đã ghim cố định của riêng nó (pool.pinned_login_proxy,
không bao giờ không proxy), nên các lần đăng nhập liên tiếp không bao giờ dùng chung
một IP dồn dập dù chúng chạy nối nhau với khoảng nghỉ.

Script này không tự giải mã xác minh gửi qua email (orchestrator auto-login truyền cho
relogin_one một code_provider để làm việc đó, xem
social_crawler/auto_login/orchestrator.py) - tài khoản nào không có totp_secret mà bị
hiện màn hình 2FA thì chỉ được báo là cần người xử lý, không bao giờ bị tự động tắt
(xem docstring của MissingTotpSecretError để biết vì sao đoán đó là checkpoint là sai).

Cách dùng:
    # Mọi tài khoản facebook được check_facebook_cookies.py ghi gần nhất là "dead"
    python -m scripts.relogin_facebook_accounts

    # Chỉ một số tài khoản cụ thể
    python -m scripts.relogin_facebook_accounts --account 61570510702486 --account someone@example.com
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from patchright.sync_api import sync_playwright

from social_crawler.auto_login.facebook import PLATFORM, relogin_one
from social_crawler.clients.redis import RedisCache
from social_crawler.db.accounts import get_accounts_by_check_status
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_MIN_PAUSE_SECONDS = 20.0
_MAX_PAUSE_SECONDS = 60.0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--account",
        action="append",
        help="account_id/email to re-login (repeatable). Default: every facebook account last checked 'dead'.",
    )
    args = parser.parse_args()

    # Pipe script này ra file/tee (như mọi lượt chạy nền) sẽ chuyển stdout sang chế độ đệm
    # theo khối, nên các dòng tiến độ của `print()` có thể nằm trong bộ đệm nhiều phút mà
    # chưa thực sự ghi gì vào file log - đã xảy ra thật: một lượt chạy đang âm thầm thành
    # công hết tài khoản này tới tài khoản khác, chỉ nhìn file log thì trông giống hệt một
    # lượt kẹt ở ngay tài khoản đầu tiên, và đã bị kill vì chẩn đoán nhầm là "treo". Đệm theo
    # dòng ở đây nghĩa là nội dung trong file log luôn đúng với những gì thực sự đã xảy ra.
    sys.stdout.reconfigure(line_buffering=True)

    # Mạng chậm/chập chờn (VPN đang kết nối lại, chuyển wifi) có thể làm kết nối Supabase đầu
    # tiên kẹt lâu hơn nhiều so với connect_timeout của _connect() - timeout của
    # psycopg/libpq không phải nền tảng nào cũng phủ được bước phân giải DNS bị kẹt. In ra
    # trước lời gọi (không phải sau) nghĩa là khi kẹt sẽ thấy "vẫn ở dòng này" thay vì một
    # script trông như đơ/chết mà không có output nào, như đã xảy ra thực tế - xem lần
    # KeyboardInterrupt khi người dùng của project này đang chờ đúng dòng này mà chưa có gì
    # được in ra.
    print("Connecting to Supabase to find accounts marked 'dead'...")
    if args.account:
        all_dead = get_accounts_by_check_status(PLATFORM, "dead")
        needles = {a.strip().lower() for a in args.account}
        accounts = [a for a in all_dead if a["id"].lower() in needles or (a.get("email") or "").lower() in needles]
        missing = needles - {a["id"].lower() for a in accounts} - {(a.get("email") or "").lower() for a in accounts}
        if missing:
            print(f"Not found among accounts last checked 'dead': {sorted(missing)}")
    else:
        accounts = get_accounts_by_check_status(PLATFORM, "dead")

    if not accounts:
        print("No facebook accounts to re-login (none currently recorded as 'dead').")
        return

    print(f"Re-logging in {len(accounts)} facebook account(s), one at a time...\n")
    redis_cache = RedisCache()
    results: list[tuple[str, str, str | None]] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = relogin_one(pw, redis_cache, account)
            except Exception as exc:
                status, note = "error", str(exc)
                logger.error("relogin_crashed", account=label, error=str(exc))
            # relogin_one đã ghi "alive" khi thành công; "needs_human"/"error" giữ nguyên trạng thái
            # "dead" hiện có - nó vẫn chết cho tới khi có người sửa.
            results.append((label, status, note))
            print(f"  {label:45s} {status:14s} {note or ''}")
            if i < len(accounts) - 1:
                time.sleep(random.uniform(_MIN_PAUSE_SECONDS, _MAX_PAUSE_SECONDS))

    ok = sum(1 for _, s, _ in results if s == "relogged_in")
    needs_human = sum(1 for _, s, _ in results if s == "needs_human")
    other = len(results) - ok - needs_human
    print(f"\n{ok} re-logged in, {needs_human} need a human, {other} error (out of {len(results)}).")
    if needs_human:
        print("These need a real human-supervised login instead:")
        print(
            "  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>"
        )


if __name__ == "__main__":
    main()
