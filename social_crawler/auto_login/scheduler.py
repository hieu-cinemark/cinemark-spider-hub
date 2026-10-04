"""Bộ lập lịch auto-login mỗi giờ.

CHỈ CHẠY KHI BẬT CÓ CHỦ ĐÍCH. Không có vòng lặp nền nào khởi động trừ khi môi trường
có AUTO_LOGIN_ENABLED=true. Đây là chủ đích - một tài khoản đã bị gắn cờ "suspected
automated behavior" (2026-09-11) do đăng nhập tự động không ghim proxy; giờ mọi lần
đăng nhập ở đây đều đi qua proxy đã ghim của chính tài khoản
(pool.pinned_login_proxy), nhưng cả một lượt đăng nhập bằng thông tin đăng nhập vẫn là
thứ người vận hành nên bật có cân nhắc. Muốn bật thì phải:

    export AUTO_LOGIN_ENABLED=true
    python -m social_crawler.auto_login.scheduler

sau khi đã xác nhận pool proxy, dấu vân tay trình duyệt, và ít nhất một lần chạy tay
`bootstrap.py --show-browser --manual` đều ổn. AUTO_LOGIN_KILL_SWITCH=true từ chối mọi
lần đăng nhập tự động (bộ lập lịch này, Kafka consumer, bootstrap.py) bất kể thế nào.

Chạy một lần:

    python -m social_crawler.auto_login.scheduler --once [--dry-run | --no-dry-run] [--force]

--once tôn trọng AUTO_LOGIN_DRY_RUN trừ khi có --dry-run/--no-dry-run. --force chạy kể
cả khi không có AUTO_LOGIN_ENABLED, và khi đó mặc định là dry run - truyền
--no-dry-run để đăng nhập thật.

Định dạng cấu hình:

    AUTO_LOGIN_ENABLED=true                # cổng bật/tắt
    AUTO_LOGIN_INTERVAL_SECONDS=3600       # mặc định 1 giờ
    AUTO_LOGIN_PLATFORMS=facebook,threads  # mặc định cả hai
    AUTO_LOGIN_DRY_RUN=false               # mặc định false; người vận
                                           # hành nên bắt đầu bằng true
                                           # ở lượt đầu để xác nhận kế
                                           # hoạch chạy trước khi có lần
                                           # đăng nhập thật nào.

Mỗi lượt:

  1. Đọc các biến env ở trên (đọc lại mỗi lượt để người vận hành có thể đổi chúng kiểu
     SIGHUP `kill -HUP $pid` -> chỉ cần restart, không nạp lại trong tiến trình).
  2. Gọi list_accounts_needing_relogin cho từng nền tảng đang bật.
  3. Duyệt tài khoản tuần tự (KHÔNG song song - mỗi IP một bot tại một thời điểm, cùng
     nhịp mà scripts/relogin_facebook_accounts.py đang dùng).
  4. Ghi kết quả vào last_relogin_*, và nếu status == "needs_human" thì lật
     needs_manual_login=true.
  5. Log bản tổng kết mỗi lượt vào log của nền tảng + cảnh báo Telegram nếu BẤT KỲ tài
     khoản nào rơi vào needs_human HOẶC account_count_needing_manual của cả nền tảng
     tăng lên.

Bộ lập lịch không làm gì khi AUTO_LOGIN_ENABLED chưa đặt, là false, hoặc là chuỗi rỗng
- mọi giá trị khác (true / 1 / yes, không phân biệt hoa thường) đều bật nó.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

from social_crawler.auto_login.orchestrator import attempt_auto_login
from social_crawler.db.relogin import account_count_needing_manual, list_accounts_needing_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_INTERVAL_SECONDS = 3600
_DEFAULT_PLATFORMS = ("facebook", "threads")

_TRUTHY = {"true", "1", "yes", "y", "on"}


def _is_enabled() -> bool:
    return (os.environ.get("AUTO_LOGIN_ENABLED") or "").strip().lower() in _TRUTHY


def _interval_seconds() -> int:
    raw = os.environ.get("AUTO_LOGIN_INTERVAL_SECONDS") or ""
    try:
        return max(60, int(raw)) if raw else _DEFAULT_INTERVAL_SECONDS
    except ValueError:
        return _DEFAULT_INTERVAL_SECONDS


def _platforms() -> tuple[str, ...]:
    raw = os.environ.get("AUTO_LOGIN_PLATFORMS") or ""
    if not raw.strip():
        return _DEFAULT_PLATFORMS
    out = tuple(p.strip().lower() for p in raw.split(",") if p.strip())
    return out or _DEFAULT_PLATFORMS


def _dry_run() -> bool:
    raw = (os.environ.get("AUTO_LOGIN_DRY_RUN") or "").strip().lower()
    if not raw:
        return False
    return raw in _TRUTHY


def _run_one_tick(dry_run: bool | None = None) -> dict[str, Any]:
    """Một lượt của bộ lập lịch. Trả về một dict tổng kết nhỏ để chỗ gọi (lớp bọc CLI / test)
    kiểm tra; không in gì ở đây - mọi output đều là dòng log có cấu trúc. dry_run=None
    nghĩa là theo AUTO_LOGIN_DRY_RUN."""
    summary: dict[str, Any] = {"platforms": {}, "dry_run": _dry_run() if dry_run is None else dry_run}
    for platform in _platforms():
        rows = list_accounts_needing_relogin(platform)
        logger.info(
            "auto_login_tick_start",
            platform=platform,
            candidate_count=len(rows),
            dry_run=summary["dry_run"],
        )
        per_platform: dict[str, int] = {}
        for row in rows:
            outcome = attempt_auto_login(platform, row, dry_run=summary["dry_run"])
            per_platform[outcome.status] = per_platform.get(outcome.status, 0) + 1
            # Giãn cách giữa các tài khoản - cùng khoảng nghỉ tối thiểu mà các script độc lập dùng
            # (4-10s cho FB). Làm ở đây (thay vì bên trong attempt_auto_login) để một lượt dry-run
            # không bị chậm vì giãn cách.
            if not summary["dry_run"]:
                time.sleep(7)
        pending_manual = account_count_needing_manual(platform)
        summary["platforms"][platform] = {
            "attempted": len(rows),
            "by_status": per_platform,
            "needs_manual_total": pending_manual,
        }
        logger.info(
            "auto_login_tick_end",
            platform=platform,
            **summary["platforms"][platform],
        )
    return summary


def run_forever() -> None:
    """Vòng lặp chặn: chạy lượt, ngủ AUTO_LOGIN_INTERVAL_SECONDS, chạy lượt. Bắt mọi exception
    bên trong lượt để một dòng lỗi không bao giờ giết tiến trình lập lịch - lượt sau vẫn
    chạy.

    Muốn deploy kiểu daemon thì bọc nó trong một trình giám sát tiến trình (systemd /
    supervisord). Bản thân bộ lập lịch KHÔNG tự chạy thành daemon.
    """
    if not _is_enabled():
        print(
            "AUTO_LOGIN_ENABLED is not set to a truthy value. Refusing to start.\n"
            "Set AUTO_LOGIN_ENABLED=true (and ideally AUTO_LOGIN_DRY_RUN=true for the first tick) "
            "to enable.",
            file=sys.stderr,
        )
        sys.exit(2)

    interval = _interval_seconds()
    logger.info(
        "auto_login_scheduler_started", interval_seconds=interval, platforms=list(_platforms()), dry_run=_dry_run()
    )
    while True:
        try:
            _run_one_tick()
        except Exception:
            logger.exception("auto_login_tick_crashed")
        time.sleep(interval)


# Helper cho test/dry-run: chạy đúng một lượt rồi thoát, bất kể AUTO_LOGIN_ENABLED có đặt
# hay không. Hữu ích cho "cho tôi xem chuyện gì sẽ xảy ra ngay bây giờ" mà không phải lật
# biến env.
def run_once(*, force: bool = False, dry_run: bool | None = None) -> dict[str, Any]:
    """dry_run=None theo AUTO_LOGIN_DRY_RUN - trừ khi chính force là thứ cho lượt chạy này
    vượt qua AUTO_LOGIN_ENABLED đang tắt, khi đó mặc định là dry run; chỉ khi truyền rõ
    dry_run=False thì mới đăng nhập thật."""
    enabled = _is_enabled()
    if not force and not enabled:
        print(
            "run_once(force=False) requires AUTO_LOGIN_ENABLED=true. "
            "Pass force=True to override (one-shot, dry run unless dry_run=False).",
            file=sys.stderr,
        )
        sys.exit(2)
    if dry_run is None and not enabled:
        dry_run = True
    return _run_one_tick(dry_run)


if __name__ == "__main__":
    # Điểm vào CLI: `python -m social_crawler.auto_login.scheduler` chạy mãi; `... --once
    # [--dry-run | --no-dry-run] [--force]` chạy một lượt. Dạng --once là thứ người vận hành
    # dùng trong lần deploy đầu để xác nhận danh sách ứng viên trông đúng. Không có cờ
    # dry-run nào -> None, nên AUTO_LOGIN_DRY_RUN=true được tôn trọng.
    if "--once" in sys.argv:
        force = "--force" in sys.argv
        if "--dry-run" in sys.argv:
            dry: bool | None = True
        elif "--no-dry-run" in sys.argv:
            dry = False
        else:
            dry = None
        run_once(force=force, dry_run=dry)
    else:
        run_forever()
