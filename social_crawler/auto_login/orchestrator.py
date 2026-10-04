"""Orchestrator auto-login cho tài khoản Facebook + threads.

Điều khiển một lượt đăng nhập lại cho mỗi tài khoản, chọn đúng cách xử lý 2FA (TOTP
nếu dòng có totp_secret, email IMAP nếu không có nhưng có email + email_password, còn
lại thì cứ thử đăng nhập - phần lớn tài khoản không bao giờ bị hỏi 2FA). Phần điền form
đăng nhập / ghi cookie thật vẫn nằm trong `spiders/<platform>/auth/triggers.py` của
nền tảng + `<platform>.py` (relogin_one) theo từng nền tảng của package này -
orchestrator chỉ ghép chúng lại với một chỗ gọi biết xử lý 2FA.

AN TOÀN: mọi lần đăng nhập ở đây đều đi qua pool.pinned_login_proxy (proxy đã ghim cố
định của chính tài khoản, không bao giờ không proxy - xem facebook.py:relogin_one),
cùng chính sách mà auto-login của bootstrap.py dùng, vì một tài khoản đã bị gắn cờ
"suspected automated behavior" (2026-09-11) do đăng nhập tự động không ghim proxy. Các
chỗ gọi còn phải bật có chủ đích: bộ lập lịch theo env chỉ chạy khi
AUTO_LOGIN_ENABLED=true (xem scheduler.py), Kafka consumer chỉ làm theo những gì
dashboard publish, và AUTO_LOGIN_KILL_SWITCH=true từ chối mọi lần đăng nhập tự động
cùng lúc.

Bề mặt public:
  attempt_auto_login(platform, account_row, *, dry_run=False) -> Outcome
  dataclass AutoLoginOutcome (status, note, needs_manual_login)

Các trạng thái trả về (giống tuple của relogin_one để chỗ gọi log cùng một dạng):
  - "relogged_in": đã ghi cookie mới, tài khoản hoạt động lại (lần kiểm tra gần nhất
    được ghi "alive", nên nó rời danh sách chết).
  - "needs_human": không tự giải được (không lưu mật khẩu, HOẶC bị hỏi 2FA mà không có
    totp_secret lẫn mã gửi qua email, HOẶC Facebook bắt checkpoint ảnh, HOẶC biến thể
    markup 2FA lạ). needs_manual_login=true được đặt trên dòng để bộ lập lịch bỏ qua nó
    ở các lượt sau cho tới khi có người xoá cờ.
  - "failed": luồng đăng nhập chạy hết nhưng Facebook từ chối thông tin đăng nhập (sai
    mật khẩu / tài khoản bị khoá / checkpoint thật). Ở đây KHÔNG đặt riêng
    mark_needs_manual_login - đường record_account_outcome(hard_failure=True) của nền
    tảng vốn đã lật enabled=false cho các trường hợp này (chỗ gọi chịu trách nhiệm gọi
    hàm đó sau khi ta trả về).
  - "error": lỗi hạ tầng tạm thời (proxy sập, IMAP không truy cập được, trình duyệt
    crash). last_check_status của dòng vẫn là 'dead' để lượt hằng giờ kế tiếp thử lại;
    chỉ dòng audit được ghi.
  - "skipped_dry_run": dry_run=True, không thử gì và không ghi gì vào dòng.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from social_crawler.auto_login.email_2fa import fetch_email_2fa_code
from social_crawler.db.accounts import account_dict
from social_crawler.db.relogin import mark_needs_manual_login, stamp_last_relogin
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Re-export các literal trạng thái cho chỗ gọi không muốn import chính kiểu Literal.
Status = Literal["relogged_in", "needs_human", "failed", "error", "skipped_dry_run"]

CodeProvider = Callable[[], str | None]

# Khoảng dư cho lệch đồng hồ giữa máy này và INTERNALDATE của mail server khi chỉ chấp
# nhận mã được gửi sau lúc bắt đầu thử.
_EMAIL_CLOCK_SKEW_SECONDS = 60


@dataclass
class AutoLoginOutcome:
    status: Status
    note: str | None = None
    needs_manual_login: bool = False


def _run_facebook_relogin(account_row: dict[str, Any], code_provider: CodeProvider | None) -> AutoLoginOutcome:
    """Lớp bọc mỏng quanh relogin_one của facebook.py - cùng luồng đăng nhập một tài khoản mà
    scripts/relogin_facebook_accounts.py chạy, để chỉ có một phiên bản "cách đăng nhập
    Facebook" thay vì hai bản lệch dần nhau.

    account_row là một dòng platform_accounts thô (bộ cột của db/relogin.py: `id` dạng số,
    định danh đăng nhập trong `account_id`, `totp_secret`) - được chuyển sang dạng
    db.accounts.Account mà relogin_one/auto_login đọc (định danh đăng nhập trong `id`,
    secret TOTP trong `2fa`) trước khi gọi.

    Dùng API Playwright đồng bộ, vốn từ chối khởi động trên thread đang có asyncio loop
    chạy - Kafka consumer gọi attempt_auto_login qua asyncio.to_thread chính vì lý do đó.

    Trả về AutoLoginOutcome thay vì tuple để chỗ gọi không phải nhớ thứ tự vị trí.
    "needs_human" ánh xạ thành needs_manual_login=True; mọi trường hợp khác là False.
    """
    # Import lười để đường email-2fa không phải kéo Playwright vào chỉ để đọc một mã từ IMAP.
    from patchright.sync_api import sync_playwright

    from social_crawler.auto_login.facebook import relogin_one
    from social_crawler.clients.redis import RedisCache

    with sync_playwright() as pw:
        status, note = relogin_one(pw, RedisCache(), account_dict(account_row), code_provider=code_provider)

    return AutoLoginOutcome(
        status=status,  # type: ignore[arg-type]
        note=note,
        needs_manual_login=status == "needs_human",
    )


def _run_threads_relogin(account_row: dict[str, Any], code_provider: CodeProvider | None) -> AutoLoginOutcome:
    """Bản tương ứng của _run_facebook_relogin cho threads. scripts/relogin_threads_accounts.py
    chưa tồn tại (mới chỉ có bản FB - bản threads nằm trong lộ trình Phase 2). Cho tới khi
    có, ta báo rõ ràng là "needs_human" thay vì âm thầm không làm gì, để người vận hành chạy
    bộ lập lịch cho threads thấy ngay lỗ hổng thay vì lượt nào cũng âm thầm thất bại."""
    return AutoLoginOutcome(
        status="needs_human",
        note="threads auto-relogin not implemented yet (scripts/relogin_threads_accounts.py missing)",
        needs_manual_login=False,
    )


def _resolve_2fa(account_row: dict[str, Any], attempt_started_at: float) -> tuple[CodeProvider | None, str]:
    """Trả về (code_provider, strategy_label) cho một tài khoản:
      - (None, "totp") nếu dòng có totp_secret - auto_login tự sinh mã từ đó
      - (email provider, "email_imap") nếu không, khi có email + email_password - chỉ nhận
        mã được gửi sau attempt_started_at
      - (None, "none") nếu không cấu hình cái nào - vẫn thử đăng nhập; khi đó nếu bị hỏi
        2FA thì báo needs_human

    Hộp thư không truy cập được sẽ raise Email2FAUnreachableError ra từ provider, và
    relogin_one báo đó là "error" tạm thời (thử lại ở lượt sau) thay vì gắn cờ cần người
    xử lý cho tài khoản. Đặt phần chọn lựa ở đây (thay vì viết thẳng trong
    attempt_auto_login) giúp quyết định "tài khoản này dùng kênh 2FA nào" unit test được mà
    không cần dựng Playwright.
    """
    if (account_row.get("totp_secret") or "").strip():
        return None, "totp"

    if (account_row.get("email") or "").strip() and (account_row.get("email_password") or ""):

        def _email_code() -> str | None:
            return fetch_email_2fa_code(account_row, since_unix=attempt_started_at - _EMAIL_CLOCK_SKEW_SECONDS)

        return _email_code, "email_imap"

    return None, "none"


def attempt_auto_login(
    platform: str,
    account_row: dict[str, Any],
    *,
    dry_run: bool = False,
) -> AutoLoginOutcome:
    """Một lần thử đăng nhập lại cho một dòng tài khoản. Xem docstring module về ý nghĩa các
    trạng thái.

    `account_row` phải là dict có dạng như db/relogin.list_accounts_needing_relogin trả về
    (hoặc tương thích) - cụ thể phải có `id` (khoá chính dạng số) và tối thiểu các cột
    account_id / password / totp_secret / cookie / token / email / email_password. Provider
    mã 2FA được xác định bên trong; phần điền form đăng nhập + ghi cookie thật vẫn nằm
    trong luồng đăng nhập lại riêng của nền tảng.
    """
    row_id = account_row.get("id")
    account_label = account_row.get("account_id") or row_id or "?"
    logger.info("auto_login_attempt_start", platform=platform, account_id=account_label, dry_run=dry_run)

    if dry_run:
        # Không thử gì nên không ghi gì - một lượt xem trước không được ghi đè các cột audit
        # last_relogin_* của lần thử thật gần nhất.
        return AutoLoginOutcome(status="skipped_dry_run", note="dry-run mode - no login attempted")

    code_provider, strategy = _resolve_2fa(account_row, time.time())

    # Chuyển sang runner riêng của nền tảng. Nền tảng mới cắm vào bằng cách thêm một nhánh
    # elif ở đây.
    runner: Callable[[dict[str, Any], CodeProvider | None], AutoLoginOutcome] | None
    if platform == "facebook":
        runner = _run_facebook_relogin
    elif platform == "threads":
        runner = _run_threads_relogin
    else:
        runner = None

    if runner is None:
        outcome = AutoLoginOutcome(
            status="needs_human",
            note=f"unsupported platform {platform!r} - only facebook and threads are wired in",
            needs_manual_login=True,
        )
    else:
        try:
            outcome = runner(account_row, code_provider)
        except Exception as exc:  # bắt-tất-cả cuối cùng để bug của runner không giết lượt của bộ lập lịch
            logger.exception("auto_login_runner_crashed", platform=platform, account_id=account_label)
            outcome = AutoLoginOutcome(status="error", note=f"runner crashed: {exc}")

    if row_id is not None:
        stamp_last_relogin(int(row_id), status=outcome.status)
        if outcome.needs_manual_login:
            mark_needs_manual_login(int(row_id), outcome.note or "")

    logger.info(
        "auto_login_attempt_end",
        platform=platform,
        account_id=account_label,
        status=outcome.status,
        strategy=strategy,
        note=outcome.note,
    )
    return outcome
