"""Các helper dùng bởi bộ lập lịch / consumer / orchestrator auto-login.

Cố ý tách khỏi db/accounts.py: module đó là nguồn sự thật duy nhất cho CRUD thường ngày
của bảng platform_accounts (get/update/claim/disable), còn luồng auto-login dùng một
phần khác của bảng (các dòng có cookie chết, cộng các dòng cần người giám sát đăng nhập
lại sau checkpoint). Trộn cả hai vào một file trước đây đã làm khó thấy query nào là
"mọi lượt crawl đều cần" và query nào là "chỉ bộ lập lịch đăng nhập lại cần".

Mọi hàm chủ yếu là đọc - chỉ ghi vào `last_check_*`, `needs_manual_login` và
`last_relogin_at`, cùng các cột kiểm tra tay mà check_facebook_cookies.py vốn điền. Các
cột cookie/totp/password thật thì mọi helper ở đây đều để nguyên; đó là việc riêng của
auto_login/facebook.py / bootstrap.py.
"""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# Những dòng mà luồng auto-login được phép đụng tới - dùng chung cho query danh sách của
# bộ lập lịch và lần tra theo từng message của consumer, để một tài khoản đã bị tắt, bị
# checkpoint, bị gắn cờ cần người xử lý, hoặc đã được đánh dấu sống sau khi một lượt
# publish nó, không bao giờ bị đăng nhập vì một message cũ.
_RELOGIN_ELIGIBLE = (
    "platform = %s AND enabled = true AND status != 'checkpoint' "
    "AND last_check_status = 'dead' AND needs_manual_login = false"
)
_RELOGIN_COLUMNS = "id, account_id, password, totp_secret, cookie, token, email, email_password"


def list_accounts_needing_relogin(platform: str) -> list[dict[str, Any]]:
    """Các tài khoản đang bật, không bị checkpoint, có lần kiểm tra cookie còn sống gần nhất
    trả về `dead` (xem scripts/check_facebook_cookies.py / check_threads_cookies.py) VÀ chưa
    bị gắn cờ cần người can thiệp. Bộ lập lịch auto-login duyệt danh sách này mỗi lượt.

    Cố ý tính cả dòng đang cooldown: một lỗi nhẹ trước đó không nên chặn việc thử đăng nhập
    lại ở lượt hằng giờ kế tiếp, vì "cookie chết" là lỗi nặng cần đăng nhập lại hoàn toàn
    bất kể backoff tạm thời nào - bản thân lần đăng nhập lại thành công hay thất bại theo
    đúng thực tế của nó (record_account_outcome xử lý trạng thái sau đó).
    """
    try:
        with connect() as conn:
            rows = conn.execute(
                f"SELECT {_RELOGIN_COLUMNS} FROM platform_accounts WHERE {_RELOGIN_ELIGIBLE} "
                "ORDER BY last_checked_at ASC NULLS LAST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_accounts_needing_relogin_failed", platform=platform, error=str(exc))
        return []
    return list(rows)


def mark_needs_manual_login(account_row_id: int, reason: str) -> None:
    """Lật needs_manual_login=true trên một dòng platform_accounts, kèm text lý do ngắn trong
    last_check_note để cột "cần chú ý" trên dashboard hiển thị thẳng. Idempotent (gọi lần
    hai chỉ ghi đè ghi chú).

    Được bộ lập lịch auto-login gọi khi một lần thử đăng nhập lại gặp thứ chỉ người mới xử
    lý được (checkpoint ảnh của Facebook, 2FA qua email mà không có email_password, markup
    màn hình 2FA lạ, ...). Dòng bị loại khỏi list_accounts_needing_relogin khi cờ đã đặt,
    nên bộ lập lịch không thử lại cùng ngõ cụt mỗi giờ; người dùng gỡ cờ qua dashboard sau
    khi đã đăng nhập tay.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET needs_manual_login = true, "
                "last_check_note = %s, last_checked_at = now() WHERE id = %s",
                (reason, account_row_id),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_needs_manual_login_failed", account_row_id=account_row_id, error=str(exc))


def clear_needs_manual_login(account_row_id: int) -> None:
    """Gỡ cờ "cần người can thiệp" trên dashboard sau khi người đã xử lý xong (hoặc sau khi một
    lần auto-login thành công chứng minh lỗi trước đó chỉ là tạm thời, ví dụ Facebook thoáng
    dính checkpoint rồi tự hồi phục). Cùng hợp đồng idempotent như mark_needs_manual_login.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET needs_manual_login = false, last_check_note = NULL WHERE id = %s",
                (account_row_id,),
            )
    except psycopg.Error as exc:
        logger.error("db_clear_needs_manual_login_failed", account_row_id=account_row_id, error=str(exc))


def stamp_last_relogin(account_row_id: int, *, status: str) -> None:
    """Vết audit cho bộ lập lịch auto-login: last_relogin_at + last_relogin_status, được
    dashboard hiển thị cạnh last_check_* để người vận hành biết "tài khoản này được thử lần
    cuối khi nào, và kết quả ra sao". `status` là một trong: "relogged_in" | "needs_human" |
    "failed" | "error" - khớp với tuple mà relogin_one() của auto_login/facebook.py vốn trả
    về. Không bao giờ được gọi cho dry run, để một lượt xem trước không ghi đè trạng thái của
    lần thử thật gần nhất.

    KHÔNG đụng tới cookie/totp/enabled/disabled - luồng đăng nhập lại thật chịu trách nhiệm
    việc đó. Đây thuần là dòng audit.
    """
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET last_relogin_at = now(), last_relogin_status = %s WHERE id = %s",
                (status, account_row_id),
            )
    except psycopg.Error as exc:
        logger.error("db_stamp_last_relogin_failed", account_row_id=account_row_id, status=status, error=str(exc))


def account_count_needing_manual(platform: str) -> int:
    """Được bộ lập lịch auto-login dùng để quyết định có cảnh báo (Telegram) không. Mọi tài
    khoản bị gắn cờ "needs_manual_login=true" đều là việc người vận hành cần xử lý; bộ lập
    lịch log tổng số mỗi lượt để một lô bị kẹt hiện ra trong một dòng thay vì từng dòng một.
    """
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts WHERE platform = %s "
                "AND enabled = true AND needs_manual_login = true",
                (platform,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_account_count_needing_manual_failed", platform=platform, error=str(exc))
        return 0
    return int(row["n"]) if row else 0


def get_account_for_relogin(platform: str, account_id: str) -> dict[str, Any] | None:
    """Tra một dòng platform_accounts theo (platform, account_id) với cùng bộ cột mà
    list_accounts_needing_relogin trả về, để luồng auto_login/consumer.py / phía Kafka tra
    được account_id từ một message đã publish ra lại dòng mà attempt_auto_login cần (id +
    password + totp_secret + cookie + token + email + email_password).

    Áp lại bộ lọc điều kiện của list_accounts_needing_relogin lúc consume, nên trả về None
    không chỉ khi dòng đã mất, mà cả khi nó đã bị tắt, bị checkpoint, bị gắn cờ
    needs_manual_login, hoặc được đánh dấu sống sau khi message được publish (hoặc với một
    message cũ được phát lại) - và khi Supabase tạm trục trặc. Consumer log việc không tìm
    thấy rồi đi tiếp, không bao giờ ném lỗi vào vòng lặp Kafka.
    """
    try:
        with connect() as conn:
            row = conn.execute(
                f"SELECT {_RELOGIN_COLUMNS} FROM platform_accounts WHERE {_RELOGIN_ELIGIBLE} "
                "AND account_id = %s ORDER BY id ASC LIMIT 1",
                (platform, account_id),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_for_relogin_failed", platform=platform, account_id=account_id, error=str(exc))
        return None
    return dict(row) if row is not None else None
