"""platform_accounts: CRUD thường ngày của pool tài khoản - lấy/nhận/cập nhật/tắt dòng, ghi
kết quả đăng nhập/phát lại và kiểm tra cookie (xem services/pool.py cho API
acquire/release phía trên). Phần bảng này mà luồng auto-login dùng nằm ở db/relogin.py."""

from __future__ import annotations

from typing import Any

import psycopg

from social_crawler.clients.kira import diagnose_account_failure
from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


Account = dict[str, str]


def account_dict(row: dict[str, Any]) -> Account:
    return {
        "id": row["account_id"],
        "password": row["password"],
        "2fa": row["totp_secret"],
        "cookie": row["cookie"],
        "token": row["token"],
        "email": row["email"],
        # Mật khẩu của chính email khôi phục (không phải của tài khoản nền tảng) - cần để đăng
        # nhập hộp thư đó lấy mã xác minh, chưa luồng đăng nhập nào dùng, hiện chỉ được mang theo.
        "email_password": row["email_password"],
    }


def get_accounts(platform: str) -> list[Account]:
    """Các tài khoản đang bật của nền tảng mà pool (xem services/pool.py) hiện có thể giao ra -
    loại mọi tài khoản đang giữa cooldown (backoff của một lỗi tạm thời chưa hết) hoặc bị
    gắn 'checkpoint' (lỗi nặng - những tài khoản đó vốn cũng đã enabled=false, nhưng kiểm
    tra status rõ ràng để ghi lại lý do, thay vì chỉ dựa vào enabled). Sắp theo dùng lâu
    nhất chưa dùng lại trước (NULL - chưa dùng bao giờ - đứng đầu) để pool chỉ cần lấy
    accounts[0] thay vì cần bộ đếm xoay vòng riêng."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY last_used_at ASC NULLS FIRST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_failed", platform=platform, error=str(exc))
        return []

    return [account_dict(row) for row in rows]


def is_account_usable(platform: str, account_key: str) -> bool:
    """False khi dòng của tài khoản này đang bị tắt hoặc gắn 'checkpoint' - để con trỏ "tài khoản đang active" trong
    Redis không tiếp tục được dùng sau khi tài khoản đã bị tắt (2026-10-07: Threads dùng lại malanalaxx sau checkpoint
    14 lần liên tiếp vì token cache của nó vẫn còn). True với slot đăng nhập tay mặc định ("default", không có dòng) hoặc
    khi DB lỗi; tài khoản không còn dòng nào (đã xoá) là False - không chặn crawl vì một lần đọc DB trục trặc."""
    key = (account_key or "").strip().lower()
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT enabled, status FROM platform_accounts WHERE platform = %s "
                "AND (lower(account_id) = %s OR lower(email) = %s) LIMIT 1",
                (platform, key, key),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_is_account_usable_failed", platform=platform, account=key, error=str(exc))
        return True
    if row is None:
        # Chỉ slot đăng nhập tay "default" được phép không có dòng. Tài khoản khác không có dòng là tài khoản đã bị XOÁ
        # khỏi dashboard - trước 2026-10-11 hàm trả True ở đây nên crawler cứ dùng tiếp session cache của nó.
        return key == "default"
    return bool(row["enabled"]) and row["status"] != "checkpoint"


def last_checked_at_map(platform: str) -> dict[str, Any]:
    """account_id -> last_checked_at (lần kiểm tra cookie gần nhất, có thể None) - để cron kiểm tra cookie
    (scripts/check_facebook_cookies.py --stale-hours) bỏ qua tài khoản vừa được kiểm tra."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT account_id, last_checked_at FROM platform_accounts WHERE platform = %s", (platform,)
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_last_checked_at_map_failed", platform=platform, error=str(exc))
        return {}
    return {row["account_id"]: row["last_checked_at"] for row in rows}


def list_enabled_accounts(platform: str) -> list[Account]:
    """Các tài khoản đang bật, không bị checkpoint của nền tảng, kể cả các dòng đang giữa
    cooldown. Lấy tài khoản để crawl vẫn dùng get_accounts() (bỏ qua cooldown); script làm
    ấm feed dùng hàm này để một tài khoản đang cooldown vẫn có một phiên lướt nhẹ mà không
    phải chờ hết circuit breaker."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' "
                "ORDER BY last_used_at ASC NULLS FIRST, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_enabled_accounts_failed", platform=platform, error=str(exc))
        return []
    return [account_dict(row) for row in rows]


def get_accounts_by_check_status(platform: str, status: str) -> list[Account]:
    """Các tài khoản đang bật của nền tảng có lần kiểm tra gần nhất (xem record_cookie_check /
    scripts/check_facebook_cookies.py) trả về `status` - ví dụ "dead", để tìm đúng những
    tài khoản mà một lượt đăng nhập lại chạy một lần (scripts/relogin_facebook_accounts.py)
    cần đụng tới, mà không phải dò lại cookie của mọi tài khoản trước."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND enabled = true AND last_check_status = %s "
                "ORDER BY id ASC",
                (platform, status),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_get_accounts_by_check_status_failed", platform=platform, status=status, error=str(exc))
        return []
    return [account_dict(row) for row in rows]


def claim_account(platform: str) -> Account | None:
    """Chọn nguyên tử tài khoản khoẻ dùng lâu nhất chưa dùng lại (LRU) của nền tảng và ghi
    last_used_at trong cùng một transaction (SELECT ... FOR UPDATE SKIP LOCKED rồi UPDATE).
    Vì vậy hai lời gọi acquire_account() cùng lúc không thể cùng thấy một last_used_at cũ và
    cùng lấy đi một dòng - lời gọi thứ hai bỏ qua dòng bị khoá và lấy dòng LRU kế tiếp.

    get_accounts() vẫn là một lần đọc thường (xoay vòng Threads/TikTok vẫn liệt kê pool); chỉ
    đường lấy tài khoản kiểu Facebook mới cần nhận dòng."""
    try:
        with connect() as conn:
            picked = conn.execute(
                "SELECT id FROM platform_accounts WHERE platform = %s AND enabled = true "
                "AND status != 'checkpoint' AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                # Tài khoản có lần kiểm tra cookie gần nhất báo "dead" xếp cuối: chỉ dùng khi không còn tài khoản sống.
                "ORDER BY (last_check_status IS NOT DISTINCT FROM 'dead') ASC, last_used_at ASC NULLS FIRST, id ASC "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_accounts SET last_used_at = now() WHERE id = %s "
                "RETURNING account_id, password, totp_secret, cookie, token, email, email_password",
                (picked["id"],),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_claim_account_failed", platform=platform, error=str(exc))
        return None
    return account_dict(row) if row is not None else None


def get_account_by_row_id(platform: str, row_id: int) -> Account | None:
    """Cùng dạng với các dòng của get_accounts(), nhưng là một dòng theo khoá chính dạng số bất
    kể enabled - dùng bởi tiktok/auth/bootstrap.py để nhắm một tài khoản cụ thể cho việc
    làm mới danh tính bằng tay, vốn vẫn phải chạy được với một tài khoản đã bị tắt sau khi
    odin_id của nó bị cũ."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s AND id = %s",
                (platform, row_id),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_by_row_id_failed", platform=platform, row_id=row_id, error=str(exc))
        return None

    if row is None:
        return None
    return account_dict(row)


def get_account_by_key(platform: str, key: str) -> Account | None:
    """Tra một dòng platform_accounts theo email hoặc account_id, kể cả dòng bị tắt/checkpoint.
    Restore trên dashboard và refresh có ghim --account cần cookie/session đã lưu kể cả khi
    pool sẽ không giao dòng đó ra cho một lượt crawl bình thường."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT account_id, password, totp_secret, cookie, token, email, email_password "
                "FROM platform_accounts WHERE platform = %s "
                "AND (lower(coalesce(email, '')) = lower(%s) OR lower(account_id) = lower(%s)) "
                "ORDER BY id ASC LIMIT 1",
                (platform, needle, needle),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_by_key_failed", platform=platform, error=str(exc))
        return None
    return account_dict(row) if row is not None else None


def get_account_pk(platform: str, key: str) -> int | None:
    """platform_accounts.id dạng số cho email hoặc account_id, kể cả dòng bị tắt/checkpoint.
    Import cookie / restore của TikTok ghim một dòng theo cách này vì các lần ghi danh tính
    (update_tiktok_identity) dùng khoá chính, không dùng cột account_id (bị bootstrap ghi đè
    bằng device_id)."""
    needle = (key or "").strip()
    if not needle:
        return None
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT id FROM platform_accounts WHERE platform = %s "
                "AND (lower(coalesce(email, '')) = lower(%s) OR lower(account_id) = lower(%s)) "
                "ORDER BY id ASC LIMIT 1",
                (platform, needle, needle),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_account_pk_failed", platform=platform, error=str(exc))
        return None
    return int(row["id"]) if row is not None else None


def update_account_cookie(platform: str, row_id: int, cookie: str) -> bool:
    """Chỉ ghi đè cột cookie của một dòng - dùng khi import cookie TikTok trước khi bước bắt
    danh tính tiếp theo điền device_id/odin_id."""
    try:
        with connect() as conn:
            cur = conn.execute(
                "UPDATE platform_accounts SET cookie = %s WHERE platform = %s AND id = %s",
                (cookie, platform, row_id),
            )
            updated = cur.rowcount > 0
    except psycopg.Error as exc:
        logger.error("db_update_account_cookie_failed", platform=platform, row_id=row_id, error=str(exc))
        return False
    if updated:
        logger.info("account_cookie_updated", platform=platform, row_id=row_id)
    else:
        logger.warning("account_cookie_update_no_match", platform=platform, row_id=row_id)
    return updated


def reactivate_account(platform: str, account_id: str) -> None:
    """Gỡ checkpoint/tắt sau khi một lần restore (hoặc import cookie) thực sự bắt lại được
    token GraphQL với session còn sống. Khác với record_account_outcome(success=True), vốn
    không bật lại enabled - một dòng khoẻ do người tắt nên giữ tắt cho tới khi có người bật,
    nhưng một checkpoint mà cookie đã lưu vừa chứng minh là sai thì không nên giữ ở trạng
    thái kết thúc."""
    try:
        with connect() as conn:
            cur = conn.execute(
                "UPDATE platform_accounts SET enabled = true, status = 'active', "
                "consecutive_failures = 0, cooldown_until = NULL, last_check_note = NULL, "
                "last_checked_at = now() WHERE platform = %s AND account_id = %s",
                (platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error("db_reactivate_account_failed", platform=platform, account_id=account_id, error=str(exc))
        return
    if cur.rowcount > 0:
        # Log nguồn sự thật duy nhất cho lần chuyển trạng thái này - một số chỗ gọi (ví dụ
        # tiktok/auth/bootstrap.py) từng log thêm event riêng tên khác bên trên cái này; ưu tiên
        # dùng cái này để câu hỏi "tài khoản này đã được kích hoạt lại chưa" chỉ có đúng một tên
        # event để tìm, bất kể chỗ gọi.
        logger.info("account_reactivated", platform=platform, account_id=account_id)
    else:
        logger.warning("account_reactivate_no_match", platform=platform, account_id=account_id)


def update_tiktok_identity(
    row_id: int, *, device_id: str, odin_id: str, cookie: str, lookup_key: str | None = None
) -> bool:
    """Ghi một bộ danh tính vừa bắt được trở lại một dòng platform_accounts (platform='tiktok')
    - xem tiktok/auth/accounts.py để biết vì sao dùng lại account_id/token/cookie thay vì
    cột riêng (account_id -> device_id, token -> odin_id, cookie -> header Cookie thô). Được
    tiktok/auth/bootstrap.py gọi sau khi bắt bằng trình duyệt thành công. Không raise khi lỗi
    DB, cùng lý do như disable_account: bản thân lần bắt đã thành công, không nên lẫn lỗi ghi
    ở đây với chuyện đó.

    lookup_key: ghim do người đặt (email / pending-tiktok) để lần refresh --account tiếp
    theo vẫn tìm được dòng này sau khi account_id thành device_id.
    """
    try:
        with connect() as conn:
            if lookup_key:
                conn.execute(
                    "UPDATE platform_accounts SET account_id = %s, token = %s, cookie = %s, "
                    "email = CASE WHEN coalesce(email, '') = '' THEN %s ELSE email END "
                    "WHERE platform = 'tiktok' AND id = %s",
                    (device_id, odin_id, cookie, lookup_key, row_id),
                )
            else:
                conn.execute(
                    "UPDATE platform_accounts SET account_id = %s, token = %s, cookie = %s "
                    "WHERE platform = 'tiktok' AND id = %s",
                    (device_id, odin_id, cookie, row_id),
                )
    except psycopg.Error as exc:
        logger.error("db_update_tiktok_identity_failed", row_id=row_id, error=str(exc))
        return False
    return True


def record_account_outcome(
    platform: str, account_id: str, *, success: bool, hard_failure: bool = False, reason: str | None = None
) -> None:
    """Cập nhật circuit-breaker sau một lần thử đăng nhập/phát lại - xem services/pool.py cho
    API acquire/release mà hàm này phục vụ.

    - success: xoá mọi cooldown/chuỗi lỗi - một lần đăng nhập sạch chứng minh tài khoản lại
      ổn, bất kể trước đó đã xảy ra gì. Cũng xoá last_check_note cũ từ lần checkpoint trước -
      nó không còn mô tả trạng thái hiện tại của tài khoản.
    - hard_failure (checkpoint/2FA/không có c_user sau một lần đăng nhập thật): cùng hành
      động kết thúc mà disable_account() vốn làm cho đúng tín hiệu này - lật enabled=false để
      phải có người gỡ, không có cooldown tự hết hạn (checkpoint không tự lành theo hẹn giờ
      như bị giới hạn rate). reason, khi có, là text lỗi kỹ thuật thô (ví dụ RuntimeError mà
      bootstrap.py sắp raise) - được gửi cho Kira để có một chẩn đoán ngắn dễ đọc lưu vào
      last_check_note (xem clients/kira.diagnose_account_failure), được cinemark-api/
      dashboard hiển thị cạnh tài khoản. Cố gắng hết mức: chẩn đoán thiếu/lỗi vẫn để việc tắt
      diễn ra, chỉ để last_check_note là NULL.
    - còn lại (lỗi nhẹ - mạng chập chờn, timeout, lỗ hổng tự động hoá): backoff tăng dần
      ngắn (5 phút, 10 phút, 20 phút, ... tối đa 2 giờ) theo consecutive_failures, để một
      lượt chạy chập chờn không loại tài khoản vĩnh viễn nhưng cũng không bị thử lại ngay một
      giây sau.
    """
    try:
        with connect() as conn:
            if success:
                conn.execute(
                    "UPDATE platform_accounts SET status = 'active', consecutive_failures = 0, "
                    "cooldown_until = NULL, last_check_note = NULL, last_checked_at = now() "
                    "WHERE platform = %s AND account_id = %s",
                    (platform, account_id),
                )
            elif hard_failure:
                note = diagnose_account_failure(reason) if reason else None
                conn.execute(
                    "UPDATE platform_accounts SET status = 'checkpoint', enabled = false, "
                    "consecutive_failures = consecutive_failures + 1, "
                    "last_check_note = %s, last_checked_at = now() "
                    "WHERE platform = %s AND account_id = %s",
                    (note, platform, account_id),
                )
                # Mức error - xem _telegram_processor trong logger.py - luôn cảnh báo: tài khoản bị
                # checkpoint bị tắt và giữ nguyên như vậy cho tới khi có người gỡ, khác với cooldown tự
                # hết hạn của lỗi nhẹ.
                logger.error(
                    "account_checkpointed",
                    platform=platform,
                    account_id=account_id,
                    note=note or "account disabled - needs manual re-login/cookie-import before it's usable again",
                )
            else:
                cur = conn.execute(
                    "UPDATE platform_accounts SET consecutive_failures = consecutive_failures + 1, "
                    "cooldown_until = now() + LEAST("
                    "  power(2, consecutive_failures + 1) * interval '5 minutes', interval '2 hours'"
                    ") WHERE platform = %s AND account_id = %s "
                    "RETURNING consecutive_failures, cooldown_until",
                    (platform, account_id),
                )
                row = cur.fetchone()
                if row is not None:
                    logger.warning(
                        "account_soft_failure",
                        platform=platform,
                        account_id=account_id,
                        consecutive_failures=row["consecutive_failures"],
                        cooldown_until=str(row["cooldown_until"]),
                    )
    except psycopg.Error as exc:
        logger.error(
            "db_record_account_outcome_failed",
            platform=platform,
            account_id=account_id,
            success=success,
            hard_failure=hard_failure,
            error=str(exc),
        )


def record_cookie_check(platform: str, account_id: str, *, status: str, note: str | None = None) -> None:
    """Ghi kết quả của một lần kiểm tra cookie còn sống thụ động (xem
    scripts/check_facebook_cookies.py) - dùng lại cookie đã cache để tải một trang thật mà
    không có lần thử đăng nhập/phát lại thật nào phía sau. Chỉ đụng tới
    last_checked_at/last_check_status/last_check_note (cùng các cột kiểm tra tay mà nút
    "Check" trên dashboard của cinemark-api ghi qua update_account_check_result của nó) - cố
    ý KHÔNG dùng record_account_outcome/pool.release_account, vì docstring của chúng cảnh
    báo không gọi cho một lượt chạy "chỉ dùng lại session đã cache mà không kiểm tra mới",
    sẽ reset hoặc tăng sai một chuỗi lỗi thật mà lần kiểm tra này chưa hề chạm tới. Cố gắng
    hết mức như disable_account - một lần kiểm tra sức khoẻ không ghi được kết quả của mình
    không nên làm crash cả lô."""
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET last_checked_at = now(), last_check_status = %s, last_check_note = %s "
                "WHERE platform = %s AND account_id = %s",
                (status, note, platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_record_cookie_check_failed", platform=platform, account_id=account_id, status=status, error=str(exc)
        )


def disable_account(platform: str, account_id: str, reason: str) -> bool:
    """Lật một dòng platform_accounts thành enabled=false - được gọi khi một lần đăng nhập thật
    bằng thông tin đăng nhập đã lưu của chính tài khoản trả về mà không có cookie đã đăng
    nhập, tín hiệu rõ ràng nhất cho thấy Facebook/Instagram đã bật một màn hình
    checkpoint/2FA mà auto-login không bấm qua được (xem phép kiểm tra c_user/ds_user_id của
    bootstrap.py). Không raise khi lỗi DB - chỗ gọi đang xử lý dở lỗi checkpoint; một lần
    tắt không ghi được không nên che lỗi gốc đó, chỉ có nghĩa là tài khoản này được thử lại
    (và nhiều khả năng lỗi y vậy) ở vòng xoay sau thay vì bị bỏ qua. Trả về việc cập nhật có
    thực sự thành công không, để chỗ gọi biết có nên vẫn cảnh báo không. reason được gửi cho
    Kira để có một chẩn đoán ngắn dễ đọc lưu vào last_check_note (xem
    clients/kira.diagnose_account_failure) - cùng hợp đồng cố gắng hết mức như nhánh
    hard_failure của record_account_outcome: chẩn đoán thiếu/lỗi vẫn để việc tắt diễn ra,
    chỉ là không có ghi chú."""
    note = diagnose_account_failure(reason)
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_accounts SET enabled = false, last_check_note = %s, last_checked_at = now() "
                "WHERE platform = %s AND account_id = %s",
                (note, platform, account_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_disable_account_failed", platform=platform, account_id=account_id, reason=reason, error=str(exc)
        )
        return False
    logger.error("account_disabled", platform=platform, account_id=account_id, reason=reason, note=note)
    return True


def has_enabled_accounts(platform: str) -> bool:
    """Nền tảng có ít nhất một dòng platform_accounts đang bật hay không, bất kể trạng thái
    cooldown/checkpoint hiện tại - cho services/pool.acquire_account phân biệt "nền tảng này
    hoàn toàn chưa cấu hình tài khoản nào" (bình thường - quay về slot tay/mặc định) với "đã
    cấu hình tài khoản nhưng tất cả hiện đều bị checkpoint hoặc đang cooldown" (sự cố thật
    đáng cảnh báo)."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_accounts WHERE platform = %s AND enabled = true LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_has_enabled_accounts_failed", platform=platform, error=str(exc))
        return False
    return row is not None
