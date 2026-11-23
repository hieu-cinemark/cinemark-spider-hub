"""platform_accounts: CRUD thường ngày của pool tài khoản - lấy/nhận/cập nhật/tắt dòng, ghi
kết quả đăng nhập/phát lại và kiểm tra cookie (xem services/pool.py cho API
acquire/release phía trên)."""

from __future__ import annotations

from typing import Any

from social_crawler.clients.ai import diagnose_account_failure
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events, d1_pool

logger = get_logger(__name__)


Account = dict[str, str]


def nurture_only_accounts(platform: str) -> dict[str, str | None]:
    """account_id -> email của các tài khoản đang bật được đánh dấu chỉ nuôi."""
    return d1_pool.nurture_only_accounts(platform)


def account_dict(row: dict[str, Any]) -> Account:
    return {
        "id": row["account_id"],
        "password": row["password"],
        # Secret dạng "ABCD EFGH ...": pyotp từ chối khoảng trắng.
        "2fa": "".join(row["totp_secret"].split()) if row["totp_secret"] else row["totp_secret"],
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
    return d1_pool.get_accounts(platform)


def is_account_usable(platform: str, account_key: str) -> bool:
    """False khi dòng của tài khoản này đang bị tắt hoặc gắn 'checkpoint' - để con trỏ "tài khoản đang active" trong
    Redis không tiếp tục được dùng sau khi tài khoản đã bị tắt (2026-10-07: Threads dùng lại malanalaxx sau checkpoint
    14 lần liên tiếp vì token cache của nó vẫn còn). True với slot đăng nhập tay mặc định ("default", không có dòng) hoặc
    khi DB lỗi; tài khoản không còn dòng nào (đã xoá) là False - không chặn crawl vì một lần đọc DB trục trặc."""
    return d1_pool.is_account_usable(platform, (account_key or "").strip().lower())


def list_enabled_accounts(platform: str, *, include_cooldown: bool = False) -> list[Account]:
    """Các tài khoản đang bật, không bị checkpoint và KHÔNG đang cooldown của nền tảng - cho script làm ấm feed.
    Trước 24/10/2026 hàm này lấy cả tài khoản đang cooldown ("một phiên lướt nhẹ"): tài khoản vừa bật lại với 1 giờ
    nghỉ bị nuôi ngay lúc lượt 22:00 và dính checkpoint - giờ nghỉ phải là nghỉ thật. include_cooldown=True: lấy cả
    tài khoản đang cooldown (TikTok dùng làm dự phòng khi không còn tài khoản nào sẵn sàng)."""
    return d1_pool.list_enabled_accounts(platform, include_cooldown=include_cooldown)


def get_accounts_by_check_status(platform: str, status: str) -> list[Account]:
    """Các tài khoản đang bật của nền tảng có lần kiểm tra gần nhất (xem record_cookie_check) trả về `status`, ví dụ "dead".

    "dead" chỉ tính khi dòng không đổi gì sau lần kiểm tra đó (24/10/2026): dán cookie mới trên dashboard chỉ ghi cột
    cookie (updated_at mới) mà không xoá last_check_status, nên tài khoản đã cứu vẫn bị làn / nuôi / auto-login coi là
    chết."""
    return d1_pool.get_accounts_by_check_status(platform, status)


def accounts_created_since(platform: str, since: str) -> dict[str, str | None]:
    """account_id -> email của tài khoản đang bật được thêm từ `since` (ISO) trở đi - xem services/warmup.py."""
    return d1_pool.accounts_created_since(platform, since)


def claim_account(platform: str, exclude: set[str] | frozenset[str] = frozenset()) -> Account | None:
    """Chọn nguyên tử tài khoản khoẻ dùng lâu nhất chưa dùng lại (LRU) của nền tảng và ghi
    last_used_at trong cùng một transaction (SELECT ... FOR UPDATE SKIP LOCKED rồi UPDATE).
    Vì vậy hai lời gọi acquire_account() cùng lúc không thể cùng thấy một last_used_at cũ và
    cùng lấy đi một dòng - lời gọi thứ hai bỏ qua dòng bị khoá và lấy dòng LRU kế tiếp.

    get_accounts() vẫn là một lần đọc thường (xoay vòng Threads/TikTok vẫn liệt kê pool); chỉ
    đường lấy tài khoản kiểu Facebook mới cần nhận dòng."""
    return d1_pool.claim_account(platform, exclude)


def get_account_by_row_id(platform: str, row_id: int) -> Account | None:
    """Cùng dạng với các dòng của get_accounts(), nhưng là một dòng theo khoá chính dạng số bất
    kể enabled - dùng bởi tiktok/auth/bootstrap.py để nhắm một tài khoản cụ thể cho việc
    làm mới danh tính bằng tay, vốn vẫn phải chạy được với một tài khoản đã bị tắt sau khi
    odin_id của nó bị cũ."""
    return d1_pool.get_account_by_row_id(platform, row_id)


def rest_account_until(platform: str, key: str, until: Any) -> bool:
    """Cho tài khoản (email hoặc account_id) nghỉ tới `until` (cooldown_until) - pool không giao nó ra cho tới lúc đó.
    Không đụng tới trạng thái / đếm lỗi: đây là nghỉ chủ động (vd. hết hạn mức tìm kiếm trong ngày), không phải lỗi."""
    needle = (key or "").strip()
    if not needle:
        return False
    rested = d1_pool.rest_account_until(platform, needle, until)
    if rested:
        account_events.record(platform, needle, "rested", until=str(until))
    return rested


def session_setup_blocked(platform: str, key: str) -> bool:
    """True khi không được dựng lại phiên từ cookie đã lưu: tài khoản đang nghỉ, hoặc phiên đã bị từ chối ("dead") mà chưa
    có cookie mới (cùng quy ước updated_at với get_accounts_by_check_status). DB lỗi cũng là True: thà không gửi còn hơn
    phát lại đúng cookie vừa bị từ chối."""
    return d1_pool.session_setup_blocked(platform, (key or "").strip())


def get_account_by_key(platform: str, key: str) -> Account | None:
    """Tra một dòng platform_accounts theo email hoặc account_id, kể cả dòng bị tắt/checkpoint.
    Restore trên dashboard và refresh có ghim --account cần cookie/session đã lưu kể cả khi
    pool sẽ không giao dòng đó ra cho một lượt crawl bình thường."""
    needle = (key or "").strip()
    return d1_pool.get_account_by_key(platform, needle) if needle else None


def get_account_pk(platform: str, key: str) -> int | None:
    """platform_accounts.id dạng số cho email hoặc account_id, kể cả dòng bị tắt/checkpoint.
    Import cookie / restore của TikTok ghim một dòng theo cách này vì các lần ghi danh tính
    (update_tiktok_identity) dùng khoá chính, không dùng cột account_id (bị bootstrap ghi đè
    bằng device_id)."""
    needle = (key or "").strip()
    return d1_pool.get_account_pk(platform, needle) if needle else None


def update_account_cookie(platform: str, row_id: int, cookie: str) -> bool:
    """Chỉ ghi đè cột cookie của một dòng - dùng khi import cookie TikTok trước khi bước bắt
    danh tính tiếp theo điền device_id/odin_id."""
    updated = d1_pool.update_account_cookie(platform, row_id, cookie)
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
    found = d1_pool.reactivate_account(platform, account_id)
    if found is None:
        return
    if found:
        logger.info("account_reactivated", platform=platform, account_id=account_id)
    else:
        logger.warning("account_reactivate_no_match", platform=platform, account_id=account_id)
    return


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
    return d1_pool.update_tiktok_identity(
        row_id, device_id=device_id, odin_id=odin_id, cookie=cookie, lookup_key=lookup_key
    )


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
      bootstrap.py sắp raise) - được gửi cho AI (Bee) để có một chẩn đoán ngắn dễ đọc lưu vào
      last_check_note (xem clients/ai.diagnose_account_failure), được cinemark-api/
      dashboard hiển thị cạnh tài khoản. Cố gắng hết mức: chẩn đoán thiếu/lỗi vẫn để việc tắt
      diễn ra, chỉ để last_check_note là NULL.
    - còn lại (lỗi nhẹ - mạng chập chờn, timeout, lỗ hổng tự động hoá): backoff tăng dần
      ngắn (5 phút, 10 phút, 20 phút, ... tối đa 2 giờ) theo consecutive_failures, để một
      lượt chạy chập chờn không loại tài khoản vĩnh viễn nhưng cũng không bị thử lại ngay một
      giây sau.
    """
    note = diagnose_account_failure(reason) if (hard_failure and not success and reason) else None
    row = d1_pool.record_account_outcome(platform, account_id, success=success, hard_failure=hard_failure, note=note)
    if hard_failure and not success:
        logger.error(
            "account_checkpointed",
            platform=platform,
            account_id=account_id,
            note=note or "account disabled - needs manual re-login/cookie-import before it's usable again",
        )
    elif row is not None:
        logger.warning(
            "account_soft_failure",
            platform=platform,
            account_id=account_id,
            consecutive_failures=row["consecutive_failures"],
            cooldown_until=str(row["cooldown_until"]),
        )


def record_cookie_check(platform: str, account_id: str, *, status: str, note: str | None = None) -> None:
    """Ghi kết quả quan sát phiên từ nuôi tài khoản hoặc crawl. Chỉ đụng tới
    last_checked_at/last_check_status/last_check_note (cùng các cột kiểm tra tay mà nút
    "Check" trên dashboard của cinemark-api ghi qua update_account_check_result của nó) - cố
    ý KHÔNG dùng record_account_outcome/pool.release_account, vì docstring của chúng cảnh
    báo không gọi cho một lượt chạy "chỉ dùng lại session đã cache mà không kiểm tra mới",
    sẽ reset hoặc tăng sai một chuỗi lỗi thật mà lần kiểm tra này chưa hề chạm tới. Cố gắng
    hết mức như disable_account - một lần kiểm tra sức khoẻ không ghi được kết quả của mình
    không nên làm crash cả lô."""
    if d1_pool.record_cookie_check(platform, account_id, status=status, note=note) and status == "dead":
        account_events.record(platform, account_id, "session_marked_dead", note=note)


def disable_account(platform: str, account_id: str, reason: str) -> bool:
    """Lật một dòng platform_accounts thành enabled=false - được gọi khi một lần đăng nhập thật
    bằng thông tin đăng nhập đã lưu của chính tài khoản trả về mà không có cookie đã đăng
    nhập, tín hiệu rõ ràng nhất cho thấy Facebook/Instagram đã bật một màn hình
    checkpoint/2FA (xem phép kiểm tra c_user/ds_user_id của
    bootstrap.py). Không raise khi lỗi DB - chỗ gọi đang xử lý dở lỗi checkpoint; một lần
    tắt không ghi được không nên che lỗi gốc đó, chỉ có nghĩa là tài khoản này được thử lại
    (và nhiều khả năng lỗi y vậy) ở vòng xoay sau thay vì bị bỏ qua. Trả về việc cập nhật có
    thực sự thành công không, để chỗ gọi biết có nên vẫn cảnh báo không. reason được gửi cho
    AI (Bee) để có một chẩn đoán ngắn dễ đọc lưu vào last_check_note (xem
    clients/ai.diagnose_account_failure) - cùng hợp đồng cố gắng hết mức như nhánh
    hard_failure của record_account_outcome: chẩn đoán thiếu/lỗi vẫn để việc tắt diễn ra,
    chỉ là không có ghi chú."""
    note = diagnose_account_failure(reason)
    if not d1_pool.disable_account(platform, account_id, reason, note):
        return False
    logger.error("account_disabled", platform=platform, account_id=account_id, reason=reason, note=note)
    account_events.record(platform, account_id, "account_disabled", reason=reason[:300])
    return True


def has_enabled_accounts(platform: str) -> bool:
    """Nền tảng có ít nhất một dòng platform_accounts đang bật hay không, bất kể trạng thái
    cooldown/checkpoint hiện tại - cho services/pool.acquire_account phân biệt "nền tảng này
    hoàn toàn chưa cấu hình tài khoản nào" (bình thường - quay về slot tay/mặc định) với "đã
    cấu hình tài khoản nhưng tất cả hiện đều bị checkpoint hoặc đang cooldown" (sự cố thật
    đáng cảnh báo)."""
    return d1_pool.has_enabled_accounts(platform)
