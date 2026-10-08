"""platform_proxies: chọn proxy, sổ sách sức khoẻ/cooldown, và việc ghim cố định tài
khoản<->proxy (platform_accounts.assigned_proxy_id) mà
services/pool.acquire_proxy_for_account dựa vào."""

from __future__ import annotations

from typing import Any, TypedDict

import psycopg

from social_crawler.db.connection import connect
from social_crawler.logger import get_logger

logger = get_logger(__name__)


class ProxyRow(TypedDict):
    id: int
    url: str
    username: str
    password: str
    login_use_proxy: bool
    # Cột platform của chính dòng đó ('facebook'/'threads'/... hoặc 'all' dùng chung) - KHÔNG
    # nhất thiết bằng nền tảng mà claim_proxy() được gọi với, vì một dòng dùng chung có thể phục
    # vụ nhiều nền tảng. Chỗ gọi ghi kết quả (record_proxy_outcome/mark_proxy_used) phải dùng
    # giá trị này làm khoá, không dùng nền tảng đã tìm theo, nếu không câu UPDATE âm thầm khớp
    # 0 dòng với mọi proxy có platform='all'.
    platform: str


def list_proxies(platform: str) -> list[ProxyRow]:
    """Mọi dòng proxy đang bật của nền tảng (riêng của nền tảng + 'all' dùng chung), bất kể
    trạng thái cooldown - khác với claim_proxy, vốn cố ý giấu dòng đang cooldown
    vì chúng đang chọn một proxy để dùng ngay. Dành cho lượt ping sức khoẻ định kỳ (xem
    proxy_health_check.py) muốn thử mọi proxy đã cấu hình, kể cả proxy đang cooldown, để
    một lần hồi phục thật xoá cooldown ngay qua record_proxy_outcome(success=True) thay vì
    chờ hết hẹn giờ mà không có bằng chứng nó đã sống lại."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "ORDER BY (platform = 'all') ASC, id ASC",
                (platform,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_proxies_failed", platform=platform, error=str(exc))
        return []
    return [_proxy_row(row) for row in rows]


def claim_proxy(platform: str) -> ProxyRow | None:
    """Chọn nguyên tử proxy dùng được LRU cho nền tảng (ưu tiên dòng riêng của nền tảng hơn
    'all' dùng chung) và ghi last_used_at trong cùng một transaction. Cùng lý do SKIP LOCKED
    như claim_account - phương án dự phòng acquire_proxy() không ghim trước đây SELECT rồi
    UPDATE trên hai connection, nên hai chỗ gọi có thể cùng lấy một proxy."""
    try:
        with connect() as conn:
            picked = conn.execute(
                "SELECT id FROM platform_proxies WHERE enabled = true AND platform IN (%s, 'all') "
                "AND (cooldown_until IS NULL OR cooldown_until <= now()) "
                "ORDER BY (platform = 'all') ASC, last_used_at ASC NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE id = %s "
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy",
                (picked["id"],),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_claim_proxy_failed", platform=platform, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def get_proxy_by_id(proxy_id: int) -> ProxyRow | None:
    """Một dòng proxy cụ thể, nhưng chỉ khi nó hiện dùng được (đang bật, không giữa cooldown) -
    cùng hợp đồng "None nghĩa là hiện không dùng được" như claim_proxy(). Dùng để tra
    assigned_proxy_id đã ghim của một tài khoản; một proxy đã ghim đang không khoẻ cố ý trả
    None ở đây thay vì quay sang proxy khác, để tài khoản đã ghim không bao giờ âm thầm rơi
    sang một IP khác với IP nó đã gắn - xem services/pool.acquire_proxy_for_account."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy "
                "FROM platform_proxies WHERE id = %s AND enabled = true "
                "AND (cooldown_until IS NULL OR cooldown_until <= now())",
                (proxy_id,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_by_id_failed", proxy_id=proxy_id, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def pin_account_to_least_loaded_proxy(platform: str, account_row_id: int) -> ProxyRow | None:
    """Khoá proxy còn sống đang ít tải nhất, ghim tài khoản này vào nó, và ghi last_used_at
    trong một transaction. Hai lần lấy lần đầu không thể cùng đọc load=0 rồi rơi vào cùng
    một proxy trong khi một proxy trống hơn đang không bị khoá - SKIP LOCKED đẩy chỗ gọi thứ
    hai sang ứng viên kế tiếp."""
    try:
        with connect() as conn:
            picked = conn.execute(
                "SELECT pp.id FROM platform_proxies pp "
                "WHERE pp.enabled = true AND pp.platform IN (%s, 'all') "
                "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()) "
                "ORDER BY (pp.platform = 'all') ASC, "
                "(SELECT count(*) FROM platform_accounts pa "
                " WHERE pa.assigned_proxy_id = pp.id AND pa.enabled = true "
                " AND pa.status != 'checkpoint') ASC, "
                "pp.last_used_at ASC NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE id = %s "
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy",
                (picked["id"],),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE platform_accounts SET assigned_proxy_id = %s WHERE id = %s",
                (row["id"], account_row_id),
            )
    except psycopg.Error as exc:
        logger.error(
            "db_pin_account_to_least_loaded_proxy_failed",
            platform=platform,
            account_row_id=account_row_id,
            error=str(exc),
        )
        return None
    return _proxy_row(row)


def _proxy_row(row: dict[str, Any]) -> ProxyRow:
    return {
        "id": row["id"],
        "url": row["proxy_url"],
        "username": row["username"],
        "password": row["password"],
        "login_use_proxy": row["login_use_proxy"],
        "platform": row["platform"],
    }


def get_account_proxy_assignment(platform: str, account_key: str) -> tuple[int, int | None] | None:
    """(id dòng, assigned_proxy_id) cho một dòng platform_accounts, hoặc None nếu không có dòng
    đó (slot đăng nhập tay DEFAULT_ACCOUNT_KEY, hoặc một key không khớp dòng nào).
    account_key được tra không phân biệt hoa thường trên *cả* account_id lẫn email, khớp với
    normalize_account_key(account.get("email") or account["id"]) của
    facebook/auth/bootstrap.py - key "tài khoản đang active" cache trong Redis mà các chỗ
    gọi như comet_graphql_client.py truyền vào đây có thể là một trong hai trường tuỳ tài
    khoản đó có đặt trường nào, nên chỉ khớp account_id sẽ âm thầm trượt mọi tài khoản có
    key lấy từ trường email.

    assigned_proxy_id là None khi tài khoản này chưa được ghim cố định vào proxy nào - xem
    services/pool.acquire_proxy_for_account, nơi tự ghim ở lần dùng đầu."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT id, assigned_proxy_id FROM platform_accounts "
                "WHERE platform = %s AND (lower(account_id) = lower(%s) OR lower(email) = lower(%s))",
                (platform, account_key, account_key),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error(
            "db_get_account_proxy_assignment_failed", platform=platform, account_key=account_key, error=str(exc)
        )
        return None
    if row is None:
        return None
    return row["id"], row["assigned_proxy_id"]


def get_proxy_raw_status(proxy_id: int) -> dict[str, Any] | None:
    """enabled/consecutive_failures của một dòng proxy bất kể sức khoẻ hiện tại (khác với
    get_proxy_by_id, vốn trả None cho mọi thứ hiện không dùng được) - cho
    services/pool.acquire_proxy_for_account phân biệt một proxy chỉ đang cooldown tạm thời
    (giữ ghim, bỏ qua lượt này) với một proxy bị cố ý tắt hoặc đã lỗi liên tiếp đủ nhiều lần
    để coi là chết (ghim tài khoản sang chỗ khác). None nếu dòng không còn tồn tại."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT enabled, consecutive_failures FROM platform_proxies WHERE id = %s",
                (proxy_id,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_raw_status_failed", proxy_id=proxy_id, error=str(exc))
        return None
    return dict(row) if row is not None else None


def platform_has_any_proxy(platform: str) -> bool:
    """Nền tảng có ít nhất một dòng platform_proxies nào (riêng của nền tảng hoặc 'all' dùng
    chung) hay không, bất kể sức khoẻ - cho services/pool.acquire_proxy_for_account(
    required=True) phân biệt "nền tảng này đã cấu hình proxy nhưng hiện không cái nào dùng
    được" (phải lỗi rõ ràng thay vì âm thầm chạy không proxy - xem hàm đó) với "nền tảng này
    chưa bao giờ dùng proxy" (chạy không proxy là ổn, không đổi so với trước khi có ghim cố
    định)."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM platform_proxies WHERE platform IN (%s, 'all') LIMIT 1", (platform,)
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_platform_has_any_proxy_failed", platform=platform, error=str(exc))
        return False
    return row is not None


def mark_proxy_used(platform: str, proxy_url: str) -> None:
    """Cùng lý do như last_used_at của claim_account - ghi lúc lấy, không phải lúc trả, để các lời gọi
    acquire_proxy() liên tiếp không cùng thấy một last_used_at cũ."""
    try:
        with connect() as conn:
            conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE platform = %s AND proxy_url = %s",
                (platform, proxy_url),
            )
    except psycopg.Error as exc:
        logger.error("db_mark_proxy_used_failed", platform=platform, proxy_url=proxy_url, error=str(exc))


# 2^20 x kể cả một base lớn (ví dụ 60 phút) vẫn nằm xa trong phạm vi interval của Postgres,
# và cao xa hơn mọi trần cooldown_max_minutes hợp lý - nên trần bên dưới luôn là thứ thực
# sự giới hạn cooldown.
_COOLDOWN_MAX_EXPONENT = 20


def record_proxy_outcome(platform: str, proxy_url: str, *, success: bool) -> None:
    """Cập nhật circuit-breaker sau một lần thử request/đăng nhập qua proxy này - xem
    services/pool.py. Proxy chỉ bao giờ bị xử lý kiểu lỗi nhẹ (khái niệm "checkpoint" không
    áp dụng cho proxy) - một proxy tồi vẫn là proxy, chỉ đáng lùi lại một thời gian thay vì
    tắt hẳn, vì một IP dân cư/di động đang chập chờn lúc này rất có thể hồi phục khi việc
    xoay vòng phía nhà cung cấp chuyển sang IP khác."""
    try:
        with connect() as conn:
            if success:
                # Không log vô điều kiện - đoạn này chạy sau gần như mọi request, nên một dòng log vô điều
                # kiện ở đây chỉ là nhiễu. Đọc giá trị trước khi cập nhật (một cuộc đua nhỏ, chỉ ảnh hưởng
                # log, với chỗ gọi đồng thời là chấp nhận được) để chỉ báo trường hợp thực sự đáng xem: một
                # proxy đang xuống cấp vừa hồi phục, đối xứng với cảnh báo "proxy_degraded" bên dưới cho
                # nhánh lỗi.
                before = conn.execute(
                    "SELECT consecutive_failures FROM platform_proxies WHERE platform = %s AND proxy_url = %s",
                    (platform, proxy_url),
                ).fetchone()
                conn.execute(
                    "UPDATE platform_proxies SET status = 'active', consecutive_failures = 0, "
                    "cooldown_until = NULL WHERE platform = %s AND proxy_url = %s",
                    (platform, proxy_url),
                )
                if before is not None and before["consecutive_failures"] > 0:
                    logger.info(
                        "proxy_recovered",
                        platform=platform,
                        proxy_url=proxy_url,
                        previous_consecutive_failures=before["consecutive_failures"],
                    )
            else:
                # Base/trần lấy từ dashboard (proxy_settings). Số mũ bị giới hạn ở _COOLDOWN_MAX_EXPONENT
                # trước khi nhân: không giới hạn thì power(2, n) * interval tràn phạm vi interval của
                # Postgres ở n=35 ("interval out of range", đã xác nhận 2026-09-28 với một proxy kẹt ở 34
                # lần lỗi) - mọi UPDATE sau đó đều lỗi, nên cooldown_until đóng băng trong quá khứ,
                # get_proxy_by_id cứ giao proxy chết ra như "dùng được", và việc ghim lại của
                # acquire_proxy_for_account (chỉ kích hoạt khi proxy đang cooldown) không bao giờ chạy.
                from social_crawler.db.proxy_settings import get_setting

                base_minutes = float(get_setting("cooldown_base_minutes"))
                max_minutes = float(get_setting("cooldown_max_minutes"))
                cur = conn.execute(
                    "UPDATE platform_proxies SET status = 'degraded', consecutive_failures = consecutive_failures + 1, "
                    "cooldown_until = now() + LEAST("
                    "  power(2, LEAST(consecutive_failures + 1, %s)) * (%s * interval '1 minute'), "
                    "  %s * interval '1 minute'"
                    ") WHERE platform = %s AND proxy_url = %s "
                    "RETURNING id, consecutive_failures, cooldown_until",
                    (_COOLDOWN_MAX_EXPONENT, base_minutes, max_minutes, platform, proxy_url),
                )
                row = cur.fetchone()
                if row is not None:
                    log_fields: dict[str, Any] = {
                        "platform": platform,
                        "proxy_url": proxy_url,
                        "consecutive_failures": row["consecutive_failures"],
                        "cooldown_until": str(row["cooldown_until"]),
                    }
                    if platform == "all":
                        # Lỗi của một proxy dùng chung không chỉ làm cooldown chỗ gọi nào tình cờ dùng nó - nó
                        # làm cooldown mọi tài khoản đang bật trên mọi nền tảng được ghim ở đây. Nêu rõ phạm vi
                        # ảnh hưởng thật ở đây (không chỉ "platform=all", vốn đọc như "không nền tảng cụ thể nào"
                        # thay vì "mọi nền tảng") chính là thứ lẽ ra đã làm vụ lây chéo TikTok/Facebook hôm đó
                        # (cả hai cùng dính 139.99.83.20 đang xuống cấp trong cùng ngày) hiện rõ từ một dòng log
                        # thay vì hai sự cố riêng rẽ, trông như không liên quan.
                        affected = conn.execute(
                            "SELECT platform, count(*) AS accounts FROM platform_accounts "
                            "WHERE assigned_proxy_id = %s AND enabled = true GROUP BY platform",
                            (row["id"],),
                        ).fetchall()
                        log_fields["affected_platforms"] = {r["platform"]: r["accounts"] for r in affected}
                    logger.warning("proxy_degraded", **log_fields)
    except psycopg.Error as exc:
        logger.error(
            "db_record_proxy_outcome_failed", platform=platform, proxy_url=proxy_url, success=success, error=str(exc)
        )
