"""platform_proxies: chọn proxy, sổ sách sức khoẻ/cooldown, và việc ghim cố định tài
khoản<->proxy (platform_accounts.assigned_proxy_id) mà
services/pool.acquire_proxy_for_account dựa vào."""

from __future__ import annotations

from typing import Any, TypedDict

import psycopg

from social_crawler.core.logger import get_logger
from social_crawler.db.connection import connect

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
    # Gói nhà cung cấp sở hữu dòng proxy này (proxy_providers.key), None = proxy nhập
    # tay/tĩnh. Trước khi có cột này, code đoán nguồn bằng cách so proxy_url với
    # host:port trong token của từng gói.
    provider_key: str | None
    # Mã riêng của cổng này trong hệ thống nhà cung cấp (vd proxy_id của webtui, key của
    # cổng ở zingproxy). Được điền vào placeholder {ref} của link reset.
    provider_ref: str | None


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
                "SELECT id, platform, proxy_url, username, password, login_use_proxy, provider_key, provider_ref "
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
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref",
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
                "SELECT id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref "
                "FROM platform_proxies WHERE id = %s AND enabled = true "
                "AND (cooldown_until IS NULL OR cooldown_until <= now())",
                (proxy_id,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_get_proxy_by_id_failed", proxy_id=proxy_id, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def list_proxies_without_meta_accounts() -> list[ProxyRow]:
    """Proxy đang bật, ngoài cooldown, KHÔNG gán cho tài khoản Facebook/Threads nào, kể cả tài khoản đang tắt (03/11/2026: tài
    khoản tắt tạm vẫn giữ IP của nó) - cho việc gọi Facebook không đăng nhập: Meta nối tài khoản theo IP, IP bị chặn thì vạ lây
    tài khoản trên nó."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.provider_key "
                "FROM platform_proxies pp WHERE pp.enabled = true "
                "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()) "
                "AND NOT EXISTS (SELECT 1 FROM platform_accounts pa JOIN platform_proxies same ON same.id = "
                "pa.assigned_proxy_id WHERE same.proxy_url = pp.proxy_url AND pa.platform IN ('facebook', 'threads')) "
                "ORDER BY pp.id",
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_proxies_without_meta_accounts_failed", error=str(exc))
        return []
    return [_proxy_row(row) for row in rows]


def list_public_page_proxies() -> list[ProxyRow]:
    """Dedicated public endpoints only; even a disabled account keeps its assigned endpoint."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.provider_key "
                "FROM platform_proxies pp WHERE pp.enabled = true AND pp.platform = 'public' "
                "AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()) "
                "AND NOT EXISTS (SELECT 1 FROM platform_accounts pa JOIN platform_proxies same ON same.id = "
                "pa.assigned_proxy_id WHERE same.proxy_url = pp.proxy_url) ORDER BY pp.id",
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_public_page_proxies_failed", error=str(exc))
        return []
    return [_proxy_row(row) for row in rows]


def shared_proxy_violation(proxy_url: str, account_key: str) -> str | None:
    """Tài khoản khác đang giữ cùng địa chỉ proxy này hay không - nguyên tắc "một proxy,
    một tài khoản" áp cho MỌI nền tảng (12/11/2026): Meta nối Facebook với Threads qua
    IP, và một proxy của TikTok cũng là một IP, nên hai tài khoản bất kỳ nền tảng nào
    dùng chung một cổng đều là vi phạm.

    DB lỗi thì coi là vi phạm: không biết thì không gửi."""
    key = (account_key or "").strip().lower()
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts pa JOIN platform_proxies pp ON pp.id = pa.assigned_proxy_id "
                "WHERE pp.proxy_url = %s AND pa.enabled = true AND pa.status != 'checkpoint' "
                "AND lower(pa.account_id) != %s AND lower(coalesce(pa.email, '')) != %s",
                (proxy_url, key, key),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_shared_proxy_violation_failed", proxy_url=proxy_url, error=str(exc))
        return "policy_unreadable"
    return "shared_ip" if row and int(row["n"]) else None


def meta_proxy_violation(platform: str, proxy: ProxyRow, account_key: str) -> str | None:
    """Lý do tài khoản Meta không được dùng proxy này, hoặc None (04/11/2026). Trước đây nguyên tắc "mỗi tài khoản một IP cố
    định" chỉ nằm trong tài liệu: #90-92 chết trong một ngày trên proxy tự xoay, và ghim vào #15 không gặp chặn nào.

    Facebook phải đúng nhãn static; Threads chỉ bị chặn proxy tự xoay (#17 đang chờ 2 tài khoản Threads và
    chưa có IP thay thế). Quy tắc "một proxy, một tài khoản" (mọi nền tảng) nằm ở
    shared_proxy_violation - hàm này chỉ nhìn nhãn của proxy nên không cần DB."""
    ip_type = proxy.get("ip_type", "unknown")
    if ip_type == "rotating":
        return "rotating_ip"
    if platform == "facebook":
        if proxy.get("platform") == "public":
            return "public_proxy"
        if ip_type != "static":
            return "ip_type_not_static"
    return None


def proxy_held_by_other_account(proxy_id: int, account_row_id: int) -> bool:
    """Proxy này đang được một tài khoản khác giữ hay không - nguyên tắc "một proxy, một
    tài khoản" (mọi nền tảng). Dùng khi người vận hành ghim tay trên dashboard, và khi pool
    tự ghim, để không bao giờ giao cùng một IP cho hai tài khoản."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts "
                "WHERE assigned_proxy_id = %s AND id != %s AND enabled = true AND status != 'checkpoint'",
                (proxy_id, account_row_id),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_proxy_held_by_other_account_failed", proxy_id=proxy_id, error=str(exc))
        return True
    return bool(row and int(row["n"]))


def facebook_accounts_blocked_by_proxy_policy() -> set[str]:
    """account_id và email (chữ thường) của tài khoản Facebook đang bật mà proxy đã ghim vi phạm meta_proxy_violation, để làn
    không giao việc cho chúng rồi job thất bại lặp lại. Chưa ghim thì không tính: pin_account_to_free_proxy tự lọc."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT a.account_id, a.email FROM platform_accounts a JOIN platform_proxies p ON p.id = a.assigned_proxy_id "
                "WHERE a.platform = 'facebook' AND a.enabled = true AND (p.ip_type != 'static' OR p.platform = 'public' "
                "OR EXISTS (SELECT 1 FROM platform_accounts o JOIN platform_proxies same ON same.id = o.assigned_proxy_id "
                "WHERE same.proxy_url = p.proxy_url AND o.id != a.id "
                "AND o.enabled = true AND o.status != 'checkpoint'))",
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_facebook_accounts_blocked_by_proxy_policy_failed", error=str(exc))
        return set()
    return {key.strip().lower() for row in rows for key in (row["account_id"], row["email"]) if key}


def mark_proxy_rotating(proxy_id: int) -> None:
    try:
        with connect() as conn:
            conn.execute("UPDATE platform_proxies SET ip_type = 'rotating' WHERE id = %s", (proxy_id,))
    except psycopg.Error as exc:
        logger.error("db_mark_proxy_rotating_failed", proxy_id=proxy_id, error=str(exc))


def provider_key_for_proxy_url(proxy_url: str) -> str | None:
    """Nguồn nào sở hữu cổng này (proxy_providers.key qua cột platform_proxies.provider_key),
    None nếu không có (proxy nhập tay) hoặc DB lỗi."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT provider_key FROM platform_proxies WHERE proxy_url = %s AND provider_key IS NOT NULL "
                "ORDER BY id LIMIT 1",
                (proxy_url,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_provider_key_for_proxy_url_failed", proxy_url=proxy_url, error=str(exc))
        return None
    return row["provider_key"] if row is not None else None


def proxy_row_for_url(proxy_url: str) -> ProxyRow | None:
    """Dòng proxy có địa chỉ này (dùng để đo IP đầu ra qua chính cổng khi đổi IP)."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref "
                "FROM platform_proxies WHERE proxy_url = %s ORDER BY id LIMIT 1",
                (proxy_url,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_proxy_row_for_url_failed", proxy_url=proxy_url, error=str(exc))
        return None
    return _proxy_row(row) if row is not None else None


def list_proxies_by_provider(provider_key: str) -> list[ProxyRow]:
    """Mọi dòng proxy thuộc nguồn này - cho clients/proxy_provider.py (cấp cổng kèm IP mới,
    lấy cổng cố định). Không lọc trạng thái: nguồn có cổng thì dùng được cổng đó."""
    try:
        with connect() as conn:
            rows = conn.execute(
                "SELECT id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref "
                "FROM platform_proxies WHERE provider_key = %s ORDER BY id",
                (provider_key,),
            ).fetchall()
    except psycopg.Error as exc:
        logger.error("db_list_proxies_by_provider_failed", provider_key=provider_key, error=str(exc))
        return []
    return [_proxy_row(row) for row in rows]


def accounts_on_proxy_url(proxy_url: str) -> int:
    """Số tài khoản đang bật, không checkpoint, ghim vào dòng platform_proxies có proxy_url này -
    MỌI nền tảng (nguyên tắc 12/11/2026: một proxy thuộc riêng một tài khoản; Meta nối
    Facebook với Threads qua IP nên một proxy của TikTok cũng là một IP). Lỗi DB -> trả số
    lớn (coi như có tài khoản) để chỗ gọi chọn hướng an toàn: không đổi IP."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts pa JOIN platform_proxies pp ON pp.id = pa.assigned_proxy_id "
                "WHERE pp.proxy_url = %s AND pa.enabled = true AND pa.status != 'checkpoint'",
                (proxy_url,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_accounts_on_proxy_url_failed", proxy_url=proxy_url, error=str(exc))
        return 1_000
    return int(row["n"]) if row else 0


def meta_accounts_on_proxy_url(proxy_url: str) -> int:
    """Số tài khoản Facebook / Threads đang bật, không checkpoint, ghim vào dòng platform_proxies có proxy_url này.
    Lỗi DB -> trả số lớn (coi như có tài khoản) để chỗ gọi chọn hướng an toàn: không đổi IP."""
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM platform_accounts pa JOIN platform_proxies pp ON pp.id = pa.assigned_proxy_id "
                "WHERE pp.proxy_url = %s AND pa.platform IN ('facebook', 'threads') AND pa.enabled = true "
                "AND pa.status != 'checkpoint'",
                (proxy_url,),
            ).fetchone()
    except psycopg.Error as exc:
        logger.error("db_meta_accounts_on_proxy_failed", proxy_url=proxy_url, error=str(exc))
        return 1_000
    return int(row["n"]) if row else 0


def pin_account_to_free_proxy(platform: str, account_row_id: int) -> ProxyRow | None:
    """Ghim tài khoản này vào một proxy CHƯA có tài khoản nào khác, và ghi last_used_at trong
    một transaction.

    Nguyên tắc từ 12/11/2026: một proxy thuộc về đúng một tài khoản. Proxy đã có tài khoản
    khác thì không được chọn - tài khoản này phải chờ người vận hành ghim tay trên dashboard
    (hoặc một proxy trống được thêm).

    Tải = số tài khoản (MỌI nền tảng) đang bật, không checkpoint, đã ghim vào proxy. Chỉ
    proxy có tải 0 mới được chọn, nên cảnh báo proxy_over_capacity không còn cần: một proxy
    không bao giờ mang hai tài khoản."""
    try:
        with connect() as conn:
            # Theo endpoint, không theo id: hai dòng proxy cùng địa chỉ là cùng một IP.
            load_sql = (
                "(SELECT count(*) FROM platform_accounts pa JOIN platform_proxies same ON same.id = pa.assigned_proxy_id "
                " WHERE same.proxy_url = pp.proxy_url AND pa.enabled = true AND pa.status != 'checkpoint')"
            )
            meta_filter = {
                "facebook": " AND pp.ip_type = 'static'",
                "threads": " AND pp.ip_type != 'rotating'",
            }.get(platform, "")
            picked = conn.execute(
                f"SELECT pp.id FROM platform_proxies pp "
                "WHERE pp.enabled = true AND pp.platform IN (%s, 'all') "
                f"AND (pp.cooldown_until IS NULL OR pp.cooldown_until <= now()){meta_filter} "
                f"AND {load_sql} = 0 "
                "ORDER BY (pp.platform = 'all') ASC, pp.last_used_at ASC NULLS FIRST "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (platform,),
            ).fetchone()
            if picked is None:
                return None
            row = conn.execute(
                "UPDATE platform_proxies SET last_used_at = now() WHERE id = %s "
                "RETURNING id, platform, proxy_url, username, password, login_use_proxy, ip_type, provider_key, provider_ref",
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
            "db_pin_account_to_free_proxy_failed",
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
        "ip_type": row.get("ip_type", "unknown"),
        "provider_key": row.get("provider_key"),
        "provider_ref": row.get("provider_ref"),
    }


class AccountLookupError(RuntimeError):
    """DB lỗi khi tra dòng tài khoản - khác với "không có dòng đó": không biết proxy đã ghim thì không được đoán."""


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
    services/pool.acquire_proxy_for_account, nơi tự ghim ở lần dùng đầu. DB lỗi thì raise AccountLookupError."""
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
        raise AccountLookupError(f"{platform}: cannot look up account {account_key!r}") from exc
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
