"""platform_proxies: chọn proxy và sổ sách sức khoẻ/cooldown. Proxy chia theo nền tảng (cột platform), job có tài khoản luân
phiên trong danh sách của nền tảng (runtime/proxy_lease.py). Không còn ghim tài khoản<->proxy (assigned_proxy_id) từ 13/11/2026;
từ 18/11/2026 proxy tự đổi IP mỗi 60 giây."""

from __future__ import annotations

from typing import Any, TypedDict

from social_crawler.core.logger import get_logger
from social_crawler.db import d1, d1_pool

logger = get_logger(__name__)


class ProxyRow(TypedDict):
    id: int
    url: str
    username: str
    password: str
    login_use_proxy: bool
    # Nền tảng của dòng proxy: proxy gán cho nền tảng nào thì chỉ nền tảng đó dùng.
    platform: str
    # Gói nhà cung cấp sở hữu dòng proxy này (proxy_providers.key), None = proxy nhập
    # tay/tĩnh. Trước khi có cột này, code đoán nguồn bằng cách so proxy_url với
    # host:port trong token của từng gói.
    provider_key: str | None
    # Mã riêng của cổng này trong hệ thống nhà cung cấp (vd proxy_id của webtui, key của
    # cổng ở zingproxy). Được điền vào placeholder {ref} của link reset.
    provider_ref: str | None


def list_proxies(platform: str) -> list[ProxyRow]:
    """Mọi dòng proxy đang bật của nền tảng, bất kể
    trạng thái cooldown - khác với claim_proxy, vốn cố ý giấu dòng đang cooldown
    vì chúng đang chọn một proxy để dùng ngay. Dành cho lượt ping sức khoẻ định kỳ (xem
    proxy_health_check.py) muốn thử mọi proxy đã cấu hình, kể cả proxy đang cooldown, để
    một lần hồi phục thật xoá cooldown ngay qua record_proxy_outcome(success=True) thay vì
    chờ hết hẹn giờ mà không có bằng chứng nó đã sống lại."""
    return d1_pool.list_proxies(platform)


def claim_proxy(platform: str) -> ProxyRow | None:
    """Chọn nguyên tử proxy dùng được LRU của nền tảng và ghi last_used_at trong cùng một transaction. Cùng lý do SKIP LOCKED
    như claim_account - phương án dự phòng acquire_proxy() không ghim trước đây SELECT rồi
    UPDATE trên hai connection, nên hai chỗ gọi có thể cùng lấy một proxy."""
    return d1_pool.claim_proxy(platform)


def list_proxies_without_meta_accounts() -> list[ProxyRow]:
    """Proxy đang bật, ngoài cooldown, KHÔNG phải đường ra của tài khoản Facebook/Threads - cho việc gọi Facebook không đăng
    nhập: Meta nối tài khoản theo IP, IP bị chặn thì vạ lây tài khoản trên nó. Proxy của tài khoản Meta là mọi dòng có
    platform facebook/threads (luân phiên, không ghim); dòng khác trỏ cùng cổng với một dòng như vậy cũng bị loại."""
    return d1_pool.list_proxies_without_meta_accounts()


def list_public_page_proxies() -> list[ProxyRow]:
    """Proxy nền tảng "public" đang bật, không cooldown - đọc page Facebook / kênh Threads không tài khoản (13/11/2026: không
    còn ghim tài khoản nên không còn lọc theo tài khoản)."""
    return d1_pool.list_public_page_proxies()


def provider_key_for_proxy_url(proxy_url: str) -> str | None:
    """Nguồn nào sở hữu cổng này (proxy_providers.key qua cột platform_proxies.provider_key),
    None nếu không có (proxy nhập tay) hoặc DB lỗi."""
    return d1_pool.provider_key_for_proxy_url(proxy_url)


def proxy_row_for_url(proxy_url: str) -> ProxyRow | None:
    """Dòng proxy có địa chỉ này (dùng để đo IP đầu ra qua chính cổng khi đổi IP)."""
    return d1_pool.proxy_row_for_url(proxy_url)


def list_proxies_by_provider(provider_key: str) -> list[ProxyRow]:
    """Mọi dòng proxy thuộc nguồn này - cho clients/proxy_provider.py (cấp cổng kèm IP mới,
    lấy cổng cố định). Không lọc trạng thái: nguồn có cổng thì dùng được cổng đó."""
    return d1_pool.list_proxies_by_provider(provider_key)


def proxy_row(row: dict[str, Any]) -> ProxyRow:
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


def platform_has_any_proxy(platform: str) -> bool:
    """Nền tảng có ít nhất một dòng platform_proxies nào hay không, bất kể sức khoẻ - cho services/pool.acquire_proxy_for_account(
    required=True) phân biệt "nền tảng này đã cấu hình proxy nhưng hiện không cái nào dùng
    được" (phải lỗi rõ ràng thay vì âm thầm chạy không proxy - xem hàm đó) với "nền tảng này
    chưa bao giờ dùng proxy" (chạy không proxy là ổn, không đổi so với trước khi có ghim cố
    định)."""
    return d1_pool.platform_has_any_proxy(platform)


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
        if success:
            before = d1_pool.proxy_failures(platform, proxy_url)
            d1_pool.record_proxy_success(platform, proxy_url)
            if before:
                logger.info(
                    "proxy_recovered", platform=platform, proxy_url=proxy_url, previous_consecutive_failures=before
                )
        else:
            from social_crawler.db.proxy_settings import get_setting

            row = d1_pool.record_proxy_failure(
                platform,
                proxy_url,
                float(get_setting("cooldown_base_minutes")),
                float(get_setting("cooldown_max_minutes")),
            )
            if row is not None:
                logger.warning(
                    "proxy_degraded",
                    platform=platform,
                    proxy_url=proxy_url,
                    consecutive_failures=row["consecutive_failures"],
                    cooldown_until=str(row["cooldown_until"]),
                )
    except d1.D1Error as exc:
        logger.error(
            "db_record_proxy_outcome_failed",
            platform=platform,
            proxy_url=proxy_url,
            success=success,
            error=exc,
        )
