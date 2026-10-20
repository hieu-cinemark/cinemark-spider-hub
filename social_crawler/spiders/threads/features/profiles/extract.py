"""Parse response query hovercard hồ sơ Threads (BarcelonaUsernameHovercardImplDirectQuery) - xem
ThreadsGraphQLClient.fetch_profile. Dạng đã xác nhận với response thật bắt từ trình duyệt (20/10/2026):

    {"data": {"user": {"pk": "...", "username": "...", "full_name": "...", "follower_count": 80,
                       "is_verified": false, ...}}, "extensions": {...}}

Người dùng không tồn tại: data.user = null."""

from __future__ import annotations

from typing import Any


class ProfileQueryError(RuntimeError):
    """Response không có data.user (kể cả null) - thường là doc_id đã hết hạn (Threads trả errors) hoặc bị chặn. Khác
    "không tìm thấy": chỗ gọi phải dừng và trả handle về hàng đợi, không ghi kết quả."""


def extract_profile(response: Any) -> dict[str, Any] | None:
    """{username, pk, full_name, follower_count, is_verified} hoặc None khi không có người dùng này."""
    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, dict) or "user" not in data:
        errors = response.get("errors") if isinstance(response, dict) else None
        raise ProfileQueryError(f"Threads profile response has no data.user: {str(errors or response)[:300]}")
    user = data.get("user")
    if user is None:
        return None
    if not isinstance(user, dict):
        raise ProfileQueryError(f"Threads profile data.user is not an object: {str(user)[:300]}")
    followers = user.get("follower_count")
    try:
        follower_count = int(followers) if followers is not None else None
    except TypeError, ValueError:
        follower_count = None
    return {
        "username": user.get("username"),
        "pk": str(user.get("pk") or user.get("id") or "") or None,
        "full_name": user.get("full_name") or None,
        "follower_count": follower_count,
        "is_verified": bool(user.get("is_verified")),
    }
