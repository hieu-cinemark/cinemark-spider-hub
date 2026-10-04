"""Chọn dòng platform_accounts mà một lần chạy bootstrap đóng vai - query mới từ
Supabase/DB dev local ở mỗi lời gọi (xem social_crawler/db/accounts.py,
services/pool.py), không cache lúc import như biến env FACEBOOK_ACCOUNTS cũ. Tài khoản
được thêm/tắt/xoay ra đủ thường xuyên nên một danh sách cũ trong bộ nhớ sẽ khiến sửa
bảng không có hiệu lực cho tới khi mọi tiến trình sống lâu được restart."""

from __future__ import annotations

from social_crawler.services import pool


def account_key(user: str) -> str:
    """Hậu tố key Redis định danh một tài khoản - email đăng nhập đã chuẩn hoá, để cùng một tài
    khoản luôn ánh xạ tới cùng cache storage_state/token bất kể chữ hoa/khoảng trắng lúc lưu."""
    return user.strip().lower()


def next_account() -> dict[str, str] | None:
    """Tài khoản kế tiếp mà một lần chạy bootstrap đóng vai - xem services/pool.acquire_account
    cho quy tắc chọn (dùng lâu nhất chưa dùng lại trong các tài khoản khoẻ, bỏ qua mọi tài
    khoản đang cooldown hoặc bị checkpoint). None nếu hiện không có dòng facebook đang bật
    nào dùng được - chỗ gọi coi đó là "quay về đăng nhập tay / một slot mặc định duy nhất",
    giống nghĩa của FACEBOOK_ACCOUNTS rỗng trước đây.

    Bootstrap.py phải gọi pool.release_account() kèm kết quả khi lần thử đăng nhập mà tài
    khoản này được chọn thực sự xong - lấy ở đây chỉ đánh dấu nó "đang dùng" (last_used_at),
    chưa biết lần thử có thành công không."""
    return pool.acquire_account("facebook")
