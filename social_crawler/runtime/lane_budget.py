"""Chia tổng số làn cho các nhóm cào chạy song song (proxy_settings: lanes_total, cài ở dashboard).

Bốn nhóm chia đều khi để 0 ("tự chia"): YouTube, đọc page Facebook không đăng nhập, kênh Threads không đăng nhập và TikTok.
Mặc định lanes_total = 16 nên mỗi nhóm 4 làn. Nhóm nào đặt số riêng (> 0) thì dùng số đó, không bị ảnh hưởng bởi tổng.
TikTok và Facebook có tài khoản không nằm trong phép chia này: làn của chúng gắn với cổng proxy / tài khoản và chỉ đổi khi
khởi động (TIKTOK_LANES, FACEBOOK_LANES trong runtime/lanes.py)."""

from __future__ import annotations

GROUPS = 4


def share(total: int) -> int:
    """Số làn của một nhóm khi để tự chia."""
    return max(1, int(total) // GROUPS)


def resolve(explicit: int, total: int) -> int:
    """`explicit` > 0: số người dùng đặt cho riêng nhóm; 0: phần chia đều của `total`."""
    return int(explicit) if int(explicit) > 0 else share(total)
