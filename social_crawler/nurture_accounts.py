"""Đường dẫn cũ, chỉ giữ tạm để unit systemd/cron trên VPS chưa sửa vẫn chạy - dùng `python -m social_crawler.accounts.nurture`.

Xoá file này khi mọi nơi gọi đã chuyển sang đường dẫn mới.
"""

from social_crawler.accounts.nurture import main

if __name__ == "__main__":
    raise SystemExit(main())
