"""Đường dẫn cũ, chỉ giữ tạm để unit systemd/cron trên VPS chưa sửa vẫn chạy - dùng `python -m social_crawler.health.proxy_health_check`.

Xoá file này khi mọi nơi gọi đã chuyển sang đường dẫn mới.
"""

import sys

from social_crawler.health.proxy_health_check import run

if __name__ == "__main__":
    run()
    sys.exit(0)
