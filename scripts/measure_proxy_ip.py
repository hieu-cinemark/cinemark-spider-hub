"""Đo IP thoát của một proxy nhiều lần để biết nó tĩnh hay tự xoay. Chỉ gọi api.ipify.org, không đụng Facebook hay tài khoản.

Facebook chỉ chạy trên proxy có nhãn static, nên đây là bước trước khi ghim tài khoản. --write ghi nhãn đo được vào DB.

    .venv/bin/python -m scripts.measure_proxy_ip 18
    .venv/bin/python -m scripts.measure_proxy_ip 18 --samples 7 --interval 300 --write
"""

from __future__ import annotations

import argparse
import time

from curl_cffi import requests as cr

from social_crawler.core import env  # noqa: F401 - nạp .env
from social_crawler.db.connection import connect
from social_crawler.runtime.pool import build_proxy_url


def classify(ips: list[str | None]) -> str | None:
    """None khi không đủ dữ liệu để kết luận: một mẫu lỗi không chứng minh IP đứng yên."""
    seen = [ip for ip in ips if ip]
    if len(set(seen)) > 1:
        return "rotating"
    return "static" if seen and len(seen) == len(ips) else None


def sample(proxy_url: str) -> str | None:
    with cr.Session(proxies={"http": proxy_url, "https": proxy_url}, trust_env=False) as session:
        try:
            return session.get("https://api.ipify.org", timeout=20).text.strip() or None
        except cr.RequestsError:
            return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("proxy_id", type=int)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--interval", type=int, default=300, help="số giây giữa hai mẫu")
    parser.add_argument("--write", action="store_true", help="ghi nhãn đo được vào platform_proxies.ip_type")
    args = parser.parse_args(argv)
    with connect() as conn:
        row = conn.execute(
            "SELECT proxy_url, username, password FROM platform_proxies WHERE id = %s", (args.proxy_id,)
        ).fetchone()
    if row is None:
        parser.error(f"Không có proxy #{args.proxy_id}")
    url = build_proxy_url({"url": row["proxy_url"], "username": row["username"], "password": row["password"]})
    ips: list[str | None] = []
    for index in range(args.samples):
        ips.append(sample(url))
        print(time.strftime("%H:%M:%S"), ips[-1] or "không đo được", flush=True)
        if index < args.samples - 1:
            time.sleep(args.interval)
    label = classify(ips)
    print(f"Proxy #{args.proxy_id}: {label or 'không kết luận được, không ghi nhãn'}")
    if args.write and label:
        with connect() as conn:
            conn.execute("UPDATE platform_proxies SET ip_type = %s WHERE id = %s", (label, args.proxy_id))
        print(f"Đã ghi ip_type = {label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
