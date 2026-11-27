"""So vân tay mạng giữa hai máy (Mac / VPS): gửi MỘT request qua cùng một proxy tới tls.peet.ws, in vân tay TLS (JA3/JA4),
HTTP/2 (Akamai), thứ tự header thực tế server nhận được, cùng thông tin máy (hệ điều hành, múi giờ, phiên bản). Chạy y hệt trên
hai máy rồi so hai file kết quả - khác nhau ở đâu thì Facebook cũng thấy khác ở đó.

Chỉ đọc: không dùng tài khoản, không chạm Facebook, không ghi DB. Dùng đúng profile giả lập của spider cho một phiên Chrome Mac.

    .venv/bin/python -m scripts.probe_tls_fingerprint --proxy-port 8159
    .venv/bin/python -m scripts.probe_tls_fingerprint --proxy-port 8159 --target chrome152_macos --out output/tls_probe_vps.json

Kết quả ở output/tls_probe_<tên máy>.json. So hai file: `diff <(jq -S . mac.json) <(jq -S . vps.json)`.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import time
from pathlib import Path

import curl_cffi
from curl_cffi import requests as cr

from social_crawler.core import env  # noqa: F401 - nạp .env
from social_crawler.db.script_connect import connect
from social_crawler.runtime.pool import build_proxy_url

PROBE_URL = "https://tls.peet.ws/api/all"
# UA đúng dạng phiên Chrome 152 trên Mac mà các tài khoản đang dùng (xem docs/facebook-accounts.md).
MAC_CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/152.0.0.0 Safari/537.36"
)


def proxy_for_port(port: int) -> str:
    with connect() as conn:
        row = conn.execute(
            "SELECT proxy_url, username, password FROM platform_proxies WHERE platform = 'facebook' AND proxy_url LIKE %s",
            (f"%:{port}",),
        ).fetchone()
    if row is None:
        raise SystemExit(f"Không có proxy Facebook nào có cổng {port}")
    return build_proxy_url({"url": row["proxy_url"], "username": row["username"], "password": row["password"]})


def main(argv: list[str] | None = None) -> int:
    """Gửi một request dò qua proxy của cổng đã chọn, rút gọn vân tay nhận được kèm thông tin máy, rồi ghi ra file JSON."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--proxy-port", type=int, required=True)
    parser.add_argument("--target", default="chrome152_macos", help="profile curl_cffi (mặc định: như spider với UA Mac)")
    parser.add_argument("--out", help="file kết quả (mặc định output/tls_probe_<tên máy>.json)")
    args = parser.parse_args(argv)

    proxy = proxy_for_port(args.proxy_port)
    response = cr.get(
        PROBE_URL,
        impersonate=args.target,
        headers={"user-agent": MAC_CHROME_UA},
        proxies={"http": proxy, "https": proxy},
        timeout=30,
    )
    data = response.json()
    http2 = data.get("http2") or {}
    tls = data.get("tls") or {}
    result = {
        "machine": {
            "host": socket.gethostname(),
            "os": platform.platform(),
            "python": platform.python_version(),
            "curl_cffi": curl_cffi.__version__,
            "libcurl": curl_cffi.__curl_version__,
            "timezone": time.tzname,
            "utc_offset_hours": -time.timezone / 3600,
        },
        "target": args.target,
        "exit_ip": data.get("ip"),
        "http_version": data.get("http_version"),
        "ja3_hash": tls.get("ja3_hash"),
        "ja4": tls.get("ja4"),
        "peetprint_hash": tls.get("peetprint_hash"),
        "akamai_hash": http2.get("akamai_fingerprint_hash"),
        "akamai": http2.get("akamai_fingerprint"),
        "header_order": [
            line.split(":", 1)[0] for line in (http2.get("sent_frames") or [{}])[-1].get("headers", []) if ":" in line
        ],
        "user_agent_seen": data.get("user_agent"),
    }
    out = Path(args.out or f"output/tls_probe_{result['machine']['host']}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\nĐã lưu {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
