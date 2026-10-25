"""Test tải cho job post_metrics (cập nhật like/comment/share bài Facebook KHÔNG đăng nhập): gọi lại link bài đã biết
bằng đúng code của job (spiders/facebook/features/post_metrics), qua một proxy trong platform_proxies, để tìm ngưỡng bị
chặn (sau bao nhiêu request, chặn trông thế nào).

Chỉ đọc - không publish Kafka, không ghi DB, không đặt cooldown. Mặc định từ chối proxy đang chở tài khoản
Facebook/Threads (Meta nối tài khoản theo IP - IP bị đánh dấu thì vạ lây tài khoản):

    .venv/bin/python -m scripts.probe_fb_logged_out --proxy-id 10
    .venv/bin/python -m scripts.probe_fb_logged_out --proxy-id 10 --limit 50 --delay 2
    .venv/bin/python -m scripts.probe_fb_logged_out --proxy-id 0           # IP máy đang chạy, không proxy

--urls: file JSON (list {url, like_count, reply_count} - xuất từ bảng posts của D1) hoặc file text mỗi dòng một URL.
Log CSV + HTML của request không có số nằm ở output/fb_logged_out/.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import time
from collections import Counter
from pathlib import Path

from social_crawler.spiders.facebook.features.post_metrics.post_metrics import BLOCKED_STATUSES, fetch_post_metrics

OUT_DIR = Path("output/fb_logged_out")
DEFAULT_URLS = Path("output/fb_logged_out_samples.json")


def _proxy(proxy_id: int, allow_meta_proxy: bool) -> str | None:
    if not proxy_id:
        return None
    from social_crawler.db.proxies import get_proxy_by_id, meta_accounts_on_proxy_url
    from social_crawler.services.pool import build_proxy_url

    row = get_proxy_by_id(proxy_id)
    if row is None:
        raise SystemExit(f"proxy #{proxy_id} không tồn tại, đang tắt hoặc đang cooldown")
    meta_accounts = meta_accounts_on_proxy_url(row["url"])
    if meta_accounts and not allow_meta_proxy:
        raise SystemExit(
            f"proxy #{proxy_id} đang chở {meta_accounts} tài khoản Facebook/Threads - chọn proxy khác "
            "(hoặc --allow-meta-proxy nếu chấp nhận rủi ro)"
        )
    return build_proxy_url(row)


def _load_urls(path: Path) -> list[dict]:
    text = path.read_text()
    if path.suffix == ".json":
        return json.loads(text)
    return [{"url": line.strip()} for line in text.splitlines() if line.strip()]


def _kind(url: str) -> str:
    for kind, marker in (("reel", "/reel/"), ("video", "/videos/"), ("group", "/groups/"), ("post", "/posts/")):
        if marker in url:
            return kind
    return "permalink"


def probe(rows: list[dict], proxy: str | None, delay: float, jitter: float, stop_after: int, label: str) -> None:
    (OUT_DIR / "html").mkdir(parents=True, exist_ok=True)
    log_path = OUT_DIR / f"{time.strftime('%Y%m%d-%H%M%S')}-{label}.csv"
    print(f"{len(rows)} bài, {label}, delay {delay}±{jitter}s, log {log_path}\n", flush=True)

    stats: Counter[str] = Counter()
    versus_db: Counter[str] = Counter()
    total_bytes = 0
    streak = 0
    first_block = None
    started = time.monotonic()
    with log_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["i", "t_sec", "kind", "status", "http", "ms", "bytes", "like", "comment", "share",
                         "db_like", "db_comment", "url"])  # fmt: skip
        for i, row in enumerate(rows):
            url = row["url"]
            t0 = time.monotonic()
            result = fetch_post_metrics(url, proxy)
            ms = int((time.monotonic() - t0) * 1000)
            metrics = result.metrics
            stats[result.status] += 1
            total_bytes += len(result.html)
            db_like = row.get("like_count")
            if result.status == "ok":
                streak = 0
                if db_like is not None:
                    like = metrics["like"] or 0
                    versus_db["equal" if like == db_like else "grew" if like > db_like else "lower"] += 1
            else:
                (OUT_DIR / "html" / f"{label}-{i:03d}-{result.status}.html").write_text(result.html)
                if result.status in BLOCKED_STATUSES:
                    streak += 1
                    if first_block is None:
                        first_block = i
            elapsed = int(time.monotonic() - started)
            writer.writerow([i, elapsed, _kind(url), result.status, result.http_status, ms, len(result.html),
                             metrics.get("like"), metrics.get("comment"), metrics.get("share"), db_like,
                             row.get("reply_count"), url])  # fmt: skip
            f.flush()
            print(
                f"#{i:03d} {elapsed:4d}s {_kind(url):9s} {result.status:15s} {ms:5d}ms {len(result.html) // 1024:5d}KB "
                f"like {metrics.get('like')}/{db_like} cmt {metrics.get('comment')}/{row.get('reply_count')} "
                f"share {metrics.get('share')}",
                flush=True,
            )
            if streak >= stop_after:
                print(f"\n>>> Dừng: {streak} request bị chặn liên tiếp", flush=True)
                break
            time.sleep(max(0.0, delay + random.uniform(-jitter, jitter)))

    elapsed = int(time.monotonic() - started)
    done = sum(stats.values())
    print("\n===== TỔNG KẾT =====")
    print(
        f"{done} request trong {elapsed}s (~{done * 60 / max(elapsed, 1):.1f}/phút), {total_bytes / 1e6:.1f} MB "
        f"(~{total_bytes / max(done, 1) / 1e3:.0f} KB/bài)"
    )
    print("trạng thái:", dict(stats))
    print("like so với D1:", dict(versus_db))
    print("request bị chặn đầu tiên:", first_block if first_block is not None else "không có")
    print("log:", log_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--proxy-id", type=int, default=10, help="id trong platform_proxies; 0 = không proxy")
    parser.add_argument("--urls", type=Path, default=DEFAULT_URLS)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--delay", type=float, default=4.0)
    parser.add_argument("--jitter", type=float, default=2.0)
    parser.add_argument("--stop-after", type=int, default=5, help="dừng sau N request bị chặn liên tiếp")
    parser.add_argument("--allow-meta-proxy", action="store_true")
    args = parser.parse_args()

    proxy = _proxy(args.proxy_id, args.allow_meta_proxy)
    rows = _load_urls(args.urls)[: args.limit]
    label = f"proxy{args.proxy_id}" if args.proxy_id else "local"
    probe(rows, proxy, args.delay, args.jitter, args.stop_after, label)


if __name__ == "__main__":
    main()
