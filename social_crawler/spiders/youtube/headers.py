import random

from social_crawler.spiders.youtube.config import CLIENT_VERSION, YOUTUBE_BASE_URL

_IMPERSONATE_POOL = ["chrome131", "chrome130", "chrome129", "edge131"]
_DEFAULT_ACCEPT_LANGUAGE = "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"


def get_youtube_headers(visitor_data: str | None = None, client_version: str | None = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Language": _DEFAULT_ACCEPT_LANGUAGE,
        "Origin": YOUTUBE_BASE_URL,
        "Referer": f"{YOUTUBE_BASE_URL}/",
        "Connection": "keep-alive",
        "X-Youtube-Client-Name": "1",
        "X-Youtube-Client-Version": client_version or CLIENT_VERSION,
    }
    if visitor_data:
        headers["X-Goog-Visitor-Id"] = visitor_data
    # data-miner gửi kèm "_impersonate" làm header thật (httpx không hiểu khoá này); bỏ để request sạch.
    headers["User-Agent"] = (
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{random.choice(_IMPERSONATE_POOL)[-3:]}.0.0.0 Safari/537.36"
    )
    return headers
