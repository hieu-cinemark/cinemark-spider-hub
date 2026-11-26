"""Video theo hashtag (youtube.com/hashtag/<tên>) qua InnerTube browse, không đăng nhập (26/11/2026).

POST /youtubei/v1/browse với browseId "FEhashtag" và `params` = protobuf {93: {1: tên, 3: tab}} (base64). Trang hashtag có 2 tab,
không trùng video nhau, phải lấy cả hai:
- "all" (tab=1): video thường (videoRenderer: tiêu đề, kênh, thời gian tương đối, view, thời lượng).
- "shorts" (tab=3): Shorts (shortsLockupViewModel: chỉ tiêu đề và view làm tròn - kênh, ngày đăng lấy ở API chi tiết).
Đo 26/11/2026: YouTube không trả hết số video nó báo (#traigiamhanhphuc báo 527, lấy được ~228). Hashtag nhỏ tab "all" chỉ có
một trang; tab Shorts lật bằng continuation.
"""

import base64

from social_crawler.spiders.youtube.client import (
    create_httpx_client,
    get_context,
    get_youtube_api_key,
    get_youtube_api_url,
)
from social_crawler.spiders.youtube.config import ENDPOINT_BROWSE, YOUTUBE_BASE_URL
from social_crawler.spiders.youtube.headers import get_youtube_headers
from social_crawler.spiders.youtube.search.search import extract_shorts

from ..shared.parsers import extract_continuation_token, parse_video_renderer

TABS = {"all": 1, "shorts": 3}


def _varint(value: int) -> bytes:
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def hashtag_name(keyword: str) -> str:
    """ "#BoSuaBay " -> "bosuabay" (tên hashtag không phân biệt hoa thường, không có khoảng trắng)."""
    return "".join(keyword.split()).lstrip("#").lower()


def hashtag_params(keyword: str, tab: str) -> str:
    name = hashtag_name(keyword).encode()
    inner = b"\x0a" + _varint(len(name)) + name + b"\x18" + _varint(TABS[tab])
    # Field 93, kiểu length-delimited: tag = (93 << 3) | 2.
    return base64.b64encode(_varint((93 << 3) | 2) + _varint(len(inner)) + inner).decode()


def _walk(node, key: str):
    if isinstance(node, dict):
        for k, value in node.items():
            if k == key:
                yield value
            yield from _walk(value, key)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value, key)


def page_videos(data: dict) -> list[dict]:
    """Video trong một trang (cả videoRenderer lẫn Shorts), cùng dạng với kết quả tìm kiếm (search.py)."""
    videos = []
    for renderer in _walk(data, "videoRenderer"):
        if not isinstance(renderer, dict) or not renderer.get("videoId"):
            continue
        parsed = parse_video_renderer(renderer)
        snippet = (renderer.get("detailedMetadataSnippets") or [{}])[0].get("snippetText", {}).get("runs", [])
        parsed["description_snippet"] = "".join(run.get("text", "") for run in snippet)
        parsed["is_short"] = False
        videos.append(parsed)
    seen: set[str] = set()
    return [v for v in videos + extract_shorts(data) if not (v["video_id"] in seen or seen.add(v["video_id"]))]


def next_token(data: dict) -> str | None:
    """Token trang kế: continuationItemRenderer cuối cùng của danh sách video."""
    token = None
    for item in _walk(data, "continuationItemRenderer"):
        token = extract_continuation_token({"continuationItemRenderer": item}) or token
    return token


async def hashtag_pages(keyword: str, *, tab: str, max_pages: int = 20, proxy: str | None = None):
    """Lật từng trang một tab của hashtag, yield (số trang, danh sách video) để chỗ gọi dừng sớm khi toàn video đã thấy.
    max_pages=0 là không giới hạn (tới khi YouTube hết trang)."""
    name = hashtag_name(keyword)
    if not name:
        return
    api_key = await get_youtube_api_key(proxy=proxy)
    url = get_youtube_api_url(ENDPOINT_BROWSE, api_key)
    page_url = f"{YOUTUBE_BASE_URL}/hashtag/{name}"
    async with create_httpx_client(proxy=proxy, headers=get_youtube_headers()) as client:
        body = {"browseId": "FEhashtag", "params": hashtag_params(name, tab)}
        page = 0
        shown: set[str] = set()
        while True:
            resp = await client.post(url, json={"context": get_context(page_url), **body})
            resp.raise_for_status()
            data = resp.json()
            page += 1
            videos = [v for v in page_videos(data) if v["video_id"] not in shown]
            shown.update(v["video_id"] for v in videos)
            yield page, videos
            token = next_token(data)
            # Trang không có gì mới mà vẫn còn token là YouTube lặp trang: dừng để không lật vô hạn.
            if not token or not videos or (max_pages and page >= max_pages):
                return
            body = {"continuation": token}
