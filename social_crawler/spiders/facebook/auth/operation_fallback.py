"""Dò Relay có giới hạn bằng runtime V8 facebook-gql-scraper đã ghim phiên bản.

Ở đây chỉ chạy các factory module JavaScript. Không đưa dữ liệu tài khoản, callback Python, trình duyệt,
API file hay mạng cho JavaScript; HTTP do bên gọi đảm nhận.
"""

from __future__ import annotations

import html
import json
import re
import time
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

MAX_BYTES = 5_000_000
MAX_ASSETS = 30
DISCOVERY_SECONDS = 75
V8_SECONDS = 2
V8_MEMORY = 96 * 1024 * 1024
SUFFIX = "_facebookRelayOperation"


def allowed_url(url: str, *, asset: bool = False) -> bool:
    try:
        parsed = urlsplit(url)
        return bool(
            parsed.scheme == "https"
            and parsed.port in (None, 443)
            and not parsed.username
            and not parsed.password
            and (
                (parsed.hostname or "").endswith(".fbcdn.net")
                if asset
                else parsed.hostname in {"www.facebook.com", "web.facebook.com", "facebook.com", "m.facebook.com"}
            )
        )
    except ValueError:
        return False


class ScriptParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        if tag == "script" and (src := dict(attrs).get("src")):
            self.urls.append(src)


def script_urls(text: str, page: str) -> list[str]:
    parser = ScriptParser()
    parser.feed(text)
    # rsrcMap của Bootloader có thể có bundle không đuôi file và dấu / bị escape trong URL.
    sources = parser.urls + re.findall(r'"src"\s*:\s*"(https:[^"\s]+)"', text)
    urls = [urljoin(page, html.unescape(src.replace("\\/", "/"))) for src in sources]
    return list(dict.fromkeys(url for url in urls if allowed_url(url, asset=True)))


class V8Operations:
    def __init__(self):
        from facebook_gql_scraper.facebookOperationDiscovery import _RUNTIME
        from py_mini_racer import MiniRacer

        self._runtime = MiniRacer()
        self._interrupted = False
        self._runtime.set_hard_memory_limit(V8_MEMORY)
        self._runtime.eval(
            "setTimeout = undefined; clearTimeout = undefined; __timer_manager = undefined;"
            "globalThis.SharedArrayBuffer = undefined; globalThis.Atomics = undefined;" + _RUNTIME,
            timeout_sec=V8_SECONDS,
        )

    def close(self):
        from py_mini_racer import JSEvalException

        try:
            if self._interrupted:
                # Chờ callback huỷ task của V8 rồi mới đóng vòng lặp asyncio.
                # Không thì mini-racer 0.14.1 có thể bắn callback "event loop is closed" muộn.
                self._runtime.eval("0", timeout_sec=0.2)
        except JSEvalException:
            pass
        finally:
            self._runtime.close()

    def _evaluate(self, code):
        from py_mini_racer import JSOOMException, JSTimeoutException

        try:
            return self._runtime.eval(code, timeout_sec=V8_SECONDS)
        except JSOOMException, JSTimeoutException:
            self._interrupted = True
            raise

    def add(self, source: str) -> dict[str, str]:
        from py_mini_racer import JSEvalException, JSOOMException, JSTimeoutException

        if len(source.encode("utf-8")) > MAX_BYTES:
            raise ValueError("Facebook bundle exceeds discovery limit")
        source = source.split(";/*FB_PKG_DELIM*/", 1)[-1]
        try:
            self._evaluate(source)
        except JSOOMException, JSTimeoutException:
            raise
        except JSEvalException:
            # Code cấp cao chỉ chạy được trên trình duyệt có thể lỗi sau khi đã đăng ký được module hữu ích.
            # Timeout và OOM cố ý được ném lên; không bao giờ chạy tiếp isolate đó.
            pass
        result = self._evaluate(
            """JSON.stringify((function() {
                var out = {};
                Object.keys(__registry).forEach(function(name) {
                    if (!/(_facebookRelayOperation|\\.graphql|\\$Parameters)$/.test(name)) return;
                    try {
                        var value = require(name);
                        var id = typeof value === 'string' ? value : value && value.params && value.params.id;
                        if (typeof id === 'string' && /^\\d{10,}$/.test(id)) {
                            out[name.replace(/(_facebookRelayOperation|\\.graphql|\\$Parameters)$/, '')] = id;
                        }
                    } catch (e) {}
                });
                return out;
            })())""",
        )
        return json.loads(result)

    def collect(self, source: str) -> dict[str, str] | None:
        """None nghĩa là isolate này không được chạy thêm gì; bên gọi giữ những gì đã tìm được."""
        from py_mini_racer import JSEvalException

        try:
            return self.add(source)
        except ValueError:
            return {}
        except JSEvalException:
            return None


class SourceUnavailable(Exception):
    """Một trang hoặc bundle không đọc được; không nói lên gì về tài khoản."""


def discover(fetch, pages: list[str], targets: set[str], *, deadline: float | None = None) -> dict[str, str]:
    """Hạn chế tài khoản được ném lên. Hết giờ hoặc hết V8 thì dừng sớm, giữ các ID đã tìm được."""
    from social_crawler.spiders.facebook.auth.http_refresh import operations_in_script

    deadline = deadline or time.monotonic() + DISCOVERY_SECONDS
    found, assets, seen = {}, [], set()
    runtime = None
    try:
        for page in pages:
            if time.monotonic() >= deadline:
                break
            try:
                source = fetch(page, False, deadline)
            except SourceUnavailable:
                continue
            except TimeoutError:
                break
            found.update(operations_in_script(source))
            assets.extend(script_urls(source, page))
            if targets <= found.keys():
                return found
        for url in assets:
            if url in seen:
                continue
            if len(seen) >= MAX_ASSETS or time.monotonic() >= deadline:
                break
            seen.add(url)
            try:
                source = fetch(url, True, deadline)
            except SourceUnavailable:
                continue
            except TimeoutError:
                break
            found.update(operations_in_script(source))
            if targets <= found.keys():
                return found
            if runtime is None:
                runtime = V8Operations()
            operations = runtime.collect(source)
            if operations is None:
                break
            found.update(operations)
            if targets <= found.keys():
                return found
    finally:
        if runtime is not None:
            runtime.close()
    return found
