"""Bounded Relay discovery using the pinned facebook-gql-scraper V8 runtime.

Only JavaScript module factories run here. No account data, Python callbacks, browser,
filesystem or network APIs are exposed to JavaScript; HTTP stays with the caller.
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
    # Bootloader rsrcMap may include extensionless bundles and escaped URL slashes.
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
                # Wait for V8's cancelled task callback before closing its asyncio loop.
                # mini-racer 0.14.1 can otherwise emit a late "event loop is closed" callback.
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
            # Browser-only top-level code can fail after registering useful modules.
            # Timeouts and OOM deliberately propagate; never continue that isolate.
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
        """None means this isolate must not run anything else; the caller keeps what it already found."""
        from py_mini_racer import JSEvalException

        try:
            return self.add(source)
        except ValueError:
            return {}
        except JSEvalException:
            return None


class SourceUnavailable(Exception):
    """One page or bundle could not be read; this says nothing about the account."""


def discover(fetch, pages: list[str], targets: set[str], *, deadline: float | None = None) -> dict[str, str]:
    """Account restrictions propagate. Running out of time or V8 stops early and keeps the IDs found so far."""
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
