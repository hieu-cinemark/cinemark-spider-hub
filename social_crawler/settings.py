# Scrapy settings for social_crawler project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

import social_crawler.env  # noqa: F401  # loads .env exactly once, however many modules import it
from social_crawler.services.db_settings import load_settings

# One connection, one query, for every DB-backed setting this module reads -
# values here win over the matching env var when present; see _setting()
# below. Falls back to {} (every _setting() call then reads .env instead)
# if DATABASE_URL isn't set yet or the DB is briefly unreachable.
_DB_SETTINGS = load_settings(
    [
        "facebook_proxy_url",
        "facebook_proxy_username",
        "facebook_proxy_password",
        "facebook_login_use_proxy",
        "api_direct_url",
        "api_direct_token",
    ]
)


def _setting(db_key: str, env_key: str, default: str | None = None) -> str | None:
    """The `settings` table wins when it has this key; otherwise falls back
    to the env var, then `default` - lets a server without DB access (or
    before these are ever seeded there - see seed_settings_from_env.py)
    keep working from .env alone."""
    if db_key in _DB_SETTINGS:
        return _DB_SETTINGS[db_key]
    return os.getenv(env_key, default)

BOT_NAME = "social_crawler"

SPIDER_MODULES = ["social_crawler.spiders"]
NEWSPIDER_MODULE = "social_crawler.spiders"

ADDONS = {}

# Every spider here calls curl_cffi directly instead of going through
# Scrapy's downloader (see each spider's own docstring for why), so this is
# only for `asyncio.to_thread()` inside their async `start()` methods to
# have a running event loop to attach to.
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "social_crawler (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
#CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# Local test output: every `scrapy crawl <name>` run writes its scraped
# items to output/<spider_name>_<timestamp>.json without needing -o.
FEEDS = {
    "output/%(name)s_%(time)s.json": {
        "format": "json",
        "encoding": "utf-8",
        "indent": 2,
        "overwrite": False,
    },
}

"""
Facebook GraphQL API proxy settings - optional, only needed if you want to
route requests through a proxy server (e.g. to avoid IP-based rate limits or blocks). Set these in your .env file or environment variables:
FACEBOOK_PROXY_URL=http://<proxy_host>:<proxy_port>/"""
FACEBOOK_PROXY_URL = _setting("facebook_proxy_url", "FACEBOOK_PROXY_URL")
FACEBOOK_PROXY_USERNAME = _setting("facebook_proxy_username", "FACEBOOK_PROXY_USERNAME")
FACEBOOK_PROXY_PASSWORD = _setting("facebook_proxy_password", "FACEBOOK_PROXY_PASSWORD")

"""
Whether bootstrap.py's Playwright login/capture browser should also use the
proxy above - off by default. Login is infrequent/low-volume, so a proxy
buys little there, but a proxy IP that doesn't match the account's usual
geography is exactly what makes Facebook challenge a fresh login with a
captcha loop (confirmed: disabling the proxy dropped it to a single
captcha). The ongoing curl_cffi replay traffic in graphql_client.py is
unaffected by this flag and always uses the proxy above when configured -
that traffic benefits from it (spreading load across IPs) in a way login
doesn't. Set FACEBOOK_LOGIN_USE_PROXY=true to opt back in, e.g. once you
have a proxy whose geography actually matches the account."""
_login_use_proxy_raw = _setting("facebook_login_use_proxy", "FACEBOOK_LOGIN_USE_PROXY", "false")
FACEBOOK_LOGIN_USE_PROXY = str(_login_use_proxy_raw).strip().lower() in ("1", "true", "yes")

"""
Facebook account credentials - optional, only needed to let bootstrap.py log
in automatically instead of pausing for manual login. Sourced from the
`settings` table (key facebook_accounts) first, falling back to the
FACEBOOK_ACCOUNTS env var - a JSON array of objects, one named key per
credential (not a "|"-joined string - too easy to get a field out of
order), e.g.:
FACEBOOK_ACCOUNTS=[{"id": "you@example.com", "password": "hunter2", "2fa": "JBSWY3DPEHPK3PXP", "cookie": "", "token": "", "email": "recovery@example.com"}]
"id" is the login identifier (email/phone/username/uid). "2fa" is a TOTP
secret, used to auto-submit a code if Facebook shows a 2FA prompt - leave ""
if the account doesn't have 2FA enabled. "cookie", if set, is a raw
`Cookie:` header string from an already-logged-in session - when present,
bootstrap.py imports it directly instead of driving a browser through the
login form. "token" is reserved, not currently used. All 6 keys must be
present (use "" for anything the account doesn't have). Parsed by
social_crawler.spiders.facebook.auth.accounts, not here - this module has no
FACEBOOK_ACCOUNTS variable of its own."""

"""
apidirect.io credentials - not currently called anywhere in this codebase
(spider-hub's own Facebook scraping goes through bootstrap.py + curl_cffi,
not a paid reseller API). Read here only so the values have somewhere to
live, sourced from `settings` (keys api_direct_url/api_direct_token) or the
API_DIRECT_URL/API_DIRECT_TOKEN env vars as a fallback."""
API_DIRECT_URL = _setting("api_direct_url", "API_DIRECT_URL")
API_DIRECT_TOKEN = _setting("api_direct_token", "API_DIRECT_TOKEN")

"""
Telegram push notifications - optional, only needed to have every
logger.warning/error (and a few completion milestones, e.g. bootstrap
finishing or a crawl run finishing) also sent to a Telegram chat instead of
only being visible in whatever console is running the crawl - see
services/telegram.py. Create a bot via @BotFather (send it /newbot, copy the
token it gives you), then message your new bot once and open
https://api.telegram.org/bot<TOKEN>/getUpdates to read back your chat id
from the response. Leave both unset to disable - every send is a no-op then,
never an error."""
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", None)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", None)