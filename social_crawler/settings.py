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
# USER_AGENT = "social_crawler (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling settings
# CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

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
Proxy and account credentials used to live here as env vars
(PROXY_URL/PROXY_USERNAME/PROXY_PASSWORD/LOGIN_USE_PROXY,
FACEBOOK_ACCOUNTS, INSTAGRAM_ACCOUNTS) - they moved to Supabase Postgres
(platform_accounts/platform_proxies tables, see services/db.py) because
they change often enough (accounts swapped/disabled, proxies rotated) that
editing .env and restarting every process that reads it stopped being
acceptable. get_accounts(platform)/get_proxy(platform) there query fresh on
every call, no caching, no restart needed after an edit. This module has no
PROXY_*/FACEBOOK_ACCOUNTS/INSTAGRAM_ACCOUNTS variables of its own anymore -
see accounts.py under each platform's auth/ package for how they're used."""

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
