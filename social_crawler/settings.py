# Cấu hình Scrapy cho project social_crawler
#
# Cho đơn giản, file này chỉ chứa các setting được coi là quan trọng hoặc hay dùng. Xem
# thêm các setting khác trong tài liệu:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

import social_crawler.env  # noqa: F401 # nạp .env đúng một lần, dù bao nhiêu module import nó

BOT_NAME = "social_crawler"

SPIDER_MODULES = ["social_crawler.spiders"]
NEWSPIDER_MODULE = "social_crawler.spiders"

ADDONS = {}

# Mọi spider ở đây gọi thẳng curl_cffi thay vì đi qua downloader của Scrapy (xem docstring
# của từng spider để biết lý do), nên cái này chỉ để `asyncio.to_thread()` bên trong các
# method async `start()` của chúng có event loop đang chạy để gắn vào.
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


# Crawl có trách nhiệm bằng cách tự định danh (và website của bạn) trong user-agent
# USER_AGENT = "social_crawler (+http://www.yourdomain.com)"

# Tuân theo quy tắc robots.txt
ROBOTSTXT_OBEY = True

# Cấu hình đồng thời và giới hạn tốc độ
# CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Tắt cookie (mặc định bật)
# COOKIES_ENABLED = False

# Tắt Telnet Console (mặc định bật)
# TELNETCONSOLE_ENABLED = False

# Ghi đè header request mặc định:
# DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
# }

# Bật hoặc tắt extension
# Xem https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Bật và cấu hình extension AutoThrottle (mặc định tắt)
# Xem https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# Độ trễ tải ban đầu
# AUTOTHROTTLE_START_DELAY = 5
# Độ trễ tải tối đa khi độ trễ mạng cao
# AUTOTHROTTLE_MAX_DELAY = 60
# Số request trung bình Scrapy nên gửi song song tới mỗi server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Bật hiển thị thống kê giới hạn tốc độ cho mỗi response nhận được:
# AUTOTHROTTLE_DEBUG = False

# Bật và cấu hình cache HTTP (mặc định tắt)
# Xem https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Đặt các setting có giá trị mặc định đã lỗi thời sang giá trị dùng được về sau
FEED_EXPORT_ENCODING = "utf-8"

# Output khi test local: mọi lần chạy `scrapy crawl <name>` ghi các item đã crawl ra
# output/<spider_name>_<timestamp>.json mà không cần -o.
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
(platform_accounts/platform_proxies tables, see db/) because
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
clients/telegram.py. Create a bot via @BotFather (send it /newbot, copy the
token it gives you), then message your new bot once and open
https://api.telegram.org/bot<TOKEN>/getUpdates to read back your chat id
from the response. Leave both unset to disable - every send is a no-op then,
never an error."""
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", None)
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", None)
