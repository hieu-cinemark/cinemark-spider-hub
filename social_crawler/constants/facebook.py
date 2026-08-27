from __future__ import annotations

from enum import StrEnum

GRAPHQL_URL = "https://www.facebook.com/api/graphql/"


class FacebookEntityType(StrEnum):
    """Facebook GraphQL `__typename` values this project branches on
    directly. Not exhaustive - just the ones with dedicated handling
    somewhere in extract.py."""

    STORY = "Story"
    FEEDBACK = "Feedback"
    PHOTO = "Photo"
    VIDEO = "Video"
    HASHTAG = "Hashtag"


# Facebook's reaction-type node id is a fixed numeric id, the same across
# every account/locale - unlike top_reactions[].node.localized_name, which
# comes back in whatever language the account's own `locale` cookie is set
# to (confirmed: an account with locale=vi_VN got "Thích" instead of "Like").
# Keyed by that id so extract.py can report a consistent English name
# regardless of which account crawled the post. Values confirmed against a
# real captured response for "Like" (1635855486666999) - the rest are
# Facebook's other long-standing standard reaction types, same ids used
# since Reactions launched.
REACTION_ID_TO_NAME = {
    "1635855486666999": "like",
    "1678524932434102": "love",
    "115940658764963": "haha",
    "478547315650144": "wow",
    "908563459236466": "sad",
    "444813342392137": "angry",
    "613557422527858": "care",
}


# --- Redis keys
# Every login-session-scoped key is templated per account (`{account}` =
# the account's login email, normalized - see bootstrap._account_key - or
# DEFAULT_ACCOUNT_KEY for manual login / imported-cookie flows that don't go
# through FACEBOOK_ACCOUNTS at all). Without this, every account would
# overwrite the same global storage_state/token cache, making rotation
# between accounts pointless - each account needs its own session so it can
# be reused independently on the next run instead of clobbering the last
# account's.
DEFAULT_ACCOUNT_KEY = "default"
CACHE_REDIS_KEY_TMPL = "facebook:session_cache:{account}"
STATE_REDIS_KEY_TMPL = "facebook:storage_state:{account}"
COMMENTS_REDIS_KEY_TMPL = "facebook:comments_query:{account}"
# Which account's cache FacebookGraphQLClient uses when not given one
# explicitly - set by bootstrap.py after each run, so `scrapy crawl ...`
# picks up whichever account was most recently (re)bootstrapped.
ACTIVE_ACCOUNT_REDIS_KEY = "facebook:active_account"
# Index into accounts.FACEBOOK_ACCOUNTS of the next account to log in with -
# persisted so consecutive bootstrap runs (e.g. cron, hours/days apart) cycle
# through every account instead of always reusing the first one.
ACCOUNT_ROTATION_REDIS_KEY = "facebook:account_rotation_index"

# Global (not per-query) sets: the same post/entity id means the same real
# Facebook object no matter which search query surfaced it, so dedupe applies
# across queries too, not just across repeated runs of the same query.
SEEN_POSTS_KEY = "facebook:seen_post_ids"
SEEN_ENTITIES_KEY = "facebook:seen_entity_ids"
SEEN_COMMENTS_KEY = "facebook:seen_comment_ids"

# --- Token cache
# fb_dtsg/lsd/__rev usually stay valid for a few hours - re-bootstrap past this
CACHE_MAX_AGE_SECONDS = 6 * 3600

# --- Retry/backoff (graphql_client.py)
# Retry transient failures (rate limiting, 5xx, network blips) with backoff.
# 401/403 are NOT retried - those mean the token is dead, not overloaded.
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0

# --- Request pacing (graphql_client.py)
# Every spider here calls curl_cffi directly instead of going through
# Scrapy's downloader, so Scrapy's own DOWNLOAD_DELAY/AUTOTHROTTLE never
# apply - without this, back-to-back pages/queries would fire with no gap
# at all. Jitter avoids a perfectly uniform interval, which is itself a
# bot-like signal.
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0

# --- Captured request fields (bootstrap.py)
# Fields from the form-urlencoded body worth keeping to replay the GraphQL
# request over plain HTTP. __dyn/__csr/__hsdp/__hblp/__sjsp are intentionally
# skipped: they're bytecode describing which JS modules were loaded, only
# used for client-side code-splitting - the server still responds fine
# without them (tested with the search query).
STATIC_BODY_FIELDS = (
    "av",
    "__user",
    "__a",
    "__req",
    "__hs",
    "dpr",
    "__ccg",
    "__rev",
    "__s",
    "__hsi",
    "__comet_req",
    "fb_dtsg",
    "jazoest",
    "lsd",
    "__spin_r",
    "__spin_b",
    "__spin_t",
    "__crn",
    "fb_api_caller_class",
)

STATIC_HEADER_FIELDS = (
    "user-agent",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "sec-ch-ua-platform-version",
    "sec-ch-ua-full-version-list",
    "x-asbd-id",
)

# Same idea for the login form - Facebook's login page now generates its
# `id` at runtime (React's useId(), e.g. "_r_2_"), so #email/#pass are no
# longer stable. `name`/`autocomplete` are used by the browser's own
# autofill and by the backend's form POST handling, so they're a much safer
# bet than id - kept as fallbacks last, in case an older variant is served.
LOGIN_EMAIL_SELECTORS = (
    'input[name="email"]',
    'input[autocomplete="username"]',
    "#email",
)
LOGIN_PASSWORD_SELECTORS = (
    'input[name="pass"]',
    'input[autocomplete="current-password"]',
    'input[type="password"]',
    "#pass",
)
LOGIN_BUTTON_TEXTS = ("Log in", "Log In", "Đăng nhập")

# Facebook's post-login two-factor code screen - only shown when the account
# has 2FA enabled and this browser/session isn't already trusted.
TWO_FA_CODE_SELECTORS = (
    'input[name="approvals_code"]',
    'input[autocomplete="one-time-code"]',
    'input[aria-label="Code"]',
    'input[aria-label="Mã"]',
)
TWO_FA_CONTINUE_BUTTON_TEXTS = ("Continue", "Tiếp tục", "Submit Code", "Gửi mã")

# On a brand-new browser context (no storage_state yet, so no prior consent
# saved), Facebook shows a cookie-consent modal *over* the login form before
# anything else - it has to be dismissed first or the email/password fields
# underneath are unreachable even though they exist in the DOM. Either
# button works (both just close the modal); "Allow" is picked first since
# "Decline" sometimes triggers a second confirmation step.
COOKIE_CONSENT_BUTTON_SELECTORS = (
    'button:has-text("Allow all cookies")',
    'button:has-text("Cho phép tất cả cookie")',
    'button:has-text("Decline optional cookies")',
    'button:has-text("Từ chối cookie không bắt buộc")',
)
