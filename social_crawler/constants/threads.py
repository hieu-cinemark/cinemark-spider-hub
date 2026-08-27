from __future__ import annotations

# Confirmed against real captured traffic: the search-results query
# (BarcelonaSearchResultsRefetchableQuery) posts to /graphql/query, a newer
# endpoint - NOT /api/graphql like the BarcelonaPostPageStrongIdTargetQuery
# captured earlier in this project's development. Sending the right doc_id
# to the wrong endpoint got a 200 back with a deterministic
# invalid_variable_type error, misleadingly looking like a variables-schema
# bug rather than a wrong-URL one.
GRAPHQL_URL = "https://www.threads.com/graphql/query"

# --- Redis keys
# Same per-account templating rationale as constants/facebook.py - each
# account needs its own session/token cache so rotating between
# INSTAGRAM_ACCOUNTS entries doesn't clobber another account's cache.
DEFAULT_ACCOUNT_KEY = "default"
CACHE_REDIS_KEY_TMPL = "threads:session_cache:{account}"
STATE_REDIS_KEY_TMPL = "threads:storage_state:{account}"
ACTIVE_ACCOUNT_REDIS_KEY = "threads:active_account"
ACCOUNT_ROTATION_REDIS_KEY = "threads:account_rotation_index"

SEEN_POSTS_KEY = "threads:seen_post_ids"

# --- Token cache
CACHE_MAX_AGE_SECONDS = 6 * 3600

# --- Retry/backoff (graphql_client.py)
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0

# --- Request pacing (graphql_client.py)
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0

# --- Captured request fields (bootstrap.py)
# threads.com runs on the same Comet/Barcelona GraphQL stack as Facebook -
# confirmed against a real captured BarcelonaPostPageStrongIdTargetQuery
# request, whose form body carried exactly these fields (same set as
# constants/facebook.py's STATIC_BODY_FIELDS).
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

# Same static headers as Facebook's, plus x-ig-app-id and x-web-session-id -
# both present on the real captured request and absent from Facebook's own
# header set.
STATIC_HEADER_FIELDS = (
    "user-agent",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "sec-ch-ua-platform-version",
    "sec-ch-ua-full-version-list",
    "x-asbd-id",
    "x-ig-app-id",
    "x-web-session-id",
)

# A logged-in threads.com session needs at least these two cookies - unlike
# Facebook's c_user/xs, confirmed against a real captured request's Cookie
# header (threads.com shares Instagram's account/session system, not
# Facebook's).
REQUIRED_LOGIN_COOKIES = ("ds_user_id", "sessionid")

# threads.com has its own native login page at /login/ (a plain username +
# password form, with "Continue with Instagram" offered only as a secondary
# option) - for an account that has already joined Threads (picked a
# username, etc. - a one-time account action done once through the
# Instagram bridge or the Threads app), logging in here directly sets
# ds_user_id/sessionid on the .threads.com domain immediately, no
# instagram.com round trip needed. Confirmed against the real DOM: the
# input carries no `name` attribute at all, just autocomplete="username".
LOGIN_EMAIL_SELECTORS = (
    'input[autocomplete="username"]',
    'input[name="username"]',
    'input[name="email"]',
)
LOGIN_PASSWORD_SELECTORS = (
    'input[autocomplete="current-password"]',
    'input[type="password"]',
    'input[name="password"]',
)
LOGIN_BUTTON_TEXTS = ("Log in", "Log In", "Đăng nhập")

# Unlike Instagram's own login page, threads.com/login/ keeps the username
# field mounted behind the 2FA modal, so there are *two* input[type="text"]
# elements on the page at this point - confirmed against the real DOM that
# the code field has its own placeholder ("Mã bảo mật" / "Security code"),
# unlike the bare input[type="text"] match used for Instagram's own 2FA
# field (which has no placeholder at all). Falls back to whichever
# input[type="text"] is still empty if the placeholder text itself changes.
TWO_FA_CODE_SELECTORS = (
    'input[placeholder="Mã bảo mật"]',
    'input[placeholder="Security code"]',
    'input[placeholder="Security Code"]',
)
TWO_FA_CONTINUE_BUTTON_TEXTS = ("Gửi", "Confirm", "Continue", "Xác nhận", "Tiếp tục")

# threads.com's own cookie-consent modal - same idea as Facebook's, shown on
# a brand-new browser context before the login form underneath is reachable.
COOKIE_CONSENT_BUTTON_SELECTORS = (
    'button:has-text("Allow all cookies")',
    'button:has-text("Cho phép tất cả cookie")',
    'button:has-text("Decline optional cookies")',
    'button:has-text("Từ chối cookie không bắt buộc")',
)
