from __future__ import annotations

# Confirmed against real captured traffic (TikTok web, hashtag search). Not
# GraphQL - plain signed REST GETs, unlike Facebook/Threads.
HASHTAG_ITEM_LIST_URL = "https://www.tiktok.com/api/challenge/item_list/"
HASHTAG_DETAIL_URL = "https://www.tiktok.com/api/challenge/detail/"

# A keyword-search client was attempted too but abandoned - even
# byte-for-byte replays of real, freshly captured browser requests came
# back empty through curl_cffi (a TLS/HTTP2 fingerprint mismatch specific
# to that endpoint's stricter anti-bot check, not a logic bug - see
# client.py's module docstring). Nothing search-specific belongs here.

# --- Redis keys
# Same per-account templating rationale as constants/facebook.py.
DEFAULT_ACCOUNT_KEY = "default"
ACCOUNT_ROTATION_REDIS_KEY = "tiktok:account_rotation_index"
SEEN_POSTS_KEY = "tiktok:seen_video_ids"

# --- Request pacing
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0

# --- Retry/backoff
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0
# Same rationale as constants/facebook.py's own RETRY_BACKOFF_JITTER_SECONDS.
RETRY_BACKOFF_JITTER_SECONDS = 1.0

# Unlike Facebook/Threads, this endpoint needs no doc_id/token bootstrap via
# a browser at all - the only thing that has to come from a real, already-
# "trusted" browser session is the identity bundle below (cookie +
# device_id + odin_id). Confirmed by direct experiment: a brand-new
# Playwright-driven session (even using real Chromium, even after visiting
# the actual hashtag page and picking up a real ttwid/msToken from that
# same session) still gets an empty response - TikTok's device-trust check
# for this endpoint needs accumulated real usage history, which a one-shot
# automated visit can't manufacture. A device_id/odin_id/verifyFp lifted
# from an already-established real browser session works indefinitely
# after that, though - every other request against it (including
# pagination) just needs a freshly-computed X-Gnarly signature, which is
# generated locally per-request (see signature/gnarly.py) with no need to
# touch a browser again.
#
# Unlike Facebook/Threads (whose UA is captured fresh from a real browser
# at every bootstrap - see constants/facebook.py's STATIC_HEADER_FIELDS),
# this one is hardcoded and never touches a browser, so it never
# auto-updates either. Real Chrome ships a new version every few weeks;
# worth bumping this to whatever's current every quarter or so by hand
# (not automating it - a version bump changes what curl_cffi's own Chrome
# TLS impersonation target is too, so it needs the same live-tested
# verification any other change here gets, not a blind auto-refresh).
STATIC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

# X-Bogus is checked by request_capture's own JS but doesn't actually gate
# this endpoint - confirmed against a real captured request where it was
# already "1" verbatim, and every successful replay tested here kept it as
# "1" too without issue.
STATIC_X_BOGUS = "1"

# Static per-request params matching a real macOS Chrome web session -
# these describe the browser/device class, not the specific trusted
# identity (device_id/odin_id/verifyFp/ttwid/msToken), so they're safe to
# hardcode rather than needing to come from the captured account.
STATIC_PARAMS = {
    "aid": "1988",
    "app_language": "en",
    "app_name": "tiktok_web",
    "browser_language": "en-US",
    "browser_name": "Mozilla",
    "browser_online": "true",
    "browser_platform": "MacIntel",
    "browser_version": STATIC_UA,
    "channel": "tiktok_web",
    "cookie_enabled": "true",
    "coverFormat": "2",
    "data_collection_enabled": "true",
    "device_platform": "web_pc",
    "focus_state": "true",
    "from_page": "hashtag",
    "history_len": "6",
    "is_fullscreen": "false",
    "is_page_visible": "true",
    "language": "en",
    "os": "mac",
    "priority_region": "",
    "region": "VN",
    "screen_height": "982",
    "screen_width": "1512",
    "tz_name": "Asia/Saigon",
    "user_is_login": "false",
    "webcast_language": "en",
}
