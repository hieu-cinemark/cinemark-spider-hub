from __future__ import annotations

# Confirmed against real captured traffic (TikTok web, hashtag search). Not
# GraphQL - plain signed REST GETs, unlike Facebook/Threads.
HASHTAG_ITEM_LIST_URL = "https://www.tiktok.com/api/challenge/item_list/"
HASHTAG_DETAIL_URL = "https://www.tiktok.com/api/challenge/detail/"

COMMENT_ITEM_LIST_URL = "https://www.tiktok.com/api/comment/list/"
# Fetched via TikTokCommentClient (curl_cffi + X-Gnarly + local
# X-Dynosaur). Gnarly-only returns empty 200; Dynosaur unlocks it
# (live A/B 2026-09-18). See features/comments/comments.py.
COMMENT_REPLY_LIST_URL = "https://www.tiktok.com/api/comment/list/reply/"
# Same signing as COMMENT_ITEM_LIST_URL (Dynosaur required). Params are
# comment_id + item_id (aweme id), not aweme_id. Confirmed live 2026-09-18.

# A channel/user's own posted-videos feed (paginated via cursor, identified
# by secUid) - used by features/channel_videos. Unlike HASHTAG_ITEM_LIST_URL,
# this one DOES genuinely need a real browser-computed X-Dynosaur - confirmed
# by direct live A/B test (2026-09-16), not assumed from the hashtag case or
# carried over from search/comments' own notes:
#
#   1. A real captured request (device_id/cookies/X-Gnarly/X-Dynosaur all
#      real, from a genuine browser session) replayed verbatim through
#      curl_cffi came back with real data (itemList of 16 videos) - the
#      capture itself wasn't stale.
#   2. That EXACT same URL, byte-for-byte identical except the X-Dynosaur
#      param deleted outright, came back as an empty 200. Replaced with an
#      obviously-wrong X-Dynosaur value instead of deleted: also empty.
#      Nothing else about the request changed - same real X-Gnarly, same
#      param order, same WebIdLastTime, same cookies.
#   3. Immediately re-replaying the original untouched URL again (no
#      X-Dynosaur mutation) still worked - ruling out "this identity/IP got
#      rate-limited/blocked partway through testing" as an alternative
#      explanation for step 2's empty results. The emptiness really is
#      caused by the X-Dynosaur mutation specifically.
#
#   Separately, rebuilding the request from scratch (this project's own
#   STATIC_PARAMS-style param dict + a freshly-computed local X-Gnarly),
#   even while keeping every identity field AND the real captured
#   X-Dynosaur verbatim, still came back empty - i.e. a locally-rebuilt
#   request isn't equivalent to the browser's own for this endpoint even
#   before touching X-Dynosaur (unlike HASHTAG_ITEM_LIST_URL, where the
#   locally-signed approach works fine). This project has no local
#   implementation of X-Dynosaur that's confirmed to produce a value
#   TikTok's servers actually accept (an earlier reverse-engineering
#   attempt, signature/dynasaur.py, was removed 2026-09-17 - dead code,
#   never wired into any client, and its own construction was never
#   verified against a real captured value) - so this endpoint is
#   browser-only, same as COMMENT_ITEM_LIST_URL (though see that constant's
#   own note - "browser-only" there turned out to still mean *headless* is
#   fine, once a non-VN proxy and a JS-dispatched click were both in place)
#   and unlike HASHTAG_ITEM_LIST_URL, which only *looked* like
#   it needed a browser until a params dict bug was found and fixed.
#   COMMENT_ITEM_LIST_URL was later unlocked with local X-Dynosaur
#   (2026-09-18) — see features/comments/comments.py; the Dynosaur note
#   above still applies to post/item_list.
POST_ITEM_LIST_URL = "https://www.tiktok.com/api/post/item_list/"

# --- Redis keys
# Same per-account templating rationale as constants/facebook.py.
DEFAULT_ACCOUNT_KEY = "default"
ACCOUNT_ROTATION_REDIS_KEY = "tiktok:account_rotation_index"

# scrapy crawl tiktok_comments exits with this when every usable attempt
# failed because sticky-pinned proxies are cooling / the pool is empty -
# crawl_request_consumer maps it back to ProxyPoolExhaustedError so the
# Kafka message can be requeued with backoff instead of being committed
# as a quiet success.
PROXY_EXHAUSTED_EXIT_CODE = 75
SEEN_POSTS_KEY = "tiktok:seen_video_ids"
SEEN_COMMENTS_KEY = "tiktok:seen_comment_ids"
# Every challenge_id ever crawled, whether as a manually-queued hashtag or a
# BFS-discovered one (see hashtag_search/search.py) - a global, never-
# expiring set so the same related tag never gets queued twice across
# separate runs, and BFS can't loop back on a hashtag it (or a sibling
# branch) already covered.
SEEN_HASHTAGS_KEY = "tiktok:seen_hashtag_ids"
# Co-occurring hashtags from the last crawl of a D1 keyword, shown on the
# dashboard for a human to add/run. Written by hashtag_search/search.py.
RELATED_HASHTAGS_KEY_TMPL = "tiktok:related_hashtags:{keyword_id}"
RELATED_HASHTAGS_TTL_SECONDS = 14 * 24 * 3600

# --- BFS hashtag expansion (hashtag_search/search.py)
# Co-occurring tags are stored in Redis for dashboard review. A hop only
# runs after an operator adopts the chip (create keyword + crawl) - the
# spider never auto-publishes follow-up crawl_requests. depth 0 = the
# originally-queued hashtag; each approved hop increments bfs_depth by 1
# and stops suggesting further tags once it would exceed this.
BFS_MAX_DEPTH = 2
# How many of a run's related hashtags are stored for dashboard chips
# (the human-facing log still reports up to top_related_hashtags()'s
# own limit=10).
BFS_MAX_HASHTAGS_PER_RUN = 5
# Approved BFS hops are exploratory volume, not a deliberate deep sweep
# a human asked for on the root keyword - capped well below the 100-page
# default so one generic tag with a huge feed can't balloon on its own.
BFS_MAX_PAGES = 10
# Stop a hashtag/keyword crawl early once this many consecutive item_list
# pages yield zero *new* posts (all already in SEEN_POSTS_KEY). Re-crawls
# of a saturated tag otherwise burn the remaining max_pages budget on
# duplicates. Reset whenever a page produces at least one new post.
MAX_CONSECUTIVE_EMPTY_NEW_PAGES = 20

# --- Request pacing
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0
# Redis-backed floor above MIN_REQUEST_INTERVAL_SECONDS that grows when
# TikTokClient._post_with_retry sees 429/5xx/network stress and decays back
# down on clean responses (see TikTokClient._adjust_interval) - same
# mechanism as Facebook/Threads' own THROTTLE_REDIS_KEY_TMPL/
# ADAPTIVE_INTERVAL_MAX_SECONDS (comet_graphql_client.py), ported here
# because TikTok's own client never had it: a static interval doesn't slow
# down once an account/proxy starts getting throttled, it just keeps
# retrying at the same pace until MAX_RETRIES gives up - and this project's
# TikTok proxy pool (3 proxies/7 accounts as of 2026-09) has repeatedly
# degraded under exactly that kind of flat-pace hammering. Keyed by
# device_id (TikTok's own per-account identity unit), not "account" -
# there's no separate login/account-name concept here the way Facebook/
# Threads have one. Ceiling picked a bit above FB/Threads' 12.0 rather than
# copied verbatim - TikTok's own proxy pool is smaller (3 proxies/7
# accounts as of 2026-09) so a stressed account has less spare capacity to
# rotate onto, worth a slightly longer worst-case backoff; not derived from
# a specific measured incident the way the base MIN_REQUEST_INTERVAL_SECONDS
# values were, just a judgment call - tune freely.
THROTTLE_REDIS_KEY_TMPL = "tiktok:adaptive_interval:{device_id}"
ADAPTIVE_INTERVAL_MAX_SECONDS = 15.0

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
# worth bumping this to whatever's current every quarter or so by hand.
#
# This is the *real-browser* UA - used only where a genuine Patchright/
# Chromium context is actually running (auth/bootstrap.py's login capture;
# comments.py/channel_videos/search.py let Playwright report its own real
# UA instead of overriding it, so they never had this problem). Its Chrome
# major version must match whatever Chromium build Patchright actually
# bundles (confirmed live 2026-09-16 via a real capture: "HeadlessChrome/
# 151.0.7922.34") - a mismatch here would be a real browser lying about its
# own version, the same class of tell as CURL_CFFI_UA's own mismatch bug
# below, just in the opposite direction. Do NOT reuse this constant for any
# curl_cffi-signed request - see CURL_CFFI_UA for why they must stay two
# separate constants pinned to two different, unrelated version numbers.
STATIC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

# The UA for every curl_cffi-signed request (client.py's TikTokClient -
# hashtag_search's item_list; comments went fully browser-based, see
# features/comments/comments.py's own module docstring, and TikTokCommentClient,
# its unused REST equivalent, was removed 2026-09-17).
# This used to just be STATIC_UA (claiming Chrome/151.0.0.0) while every
# curl_cffi Session in client.py was created with the bare `impersonate=
# "chrome"` alias - a real, live-confirmed bug (2026-09-17), not a
# hypothetical one: curl_cffi's installed version has no "chrome151" TLS
# fingerprint at all (BrowserType's highest is chrome146), so bare "chrome"
# silently fell back to some other, unrelated version's ClientHello while
# the UA header and the X-Gnarly-signed browser_version param both still
# claimed 151 - a TLS-fingerprint/UA mismatch present on literally every
# curl_cffi request this project has ever sent, unlike a real browser
# (Patchright) which can't produce this particular kind of tell at all.
# Confirmed live: 8 fresh synthetic identities/IPs in a row all got an
# empty response with the old bare "chrome"+151.0.0.0 pairing; switching to
# this exact matched pairing (chrome131 TLS + a 131 UA) succeeded on the
# very first attempt, no retry needed - see client.py's own
# CURL_CFFI_IMPERSONATE_TARGET for the paired impersonate= value, which
# must always name the same Chrome version as this UA string's own
# Chrome/NNN part. Bumping either one without the other is exactly the bug
# this comment documents - change them together, and only after the same
# kind of live A/B test that caught this.
CURL_CFFI_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# The curl_cffi impersonate= value paired with CURL_CFFI_UA above - must
# always name the same Chrome major version as that UA's own Chrome/NNN
# part (see its docstring for why). Passed to curl_requests.Session(
# impersonate=...) in client.py rather than the bare "chrome" alias, which
# is what silently mismatched in the first place.
CURL_CFFI_IMPERSONATE_TARGET = "chrome131"

# X-Bogus is checked by request_capture's own JS but doesn't actually gate
# this endpoint - confirmed against a real captured request where it was
# already "1" verbatim, and every successful replay tested here kept it as
# "1" too without issue.
STATIC_X_BOGUS = "1"

# Static per-request params matching a real macOS Chrome web session -
# these describe the browser/device class, not the specific trusted
# identity (device_id/odin_id/verifyFp/ttwid/msToken/user_is_login), so
# they're safe to hardcode rather than needing to come from the captured
# account. user_is_login is deliberately NOT here - see client.py's
# TikTokClient._is_logged_in - it depends on whether this specific
# account's cookie carries a real logged-in session (sessionid), confirmed
# by direct testing to return meaningfully more results per hashtag than a
# guest-only cookie.
#
# This whole dict is only ever sent over curl_cffi (see client.py's
# TikTokClient._request) - browser_version must be CURL_CFFI_UA, not
# STATIC_UA, or it's back to the same TLS-fingerprint/UA mismatch
# CURL_CFFI_UA's own docstring describes.
STATIC_PARAMS = {
    "aid": "1988",
    "app_language": "en",
    "app_name": "tiktok_web",
    "browser_language": "en-US",
    "browser_name": "Mozilla",
    "browser_online": "true",
    "browser_platform": "MacIntel",
    "browser_version": CURL_CFFI_UA,
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
    "webcast_language": "en",
}

# Query-string key order for curl_cffi-signed /api/challenge/* requests.
# Confirmed live 2026-09-17: TikTok returns HTTP 200 + empty body when the
# same param values are urlencoded in the wrong order (Python 3.7+ dict
# insertion order from `{**STATIC_PARAMS, **extra, device_id, ...}` —
# "prod_client_order" in the A/B harness). Replaying a real browser's
# parse_qsl order with identical values works; alpha-sort / reversed /
# prod_client_order all empty. Captured from Patchright on
# /api/challenge/item_list/ (guest). challengeName sits next to
# challengeID so /api/challenge/detail/ can share this list. Keys not
# listed here are appended in the caller's insertion order after these.
SIGNED_QUERY_PARAM_ORDER = [
    "WebIdLastTime",
    "aid",
    "app_language",
    "app_name",
    "browser_language",
    "browser_name",
    "browser_online",
    "browser_platform",
    "browser_version",
    "challengeID",
    "challengeName",
    "channel",
    "clientABVersions",
    "cookie_enabled",
    "count",
    "coverFormat",
    "cursor",
    "data_collection_enabled",
    "device_id",
    "device_platform",
    "focus_state",
    "from_page",
    "history_len",
    "is_fullscreen",
    "is_page_visible",
    "language",
    "odinId",
    "os",
    "priority_region",
    "referer",
    "region",
    "root_referer",
    "screen_height",
    "screen_width",
    "tz_name",
    "user_is_login",
    "webcast_language",
    "msToken",
]
