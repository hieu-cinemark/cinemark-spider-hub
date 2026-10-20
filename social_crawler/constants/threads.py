from __future__ import annotations

# Đã xác nhận với lưu lượng thật bắt được: query kết quả tìm kiếm
# (BarcelonaSearchResultsRefetchableQuery) POST tới /graphql/query, một endpoint mới hơn -
# KHÔNG phải /api/graphql như BarcelonaPostPageStrongIdTargetQuery bắt được hồi đầu phát
# triển project. Gửi đúng doc_id tới sai endpoint vẫn nhận 200 kèm lỗi
# invalid_variable_type cố định, dễ gây hiểu nhầm là bug schema variables chứ không phải
# sai URL.
GRAPHQL_URL = "https://www.threads.com/graphql/query"

# REST riêng tư của Instagram dưới threads.com - field Relay
# xdt_api__v1__text_feed__media_id__replies__connection trên BarcelonaPostPageDirectQuery
# là lớp bọc quanh lệnh GET này. Refetch GraphQL của connection đó phát lại thành
# direct_replies: null (xem features/comments/); đường này mới là thứ thực sự phân trang
# reply mà không cần trình duyệt, dùng cùng ds_user_id/sessionid/csrftoken mà bootstrap
# vốn đã cache cho tìm kiếm. Gọi với tư cách khách bị 403 login_required (đã thử thực tế).
TEXT_FEED_REPLIES_URL = "https://www.threads.com/api/v1/text_feed/{post_id}/replies/"
# Bề mặt REST này từ chối UA Chrome desktop với "useragent mismatch" (cùng phát hiện với
# client công khai threads-go). Tìm kiếm GraphQL vẫn dùng UA trình duyệt đã bắt được; chỉ
# các lượt đọc text_feed mới ghi đè thành UA này.
REST_READ_UA = "Barcelona 289.0.0.14.109 Android"
IG_APP_ID = "238260118697367"

# --- Key Redis
# Cùng lý do tạo key theo từng tài khoản như constants/facebook.py - mỗi tài khoản cần
# cache session/token riêng để xoay vòng giữa các mục INSTAGRAM_ACCOUNTS không đè lên
# cache của tài khoản khác.
DEFAULT_ACCOUNT_KEY = "default"
CACHE_REDIS_KEY_TMPL = "threads:session_cache:{account}"
COMMENTS_REDIS_KEY_TMPL = "threads:comments_query:{account}"
STATE_REDIS_KEY_TMPL = "threads:storage_state:{account}"
ACTIVE_ACCOUNT_REDIS_KEY = "threads:active_account"
ACCOUNT_ROTATION_REDIS_KEY = "threads:account_rotation_index"

SEEN_POSTS_KEY = "threads:seen_post_ids"
SEEN_COMMENTS_KEY = "threads:seen_comment_ids"
# SEEN_POSTS_KEY dùng RedisCache.add_if_new (key TTL theo từng id), không dùng sadd - nhớ
# vĩnh viễn là sai với bài có like_count/reply_count/repost_count/quote_count cứ thay đổi
# sau lần crawl đầu, cùng lý do như SEEN_POSTS_TTL_SECONDS của TikTok
# (constants/tiktok.py). SEEN_COMMENTS_KEY cố ý giữ sadd() vĩnh viễn kiểu cũ - comment
# cũng chưa bao giờ được chuyển đổi cho TikTok (xem comments.py của spider đó), nên đây
# chỉ khớp một quyết định đã có, không phải quyết định mới.
SEEN_POSTS_TTL_SECONDS = 7 * 24 * 3600
# Cùng kiểu thoát sớm như hashtag_search của TikTok: dừng một từ khoá khi có chừng này
# trang liên tiếp không ra bài *mới* nào (tất cả đã thấy / còn trong TTL).
MAX_CONSECUTIVE_EMPTY_NEW_PAGES = 20
# Trang toàn bài ĐÃ THẤY nhưng đúng chủ đề (2026-10-16): kết quả tìm kiếm xếp theo độ liên quan nên phần đầu lần nào cũng
# là cùng các bài cũ - tính chúng như trang trống khiến lượt cào thứ 2/3 trong ngày dừng trước khi tới phần sâu chưa lấy.
# Trang như vậy chỉ bị dừng sau chừng này trang liên tiếp; trang không có bài đúng chủ đề nào vẫn theo mốc 20 ở trên.
MAX_CONSECUTIVE_SEEN_ONLY_PAGES = 80

# --- Cache token
CACHE_MAX_AGE_SECONDS = 6 * 3600

# --- Thử lại/backoff (graphql_client.py)
# 3 -> 5 (17/10/2026): proxy xoay qua đường mạng nhà dân hay rớt cả phút (Viettel "adslfail", CONNECT 500), 3 lần
# cách 2s/4s hỏng hết và bỏ cả từ khoá; 5 lần giãn 2/4/8/16s, kèm reset IP cổng proxy từ lần lỗi mạng thứ 2.
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SECONDS = 2.0
# Cùng lý do như RETRY_BACKOFF_JITTER_SECONDS trong constants/facebook.py.
RETRY_BACKOFF_JITTER_SECONDS = 1.0

# --- Giãn cách request (graphql_client.py)
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0
# Cùng lý do bóp nhịp thích ứng như THROTTLE_REDIS_KEY_TMPL/ADAPTIVE_INTERVAL_MAX_SECONDS
# trong constants/facebook.py.
THROTTLE_REDIS_KEY_TMPL = "threads:adaptive_interval:{account}"
ADAPTIVE_INTERVAL_MAX_SECONDS = 12.0

# --- Hồ sơ kênh (features/profiles/) - follower cho trang "Xuất thông tin theo kênh" của cinemark (20/10/2026)
# Query hovercard bắt từ trình duyệt thật (rê chuột lên tên người dùng). doc_id do Threads đổi theo bản build - hết hiệu
# lực thì response không còn data.user (errors) và worker dừng, không ghi "không tìm thấy" hàng loạt; bắt lại doc_id mới
# từ DevTools rồi sửa ở đây.
PROFILE_DOC_ID = "29082997647979831"
PROFILE_FRIENDLY_NAME = "BarcelonaUsernameHovercardImplDirectQuery"
PROFILE_ROOT_FIELD = "xdt_text_app_user_by_username"
PROFILE_VARIABLES_TEMPLATE = {
    "username": "",
    "__relay_internal__pv__BarcelonaIsInternalUserrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasMessagingrelayprovider": True,
    "__relay_internal__pv__BarcelonaMessagesHasLiveChatMessagingrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldShowFediverseM1Featuresrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasEventBadgerelayprovider": False,
}
# Bàn giao với cinemark-api (app/services/channel_profiles.py): bên đó SADD handle cần hỏi vào hàng đợi rồi xếp
# type=profiles lên crawl_requests; worker SPOP từng handle, ghi kết quả (JSON) vào hash theo handle; cinemark-api gom
# hash ở vòng scheduler kế tiếp, ghi D1 channel_profiles rồi HDEL. Hash có TTL để kết quả không ai gom không sống mãi.
PROFILE_QUEUE_REDIS_KEY = "threads:profile_queue"
PROFILE_RESULTS_REDIS_KEY = "threads:profile_results"
PROFILE_RESULTS_TTL_SECONDS = 7 * 24 * 3600
# Tối đa mỗi lượt (tài khoản Threads hiếm, dễ cháy) + khoảng nghỉ thêm giữa hai handle, ngoài bóp nhịp của client.
PROFILE_MAX_PER_RUN = 200
PROFILE_GAP_MIN_SECONDS = 3.0
PROFILE_GAP_MAX_SECONDS = 7.0

# --- Bài của một kênh (features/channel_posts/) - kênh booking chưa có bài trong D1 (priority_channels của cinemark-api,
# 20/10/2026). Query tab "Threads" của trang hồ sơ, bắt từ trình duyệt thật đã đăng nhập (cuộn trang hồ sơ). Cần userID
# (pk) chứ không nhận username - lấy qua fetch_profile (hovercard) trước. doc_id đổi theo bản build như PROFILE_DOC_ID:
# hết hiệu lực thì response không còn data.mediaData và spider dừng với lỗi (không âm thầm ra 0 bài).
CHANNEL_POSTS_DOC_ID = "29049009161361820"
CHANNEL_POSTS_FRIENDLY_NAME = "BarcelonaProfileThreadsTabRefetchableDirectQuery"
CHANNEL_POSTS_ROOT_FIELD = "xdt_api__v1__text_feed__user_id__profile__connection"
CHANNEL_POSTS_VARIABLES_TEMPLATE = {
    "after": None,
    "allow_page_info_for_lox_user": False,
    "before": None,
    "first": 10,
    "last": None,
    "userID": "",
    "__relay_internal__pv__BarcelonaIsLoggedInrelayprovider": True,
    "__relay_internal__pv__BarcelonaShouldFetchPostAuthorFullNamerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasProfileSelfReplyContextrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasDearAlgoConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasMetaAiContentAttachmentsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMWRLocationConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaMessagesHasLiveChatMessagingrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasEventBadgerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasBestOfThreadsrelayprovider": True,
    "__relay_internal__pv__BarcelonaMessagingHasMetaAIBotrelayprovider": False,
    "__relay_internal__pv__BarcelonaGenAIRepliesEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsSearchDiscoveryEnabledrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunitiesrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGameScoreSharerelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPublicViewCountCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEmojiUpdateCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasCommunityEntityCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasScorecardCommunityrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasSportTeamAllegianceCardrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasStitchedAttachmentrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasNewspaperLinkStylerelayprovider": False,
    "__relay_internal__pv__BarcelonaHasMessagingrelayprovider": True,
    "__relay_internal__pv__BarcelonaOptionalCookiesEnabledrelayprovider": True,
    "__relay_internal__pv__BarcelonaShouldFulfillLightboxQueryrelayprovider": True,
    "__relay_internal__pv__BarcelonaCanSeeSponsoredContentrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasUnseenRepliesrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasWebFaviconsrelayprovider": False,
    "__relay_internal__pv__BarcelonaIsCrawlerrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasDearAlgoWebProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCommunityTopContributorsrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasCustomLikesConsumptionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasViewerRepliedrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPrivateRepliesDeprecationrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasGhostPostEmojiActivationrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasGhostPostProductionrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasPodcastTranscriptConsumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasPodcastV2Consumptionrelayprovider": True,
    "__relay_internal__pv__BarcelonaHasNewsletterLinkPreviewrelayprovider": False,
    "__relay_internal__pv__BarcelonaHasPublicViewCountShareCardrelayprovider": False,
    "__relay_internal__pv__BarcelonaShouldShowFediverseM075Featuresrelayprovider": True,
    "__relay_internal__pv__BarcelonaIsInternalUserrelayprovider": False,
}
# Trình duyệt xin 10 thread/trang. Mặc định mỗi kênh: 3 trang (~30 thread) và chỉ bài trong 60 ngày gần nhất - đủ cho
# nghiệm thu booking (bài quảng bá phim mới đăng), ít request cho tài khoản Threads hiếm.
CHANNEL_POSTS_PAGE_SIZE = 10
CHANNEL_POSTS_MAX_PAGES = 3
CHANNEL_POSTS_DAYS = 60

# --- Các trường request bắt được (bootstrap.py)
# threads.com chạy trên cùng stack GraphQL Comet/Barcelona với Facebook - đã xác nhận với
# một request BarcelonaPostPageStrongIdTargetQuery thật bắt được, body form của nó mang
# đúng các trường này (cùng tập với STATIC_BODY_FIELDS của constants/facebook.py).
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

# Cùng các header tĩnh như của Facebook, cộng thêm x-ig-app-id và x-web-session-id - cả
# hai đều có trên request thật bắt được và không có trong bộ header của Facebook.
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

# Một session threads.com đã đăng nhập cần ít nhất hai cookie này - khác với c_user/xs
# của Facebook, đã xác nhận với header Cookie của một request thật bắt được (threads.com
# dùng chung hệ thống tài khoản/session của Instagram, không phải của Facebook).
REQUIRED_LOGIN_COOKIES = ("ds_user_id", "sessionid")

# threads.com có trang đăng nhập riêng ở /login/ (form username + mật khẩu thường,
# "Continue with Instagram" chỉ là lựa chọn phụ) - với tài khoản đã tham gia Threads (đã
# chọn username, v.v. - một thao tác tài khoản làm một lần qua cầu nối Instagram hoặc app
# Threads), đăng nhập trực tiếp ở đây đặt ds_user_id/sessionid trên domain .threads.com
# ngay lập tức, không cần vòng qua instagram.com. Đã xác nhận với DOM thật: ô input hoàn
# toàn không có thuộc tính `name`, chỉ có autocomplete="username".
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

# Khác trang đăng nhập của Instagram, threads.com/login/ giữ ô username vẫn mount phía
# sau modal 2FA, nên lúc này trên trang có *hai* phần tử input[type="text"] - đã xác nhận
# với DOM thật rằng ô nhập mã có placeholder riêng ("Mã bảo mật" / "Security code"), khác
# với kiểu khớp input[type="text"] trơn dùng cho ô 2FA của Instagram (vốn không có
# placeholder nào). Quay về input[type="text"] nào còn trống nếu chính text placeholder
# thay đổi.
TWO_FA_CODE_SELECTORS = (
    'input[placeholder="Mã bảo mật"]',
    'input[placeholder="Security code"]',
    'input[placeholder="Security Code"]',
)
TWO_FA_CONTINUE_BUTTON_TEXTS = ("Gửi", "Confirm", "Continue", "Xác nhận", "Tiếp tục")

# Modal đồng ý cookie riêng của threads.com - cùng ý tưởng với của Facebook, hiện trên
# browser context hoàn toàn mới trước khi chạm được tới form đăng nhập bên dưới.
COOKIE_CONSENT_BUTTON_SELECTORS = (
    'button:has-text("Allow all cookies")',
    'button:has-text("Cho phép tất cả cookie")',
    'button:has-text("Decline optional cookies")',
    'button:has-text("Từ chối cookie không bắt buộc")',
)
