from __future__ import annotations

from enum import StrEnum

GRAPHQL_URL = "https://www.facebook.com/api/graphql/"


class FacebookEntityType(StrEnum):
    """Các giá trị `__typename` GraphQL của Facebook mà project này rẽ nhánh trực tiếp. Không
    đầy đủ - chỉ những cái có xử lý riêng ở đâu đó trong extract.py."""

    STORY = "Story"
    FEEDBACK = "Feedback"
    PHOTO = "Photo"
    VIDEO = "Video"
    HASHTAG = "Hashtag"


# Node id của loại reaction trên Facebook là một id số cố định, giống nhau trên mọi tài
# khoản/ngôn ngữ - khác với top_reactions[].node.localized_name, vốn trả về theo ngôn ngữ
# mà cookie `locale` của tài khoản đang đặt (đã xác nhận: một tài khoản locale=vi_VN nhận
# "Thích" thay vì "Like"). Đặt key theo id đó để extract.py báo một tên tiếng Anh nhất quán
# bất kể tài khoản nào crawl bài. Giá trị đã xác nhận với một response thật cho "Like"
# (1635855486666999) - phần còn lại là các loại reaction chuẩn lâu đời khác của Facebook,
# cùng id được dùng từ khi Reactions ra mắt.
REACTION_ID_TO_NAME = {
    "1635855486666999": "like",
    "1678524932434102": "love",
    "115940658764963": "haha",
    "478547315650144": "wow",
    "908563459236466": "sad",
    "444813342392137": "angry",
    "613557422527858": "care",
}


# --- Key Redis
# Mọi key theo phiên đăng nhập đều được tạo theo mẫu cho từng tài khoản (`{account}` =
# email đăng nhập của tài khoản, đã chuẩn hoá - xem bootstrap._account_key - hoặc
# DEFAULT_ACCOUNT_KEY cho các luồng đăng nhập tay / import cookie hoàn toàn không đi qua
# FACEBOOK_ACCOUNTS). Không có cái này, mọi tài khoản sẽ ghi đè cùng một cache
# storage_state/token toàn cục, làm việc xoay vòng tài khoản trở nên vô nghĩa - mỗi tài
# khoản cần session riêng để được dùng lại độc lập ở lượt chạy sau thay vì đè lên của tài
# khoản trước.
DEFAULT_ACCOUNT_KEY = "default"
CACHE_REDIS_KEY_TMPL = "facebook:session_cache:{account}"
STATE_REDIS_KEY_TMPL = "facebook:storage_state:{account}"
COMMENTS_REDIS_KEY_TMPL = "facebook:comments_query:{account}"
# Cache tách riêng khỏi COMMENTS_REDIS_KEY_TMPL: trả lời một comment được định địa chỉ
# khác với danh sách comment cấp một của bài (xem FacebookGraphQLClient._reply_target_id)
# và có doc_id/variables_template bắt được riêng - xem `--type replies` của bootstrap.py.
REPLIES_REDIS_KEY_TMPL = "facebook:replies_query:{account}"
# Cache của tài khoản nào được FacebookGraphQLClient dùng khi không được chỉ định rõ - do
# bootstrap.py đặt sau mỗi lần chạy, để `scrapy crawl ...` lấy tài khoản nào vừa được
# bootstrap (lại) gần nhất.
ACTIVE_ACCOUNT_REDIS_KEY = "facebook:active_account"
# Chỉ số trong accounts.FACEBOOK_ACCOUNTS của tài khoản kế tiếp sẽ dùng để đăng nhập -
# được lưu lại để các lần bootstrap liên tiếp (ví dụ cron, cách nhau vài giờ/ngày) xoay
# vòng qua mọi tài khoản thay vì luôn dùng lại tài khoản đầu tiên.
ACCOUNT_ROTATION_REDIS_KEY = "facebook:account_rotation_index"

# Set toàn cục (không theo query): cùng một id bài/thực thể là cùng một đối tượng
# Facebook thật bất kể query tìm kiếm nào đưa nó ra, nên khử trùng áp dụng cả giữa các
# query, không chỉ giữa các lần chạy lặp lại của cùng một query.
SEEN_POSTS_KEY = "facebook:seen_post_ids"
SEEN_ENTITIES_KEY = "facebook:seen_entity_ids"
SEEN_COMMENTS_KEY = "facebook:seen_comment_ids"
# SEEN_POSTS_KEY dùng RedisCache.add_if_new (key TTL theo từng id), không dùng sadd - nhớ
# vĩnh viễn là sai với bài có comments_count/reactions_count/shares_count cứ thay đổi sau
# lần crawl đầu, cùng lý do như SEEN_POSTS_TTL_SECONDS của TikTok (constants/tiktok.py).
# Entity/comment cố ý giữ sadd() vĩnh viễn kiểu cũ - một entity (tham chiếu
# Hashtag/Photo/Video) không có số liệu riêng nào để bị cũ, và comment cũng chưa bao giờ
# được chuyển đổi cho TikTok (xem comments.py của spider đó), nên đây chỉ khớp một quyết
# định đã có, không phải quyết định mới.
SEEN_POSTS_TTL_SECONDS = 7 * 24 * 3600
# Cùng kiểu thoát sớm như hashtag_search của TikTok: dừng một từ khoá khi có chừng này
# trang GraphQL liên tiếp không ra bài *mới* nào (tất cả đã thấy rồi).
MAX_CONSECUTIVE_EMPTY_NEW_PAGES = 20

# --- Cache token
# fb_dtsg/lsd/__rev thường còn hiệu lực vài giờ - quá mốc này thì bootstrap lại
CACHE_MAX_AGE_SECONDS = 6 * 3600

# --- Thử lại/backoff (graphql_client.py)
# Thử lại các lỗi tạm thời (bị giới hạn rate, 5xx, mạng chập chờn) với backoff.
# 401/403 KHÔNG thử lại - đó là token đã chết, không phải quá tải.
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0
# Cộng thêm vào độ trễ cơ sở tăng theo cấp số để các lần thử lại không rơi đúng
# 2s/4s/8s mỗi lần - cùng lý do "khoảng cách đều tăm tắp tự nó đã là dấu hiệu của bot"
# như jitter giãn cách request bên dưới, chỉ là áp cho backoff thay vì nhịp thường.
RETRY_BACKOFF_JITTER_SECONDS = 1.0

# --- Giãn cách request (graphql_client.py)
# Mọi spider ở đây gọi thẳng curl_cffi thay vì đi qua downloader của Scrapy, nên
# DOWNLOAD_DELAY/AUTOTHROTTLE của Scrapy không bao giờ áp dụng - không có phần này, các
# trang/query liên tiếp sẽ bắn đi không có khoảng nghỉ nào. Jitter tránh khoảng cách đều
# tăm tắp, thứ tự nó đã là dấu hiệu của bot.
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0
# Mức sàn lưu trong Redis nằm trên MIN_REQUEST_INTERVAL_SECONDS, tăng lên khi
# _post_with_retry gặp 429/5xx/mạng căng thẳng và giảm dần lại khi response sạch (xem
# CometGraphQLClient._adjust_interval) - một khoảng cách cố định không chậm lại khi lượt
# chạy bắt đầu bị bóp, nó chỉ cứ thử lại cùng nhịp cho tới khi MAX_RETRIES bỏ cuộc.
THROTTLE_REDIS_KEY_TMPL = "facebook:adaptive_interval:{account}"
ADAPTIVE_INTERVAL_MAX_SECONDS = 12.0

# --- Các trường request bắt được (bootstrap.py)
# Các trường trong body form-urlencoded đáng giữ để phát lại request GraphQL qua HTTP
# thường. __dyn/__csr/__hsdp/__hblp/__sjsp cố ý bỏ qua: chúng là bytecode mô tả module JS
# nào đã được nạp, chỉ dùng cho việc chia nhỏ code phía client - server vẫn trả lời bình
# thường khi thiếu chúng (đã thử với query tìm kiếm).
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

# Cùng ý tưởng cho form đăng nhập - trang đăng nhập Facebook giờ sinh `id` lúc chạy
# (useId() của React, ví dụ "_r_2_"), nên #email/#pass không còn ổn định.
# `name`/`autocomplete` được tính năng tự điền của trình duyệt và phần xử lý POST form của
# backend dùng, nên an toàn hơn id nhiều - id được giữ làm phương án dự phòng sau cùng,
# phòng khi nhận phải biến thể cũ hơn.
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

# Màn hình nhập mã xác thực hai lớp sau khi đăng nhập Facebook - chỉ hiện khi tài khoản
# bật 2FA và trình duyệt/session này chưa được tin cậy.
TWO_FA_CODE_SELECTORS = (
    'input[name="approvals_code"]',
    'input[autocomplete="one-time-code"]',
    'input[aria-label="Code"]',
    'input[aria-label="Mã"]',
    'input[placeholder="Code"]',
    'input[placeholder="Mã"]',
)

# Các chuỗi con (so ở dạng chữ thường) chỉ xuất hiện trên màn hình nhập mã 2FA của
# Facebook - dùng để phân biệt "không có màn hình 2FA nào" (ổn, phần lớn lượt chạy dùng
# lại session đã được tin cậy) với "CÓ màn hình 2FA nhưng không selector/locator nào tìm
# được ô nhập mã" (Facebook lại đưa ra một biến thể markup mới - xem
# TwoFactorPromptNotHandledError), mà không phải đoán từ một danh sách selector cố định
# duy nhất, đúng thứ đã từng âm thầm hỏng ở đây một lần.
TWO_FA_PROMPT_TEXT_HINTS = (
    "authentication app",
    "ứng dụng xác thực",
    "6-digit code",
    "mã gồm 6 chữ số",
    "two-factor",
    "xác minh hai bước",
    "xác minh 2 bước",
)
TWO_FA_CONTINUE_BUTTON_TEXTS = ("Continue", "Tiếp tục", "Submit Code", "Gửi mã")

# Text giao diện sắp xếp comment của comments_trigger - mọi context project này tạo đều
# có locale="vi-VN" (xem browser_interaction.new_context), nên Facebook hiển thị bằng
# tiếng Việt, không phải tiếng Anh. Tiếng Anh được giữ đầu/kèm theo cho tài khoản nào có
# cookie locale riêng ghi đè thành ngôn ngữ khác (xem ghi chú của REACTION_ID_TO_NAME về
# chuỗi phụ thuộc ngôn ngữ ở trên).
COMMENT_SORT_TRIGGER_TEXTS = ("Most relevant", "Phù hợp nhất")
COMMENT_SORT_NEWEST_TEXTS = ("Newest", "Mới nhất")
COMMENT_REPLY_TEXTS = ("Reply", "Phản hồi")
# Link mở rộng "N replies"/"Xem N câu trả lời" dưới một comment thực sự có reply - cố ý
# KHÔNG dùng lại COMMENT_REPLY_TEXTS ở trên, vốn là nút "Reply"/"Phản hồi" trơn để VIẾT
# reply mới (bấm vào đó mở ô soạn thảo, không phải lấy GraphQL - nhầm hai cái này sẽ làm
# replies_trigger bấm sai phần tử). Trong cách Facebook hiển thị nó luôn đi kèm một con
# số, mà mẫu này đòi có để phân biệt hai cái; khớp bằng regex (không phải text chính xác)
# vì từ ngữ/tiền tố chính xác ("Xem ", "View ") thay đổi và chưa được xác nhận cho mọi
# ngôn ngữ/bản deploy.
COMMENT_VIEW_REPLIES_PATTERN = r"\d+\s*(phản hồi|câu trả lời|repl(y|ies))"
# URL /videos/ dẫn tới trình phát Video Home riêng của Facebook (sidebar + trình phát +
# thanh Like/Comment/Share bên dưới) thay vì permalink bài thường - danh sách comment/nút
# sắp xếp hoàn toàn chưa có trong DOM cho tới khi bấm mở cái này (đã xác nhận với ảnh
# chụp màn hình thật: không có panel comment nào, chỉ có thanh đó). Thử trên bài
# permalink cũng vô hại, ở đó comment đã mở sẵn và đơn giản là không tìm thấy gì khớp
# (click_first chấp nhận chuyện đó - xem comments_trigger).
COMMENT_OPEN_BUTTON_TEXTS = ("Comment", "Bình luận")

# Trên một browser context hoàn toàn mới (chưa có storage_state, nên chưa lưu đồng ý nào
# trước đó), Facebook hiện modal đồng ý cookie *đè lên* form đăng nhập trước mọi thứ khác
# - phải đóng nó trước, nếu không các ô email/mật khẩu bên dưới không chạm tới được dù
# chúng có trong DOM. Nút nào cũng được (cả hai đều chỉ đóng modal); chọn "Allow" trước vì
# "Decline" đôi khi kích hoạt thêm một bước xác nhận.
COOKIE_CONSENT_BUTTON_SELECTORS = (
    'button:has-text("Allow all cookies")',
    'button:has-text("Cho phép tất cả cookie")',
    'button:has-text("Decline optional cookies")',
    'button:has-text("Từ chối cookie không bắt buộc")',
)
