from __future__ import annotations

# Đã xác nhận với lưu lượng thật bắt được (TikTok web, tìm theo hashtag). Không phải
# GraphQL - chỉ là các lệnh GET REST có ký, khác Facebook/Threads.
HASHTAG_ITEM_LIST_URL = "https://www.tiktok.com/api/challenge/item_list/"
HASHTAG_DETAIL_URL = "https://www.tiktok.com/api/challenge/detail/"

COMMENT_ITEM_LIST_URL = "https://www.tiktok.com/api/comment/list/"
# Lấy qua TikTokCommentClient (curl_cffi + X-Gnarly + X-Dynosaur tính ở local). Chỉ có
# Gnarly thì trả 200 rỗng; có Dynosaur thì mở được (A/B thực tế 2026-09-18). Xem
# features/comments/comments.py.
COMMENT_REPLY_LIST_URL = "https://www.tiktok.com/api/comment/list/reply/"
# Cùng cách ký như COMMENT_ITEM_LIST_URL (bắt buộc Dynosaur). Tham số là comment_id +
# item_id (id aweme), không phải aweme_id. Đã xác nhận thực tế 2026-09-18.

# Feed video đã đăng của một kênh (định danh bằng secUid, phân trang bằng cursor là timestamp
# ms) - dùng bởi features/channel_videos. Trước 2026-10-13 endpoint này chỉ chạy qua trình
# duyệt vì nó đòi X-Gnarly 5.3.2 + X-Dynosaur thật; signer 5.3.2 mới ở signature/ mở được nó
# bằng curl_cffi + danh tính synthetic (A/B thực tế 2026-10-13: 31-35 video/trang).
POST_ITEM_LIST_URL = "https://www.tiktok.com/api/post/item_list/"

# Tìm kiếm từ khoá (tab "Top") - dùng bởi features/search. Đã xác nhận thực tế 2026-10-13:
# danh tính synthetic lấy được dữ liệu ở đây, còn /api/search/item/full/ (tab "Video") thì
# trả 200 rỗng với synthetic (chỉ chạy với danh tính khách thật có lịch sử) - nên chỉ dùng
# general/full. Synthetic cũng phải có msToken trước (xem TikTokClient.warm_session), nếu
# không trang đầu đã rỗng. Phân trang: search_id = log_pb.impr_id của trang 1, cursor/offset
# lấy từ response.
SEARCH_GENERAL_URL = "https://www.tiktok.com/api/search/general/full/"
# Gửi nguyên văn như trình duyệt (JSON gọn, không khoảng trắng).
SEARCH_WEB_SEARCH_CODE = (
    '{"tiktok":{"client_params_x":{"search_engine":{"ies_mt_user_live_video_card_use_libra":1,'
    '"mt_search_general_user_live_card":1}},"search_server":{}}}'
)

# --- Key Redis
# Cùng lý do tạo key theo từng tài khoản như constants/facebook.py.
DEFAULT_ACCOUNT_KEY = "default"
ACCOUNT_ROTATION_REDIS_KEY = "tiktok:account_rotation_index"

# scrapy crawl tiktok_comments thoát với mã này khi mọi lần thử dùng được đều thất bại vì
# proxy đã ghim đang cooldown / pool rỗng - crawl_request_consumer ánh xạ ngược nó thành
# ProxyPoolExhaustedError để message Kafka được xếp hàng lại với backoff thay vì bị
# commit như một lần thành công lặng lẽ.
PROXY_EXHAUSTED_EXIT_CODE = 75
SEEN_POSTS_KEY = "tiktok:seen_video_ids"
SEEN_COMMENTS_KEY = "tiktok:seen_comment_ids"
# Mọi challenge_id từng được crawl, dù là hashtag xếp hàng bằng tay hay tìm ra qua BFS
# (xem hashtag_search/search.py) - một set toàn cục, không bao giờ hết hạn, để cùng một
# tag liên quan không bao giờ bị xếp hàng hai lần qua các lượt chạy khác nhau, và BFS
# không thể vòng lại một hashtag mà nó (hoặc một nhánh anh em) đã phủ rồi.
SEEN_HASHTAGS_KEY = "tiktok:seen_hashtag_ids"
# Các hashtag xuất hiện cùng từ lần crawl gần nhất của một từ khoá D1, hiển thị trên
# dashboard để người dùng thêm/chạy. Do hashtag_search/search.py ghi.
RELATED_HASHTAGS_KEY_TMPL = "tiktok:related_hashtags:{keyword_id}"
RELATED_HASHTAGS_TTL_SECONDS = 14 * 24 * 3600

# --- Mở rộng hashtag theo BFS (hashtag_search/search.py)
# Các tag xuất hiện cùng được lưu trong Redis để duyệt trên dashboard. Một bước nhảy chỉ
# chạy sau khi người vận hành chấp nhận chip (tạo từ khoá + crawl) - spider không bao giờ
# tự publish crawl_requests tiếp theo. depth 0 = hashtag xếp hàng ban đầu; mỗi bước nhảy
# được duyệt tăng bfs_depth thêm 1 và ngừng gợi ý thêm tag khi sắp vượt mức này.
BFS_MAX_DEPTH = 2
# Số hashtag liên quan của một lượt chạy được lưu làm chip trên dashboard (log cho người
# đọc vẫn báo tới giới hạn limit=10 của top_related_hashtags()).
BFS_MAX_HASHTAGS_PER_RUN = 5
# Các bước nhảy BFS đã duyệt là khối lượng mang tính khám phá, không phải lượt quét sâu có
# chủ đích mà người dùng yêu cầu cho từ khoá gốc - giới hạn thấp hơn nhiều so với mặc định
# 100 trang để một tag chung chung có feed khổng lồ không tự phình to.
BFS_MAX_PAGES = 10
# Dừng sớm một lượt crawl hashtag/từ khoá khi có chừng này trang item_list liên tiếp không
# ra bài *mới* nào (tất cả đã có trong SEEN_POSTS_KEY). Nếu không, crawl lại một tag đã
# bão hoà sẽ đốt phần ngân sách max_pages còn lại vào bài trùng. Reset mỗi khi một trang
# ra ít nhất một bài mới.
MAX_CONSECUTIVE_EMPTY_NEW_PAGES = 20

# --- Giãn cách request
MIN_REQUEST_INTERVAL_SECONDS = 1.5
REQUEST_INTERVAL_JITTER_SECONDS = 1.0
# Mức sàn lưu trong Redis nằm trên MIN_REQUEST_INTERVAL_SECONDS, tăng lên khi
# TikTokClient._post_with_retry gặp 429/5xx/mạng căng thẳng và giảm dần lại khi response
# sạch (xem TikTokClient._adjust_interval) - cùng cơ chế như
# THROTTLE_REDIS_KEY_TMPL/ADAPTIVE_INTERVAL_MAX_SECONDS của Facebook/Threads
# (comet_graphql_client.py), chuyển sang đây vì client riêng của TikTok chưa bao giờ có:
# một khoảng cách cố định không chậm lại khi một tài khoản/proxy bắt đầu bị bóp, nó chỉ
# cứ thử lại cùng nhịp cho tới khi MAX_RETRIES bỏ cuộc - và pool proxy TikTok của project
# này (3 proxy/7 tài khoản tính tới 2026-09) đã nhiều lần xuống cấp đúng vì kiểu dồn dập
# đều nhịp đó. Key theo device_id (đơn vị danh tính theo tài khoản của TikTok), không theo
# "tài khoản" - ở đây không có khái niệm đăng nhập/tên tài khoản riêng như Facebook/
# Threads. Mức trần chọn cao hơn một chút so với 12.0 của FB/Threads thay vì chép y nguyên
# - pool proxy riêng của TikTok nhỏ hơn (3 proxy/7 tài khoản tính tới 2026-09) nên một tài
# khoản đang bị căng ít chỗ dư để xoay sang, đáng có backoff trường hợp xấu nhất dài hơn
# một chút; không suy ra từ một sự cố đo được cụ thể như các giá trị
# MIN_REQUEST_INTERVAL_SECONDS cơ sở, chỉ là ước lượng - chỉnh thoải mái.
THROTTLE_REDIS_KEY_TMPL = "tiktok:adaptive_interval:{device_id}"
ADAPTIVE_INTERVAL_MAX_SECONDS = 15.0

# --- Thử lại/backoff
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0
# Cùng lý do như RETRY_BACKOFF_JITTER_SECONDS trong constants/facebook.py.
RETRY_BACKOFF_JITTER_SECONDS = 1.0

# Khác Facebook/Threads, endpoint này hoàn toàn không cần bootstrap doc_id/token qua trình
# duyệt - thứ duy nhất phải lấy từ một phiên trình duyệt thật, đã "được tin cậy" là bộ
# danh tính bên dưới (cookie + device_id + odin_id). Đã xác nhận bằng thử nghiệm trực
# tiếp: một session Playwright hoàn toàn mới (kể cả dùng Chromium thật, kể cả sau khi vào
# đúng trang hashtag và lấy được ttwid/msToken thật từ chính session đó) vẫn nhận response
# rỗng - kiểm tra độ tin cậy thiết bị của TikTok cho endpoint này cần lịch sử sử dụng thật
# tích luỹ, thứ mà một lần truy cập tự động không thể tạo ra. Tuy nhiên một bộ
# device_id/odin_id/verifyFp lấy từ một phiên trình duyệt thật đã ổn định thì dùng được
# mãi sau đó - mọi request khác với nó (kể cả phân trang) chỉ cần một chữ ký X-Gnarly mới
# tính, được sinh ở local cho từng request (xem signature/gnarly.py) mà không cần đụng tới
# trình duyệt nữa.
#
# Khác Facebook/Threads (UA được bắt mới từ trình duyệt thật ở mỗi lần bootstrap - xem
# STATIC_HEADER_FIELDS của constants/facebook.py), UA này được gán cứng và không bao giờ
# đụng tới trình duyệt, nên cũng không bao giờ tự cập nhật. Chrome thật ra phiên bản mới
# vài tuần một lần; nên tự tay nâng lên bản hiện hành mỗi quý hoặc tương đương.
#
# Đây là UA của *trình duyệt thật* - chỉ dùng ở chỗ thực sự có context Patchright/Chromium
# thật đang chạy (bắt đăng nhập trong auth/bootstrap.py; comments.py/channel_videos/
# search.py để Playwright tự báo UA thật thay vì ghi đè, nên chưa bao giờ gặp vấn đề này).
# Phiên bản Chrome chính của nó phải khớp với bản Chromium mà Patchright thực sự đóng gói
# (đã xác nhận thực tế 2026-09-16 qua một lần bắt thật: "HeadlessChrome/151.0.7922.34") -
# lệch ở đây là một trình duyệt thật nói dối về phiên bản của chính nó, cùng loại dấu
# hiệu như bug lệch của CURL_CFFI_UA bên dưới, chỉ là theo chiều ngược lại. KHÔNG dùng lại
# hằng này cho bất kỳ request ký bằng curl_cffi nào - xem CURL_CFFI_UA để biết vì sao
# chúng phải là hai hằng riêng, gắn với hai số phiên bản khác nhau, không liên quan.
STATIC_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"

# UA cho mọi request ký bằng curl_cffi (TikTokClient trong client.py). Phải luôn cùng phiên bản
# Chrome chính với CURL_CFFI_IMPERSONATE_TARGET bên dưới: UA khai một bản Chrome mà dấu vân tay
# TLS không khớp là một bug đã xác nhận thực tế (2026-09-17 - "chrome" trần + UA 151 nhận 200
# rỗng ở mọi request). Đổi cả hai cùng lúc. Nâng 131 -> 146 ngày 2026-10-13 (bản cao nhất
# curl_cffi 0.16 có) cùng lúc chuyển sang signer 5.3.2. Cũng là thứ được hash vào X-Gnarly/
# X-Dynosaur, nên luôn truyền đúng hằng này cho signer.
CURL_CFFI_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"

# Giá trị impersonate= của curl_cffi đi cặp với CURL_CFFI_UA ở trên.
CURL_CFFI_IMPERSONATE_TARGET = "chrome146"

# X-Bogus được JS của request_capture kiểm tra nhưng thực ra không chặn endpoint này - đã
# xác nhận với một request thật bắt được mà nó đã là "1" nguyên văn, và mọi lần phát lại
# thành công thử ở đây cũng giữ "1" mà không có vấn đề gì.
STATIC_X_BOGUS = "1"

# Các tham số tĩnh theo request khớp với một phiên web Chrome macOS thật - chúng mô tả lớp
# trình duyệt/thiết bị, không phải danh tính tin cậy cụ thể
# (device_id/odin_id/verifyFp/ttwid/msToken/user_is_login), nên gán cứng an toàn thay vì
# phải lấy từ tài khoản đã bắt. user_is_login cố ý KHÔNG có ở đây - xem
# TikTokClient._is_logged_in trong client.py - nó phụ thuộc vào việc cookie của chính tài
# khoản này có mang session đã đăng nhập thật (sessionid) hay không, đã xác nhận bằng thử
# trực tiếp là trả về nhiều kết quả hơn đáng kể cho mỗi hashtag so với cookie chỉ có tư
# cách khách.
#
# Cả dict này chỉ được gửi qua curl_cffi (xem TikTokClient._request trong client.py) -
# browser_version phải suy ra từ CURL_CFFI_UA, không phải STATIC_UA.
STATIC_PARAMS = {
    "aid": "1988",
    "app_language": "en",
    "app_name": "tiktok_web",
    "browser_language": "en-US",
    "browser_name": "Mozilla",
    "browser_online": "true",
    "browser_platform": "MacIntel",
    # navigator.appVersion - UA bỏ tiền tố "Mozilla/", đúng như trình duyệt thật gửi.
    "browser_version": CURL_CFFI_UA.removeprefix("Mozilla/"),
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
