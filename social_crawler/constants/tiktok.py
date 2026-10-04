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

# Feed video đã đăng của một kênh/người dùng (phân trang qua cursor, định danh bằng
# secUid) - dùng bởi features/channel_videos. Khác HASHTAG_ITEM_LIST_URL, endpoint này
# THỰC SỰ cần X-Dynosaur do trình duyệt thật tính - đã xác nhận bằng A/B test trực tiếp
# thực tế (2026-09-16), không phải suy ra từ trường hợp hashtag hay mang sang từ ghi chú
# của search/comments:
#
#   1. Một request thật bắt được (device_id/cookie/X-Gnarly/X-Dynosaur đều thật, từ một
#      phiên trình duyệt thật) phát lại nguyên văn qua curl_cffi trả về dữ liệu thật
#      (itemList 16 video) - bản thân bản bắt được không bị cũ.
#   2. Đúng URL đó, giống từng byte trừ việc xoá hẳn tham số X-Dynosaur, trả về 200 rỗng.
#      Thay bằng một giá trị X-Dynosaur sai rõ ràng thay vì xoá: cũng rỗng. Không có gì
#      khác của request thay đổi - cùng X-Gnarly thật, cùng thứ tự tham số, cùng
#      WebIdLastTime, cùng cookie.
#   3. Phát lại ngay URL gốc chưa sửa (không đổi X-Dynosaur) vẫn chạy - loại trừ khả năng
#      "danh tính/IP này bị giới hạn/chặn giữa lúc test" như một cách giải thích khác cho
#      kết quả rỗng ở bước 2. Kết quả rỗng thật sự là do việc sửa X-Dynosaur.
#
#   Riêng ra, dựng lại request từ đầu (dict tham số kiểu STATIC_PARAMS của project này +
#   một X-Gnarly mới tính ở local), kể cả khi giữ mọi trường danh tính VÀ X-Dynosaur thật
#   bắt được nguyên văn, vẫn trả về rỗng - tức là một request dựng lại ở local không
#   tương đương với request của trình duyệt cho endpoint này ngay cả trước khi đụng tới
#   X-Dynosaur (khác HASHTAG_ITEM_LIST_URL, nơi cách ký ở local chạy tốt). Project này
#   không có bản cài đặt X-Dynosaur ở local nào đã được xác nhận tạo ra giá trị mà server
#   TikTok thực sự chấp nhận (một lần thử dịch ngược trước đó, signature/dynasaur.py, đã
#   bị xoá 2026-09-17 - code chết, chưa bao giờ được nối vào client nào, và cách dựng của
#   nó chưa bao giờ được kiểm chứng với một giá trị thật bắt được) - nên endpoint này chỉ
#   dùng được qua trình duyệt, giống COMMENT_ITEM_LIST_URL (dù xem ghi chú của hằng đó -
#   "chỉ qua trình duyệt" ở đó hoá ra vẫn chạy *headless* được, khi đã có proxy ngoài VN
#   và cú click gửi bằng JS) và khác HASHTAG_ITEM_LIST_URL, vốn chỉ *trông như* cần trình
#   duyệt cho tới khi tìm ra và sửa một bug trong dict tham số. COMMENT_ITEM_LIST_URL sau
#   đó đã mở được bằng X-Dynosaur tính ở local (2026-09-18) — xem
#   features/comments/comments.py; ghi chú về Dynosaur ở trên vẫn áp dụng cho
#   post/item_list.
POST_ITEM_LIST_URL = "https://www.tiktok.com/api/post/item_list/"

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

# UA cho mọi request ký bằng curl_cffi (TikTokClient trong client.py - item_list của
# hashtag_search; comment đã chuyển hẳn sang dùng trình duyệt, xem docstring module của
# features/comments/comments.py, còn TikTokCommentClient, bản REST tương đương không dùng
# tới, đã bị xoá 2026-09-17). Trước đây đây chỉ là STATIC_UA (khai là Chrome/151.0.0.0)
# trong khi mọi Session curl_cffi trong client.py được tạo với alias trần
# `impersonate="chrome"` - một bug thật, đã xác nhận thực tế (2026-09-17), không phải giả
# định: bản curl_cffi đã cài hoàn toàn không có dấu vân tay TLS "chrome151" (cao nhất của
# BrowserType là chrome146), nên "chrome" trần âm thầm quay về ClientHello của một phiên
# bản khác không liên quan trong khi header UA và tham số browser_version đã ký X-Gnarly
# vẫn khai 151 - một chỗ lệch dấu vân tay TLS/UA có mặt trên đúng từng request curl_cffi
# mà project này từng gửi, khác với một trình duyệt thật (Patchright) vốn không thể tạo ra
# loại dấu hiệu này. Đã xác nhận thực tế: 8 danh tính/IP synthetic mới liên tiếp đều nhận
# response rỗng với cặp "chrome" trần + 151.0.0.0 cũ; chuyển sang đúng cặp khớp này (TLS
# chrome131 + UA 131) thành công ngay lần thử đầu, không cần thử lại - xem
# CURL_CFFI_IMPERSONATE_TARGET trong client.py cho giá trị impersonate= đi kèm, vốn phải
# luôn ghi cùng phiên bản Chrome với phần Chrome/NNN của chuỗi UA này. Nâng một cái mà
# không nâng cái kia chính là bug mà comment này ghi lại - đổi cả hai cùng lúc, và chỉ sau
# cùng kiểu A/B test thực tế đã bắt được lỗi này.
CURL_CFFI_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

# Giá trị impersonate= của curl_cffi đi cặp với CURL_CFFI_UA ở trên - phải luôn ghi cùng
# phiên bản Chrome chính với phần Chrome/NNN của UA đó (xem docstring của nó để biết lý
# do). Truyền vào curl_requests.Session(impersonate=...) trong client.py thay vì alias
# "chrome" trần, vốn là thứ đã âm thầm lệch ngay từ đầu.
CURL_CFFI_IMPERSONATE_TARGET = "chrome131"

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
# browser_version phải là CURL_CFFI_UA, không phải STATIC_UA, nếu không sẽ quay lại đúng
# chỗ lệch dấu vân tay TLS/UA mà docstring của CURL_CFFI_UA mô tả.
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

# Thứ tự key trong query string cho các request /api/challenge/* ký bằng curl_cffi. Đã xác
# nhận thực tế 2026-09-17: TikTok trả HTTP 200 + body rỗng khi cùng các giá trị tham số
# được urlencode sai thứ tự (thứ tự chèn dict của Python 3.7+ từ
# `{**STATIC_PARAMS, **extra, device_id, ...}` — "prod_client_order" trong bộ A/B). Phát
# lại thứ tự parse_qsl của trình duyệt thật với giá trị giống hệt thì chạy; sắp theo chữ
# cái / đảo ngược / prod_client_order đều rỗng. Bắt từ Patchright trên
# /api/challenge/item_list/ (khách). challengeName nằm cạnh challengeID để
# /api/challenge/detail/ dùng chung được danh sách này. Key không có trong danh sách được
# nối thêm theo thứ tự chèn của chỗ gọi sau các key này.
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
