# Vận hành tài khoản Facebook

Tài liệu cho người vận hành: một tài khoản Facebook đi từ lúc nhập vào tới lúc cào và gặp sự cố ra sao. Cập nhật
18/11/2026. Giá trị trong ngoặc là mặc định. Hạn mức, nhịp và giờ yên lặng sửa ở dashboard (Settings › Proxy); phần hạ tầng còn
lại (số làn, Kafka, Redis) nằm trong `/opt/spider-hub/.env`.

## 1. Nguyên tắc

1. **Proxy dân dụng Việt Nam tự đổi IP mỗi 60 giây** (chốt 18/11/2026; 13/11/2026 đã bỏ IP tĩnh vì vẫn bị ban). Không ghim
   tài khoản vào proxy: mỗi job lấy một proxy đang trống trong danh sách Facebook (mục 5). Không dùng IP datacenter - chưa có
   cách tự phân biệt, người vận hành phải tự kiểm tra.
2. **Bật account và bật lịch là được nhận việc.** Không cần số ngày nuôi, cờ chỉ nuôi hay khai báo client.
3. **Tải thấp và rải đều.** Có hạn mức request theo ngày và theo khung giờ, nghỉ giữa các job, không chạy trong giờ yên
   lặng của Meta.
4. **Không có tác vụ Facebook nào chạy ngoài consumer.** Mọi việc dùng tài khoản đều đi qua hàng đợi Kafka
   `crawl_requests`, do bộ lập lịch của cinemark-api xếp. Không có cron nào trên VPS mở Facebook
   (lượt làm mới token định kỳ đã bỏ từ 31/10/2026).

## 2. Thêm tài khoản mới

1. **Proxy trước.** Danh sách proxy nền tảng Facebook trên dashboard phải có ít nhất bằng số làn (`FACEBOOK_LANES`) cổng khác
   nhau - mỗi cổng chỉ phục vụ một tài khoản tại một thời điểm. Nhãn static/rotating chỉ để ghi chú, không còn chặn gì.
2. **Đăng nhập trên máy của người vận hành, không đăng nhập trên VPS.** Dùng Safari cửa sổ riêng tư hoặc một hồ sơ
   Chrome mới, mạng nhà, không VPN. Lướt 2-3 phút, xác nhận nếu Facebook hỏi.
3. **Lấy cookie và User-Agent.**
   - Cookie: extension Cookie-Editor → Export → Header String, hoặc chép header `Cookie` của một request facebook.com.
   - UA: `chrome://version`, hoặc header `User-Agent`.
   - Cookie cần có `c_user`, `xs`, `datr`. `sb` và `fr` không bắt buộc.
4. **Nhập vào dashboard.** Form thêm/sửa tài khoản có ô Cookie và ô User-Agent. Nhập hàng loạt dùng "Nhập tài
   khoản/proxy bằng AI" với định dạng đủ số cột, ví dụ `ID | Pass | 2FA | Cookie | Token | Mail | Pass Mail | BO_QUA |
   BO_QUA2 | Ten`. Cột lạ bị bỏ qua, nhưng dòng nào có nhiều trường hơn định dạng sẽ bị báo lỗi.
5. **Không ghim proxy** (ô chọn proxy trên dòng tài khoản đã bỏ 18/11/2026). Đăng nhập tay qua proxy của hệ thống:
   `python -m scripts.open_account_browser <id> --login` - script giữ chỗ một cổng trống cho tài khoản tới khi nhấn Ctrl+C.
6. **Không đăng xuất trên máy đã lấy cookie**, và không mở tài khoản đó trên máy mình song song với hệ thống.

Cookie bị từ chối ngay khi nhập nếu thiếu `c_user`/`xs`/`datr`, `c_user` không khớp tài khoản, thiếu UA, UA có build Chrome không tồn
tại, hoặc `xs` cũ hơn `FACEBOOK_COOKIE_MAX_AGE_DAYS` (180, chưa chứng minh là ngưỡng đúng). Không tốn request nào lên Facebook.

Cách UA Facebook được xử lý (xem [native session](facebook-native-session.md) và [HTTP fallback](facebook-sdk-fallback.md)):
- Giữ nguyên User-Agent gốc đã đi kèm phiên, không tự nâng phiên bản hoặc đổi hệ điều hành.
- Không đổi UA điện thoại/Safari thành Chrome để dùng lại cookie. Restore bằng browser cần runtime tương thích với phiên đã lưu.
- Thiếu UA gốc thì cần thiết lập metadata/phiên đúng nguồn trước khi chạy; không gán fingerprint ngẫu nhiên.
- HTTP fallback dùng cookie + UA gốc và giao thức SDK đã kiểm chứng, không cần mở browser để dựng query. Transport HTTP không được coi là có fingerprint giống browser.

Cookie dạng JSON xuất từ extension (EditThisCookie, Cookie-Editor) cũng nhập được. Thuộc tính cookie được dựng khớp
Set-Cookie thật của facebook.com: `datr/sb/fr/xs` là HttpOnly, SameSite=None.

## 3. Sẵn sàng crawl

Facebook không còn luồng nuôi phiên, bộ đếm ngày nuôi hay bước bàn giao client. Account đang bật được chọn khi tới lịch,
miễn là phiên và proxy hợp lệ. Cờ `nurture_only` và metadata client từ bản cũ không còn chặn Facebook.
Job nuôi Facebook còn tồn trong Kafka được bỏ qua, không mở trình duyệt.

## 4. Cào

**Công tắc** (cinemark-api): lịch cào Facebook (`crawl_schedules`) bật thì có:
- lượt bài mới theo giờ chạy của lịch;
- lượt đào bài cũ mỗi giờ;
- lượt cào bình luận (cần cả lịch bình luận Facebook);
- bài booking theo dõi.

Tắt lịch cào là ngừng xếp các lượt crawl theo lịch. Các lượt nền có tài khoản còn cần bật riêng `FACEBOOK_BACKGROUND_CRAWL_ENABLED`.

**Chọn từ khoá** (`app/services/crawl/crawl_lanes.py`):
- Phim ra rạp **tháng này hoặc tháng sau** được cào ở mọi lượt bài mới và xếp đầu hàng.
- Phim đang chiếu khác và phim sắp chiếu xa hơn chỉ cào ở lượt chẵn.
- Lượt đào xếp theo: tháng này/tháng sau → đang chiếu → sắp chiếu → phim cũ.

**Làn** (`social_crawler/runtime/lanes.py`):
- `FACEBOOK_LANES` (2) consumer song song, mỗi làn giữ cố định một tài khoản tới 05:30 sáng hôm sau.
- Hai làn không dùng chung tài khoản, không dùng chung proxy. Làn cũng không nhận tài khoản có cổng proxy vẫn đang bị tài
  khoản khác giữ: cổng được giữ thêm 15 phút sau request cuối.
- Làn không có tài khoản thì rút khỏi nhóm Kafka. Job bị giao lại do nhóm chia lại partition được nhận ra qua `run_id` và
  bỏ qua (`consumer/dispatch._already_done`).

**Hạn mức** (`social_crawler/runtime/search_budget.py`, cài ở dashboard, không phải `.env`):
- `fb_daily_request_cap` (400) request/tài khoản/ngày, chia `fb_fresh_share` (0.4) cho bài mới và phần còn lại
  cho đào bài cũ.
- Phần bài mới mở dần theo `fb_fresh_periods` (06, 09, 12, 15, 18, 21h), phần đào bài cũ mở dần mỗi giờ. Job bị bỏ
  qua với lý do "daily search cap reached" khi phần của khung giờ đã dùng hết. Đó là hành vi đúng.
- Tài khoản mới: 3 ngày cào đầu chỉ được 20%, 40%, 70% hạn mức (`fb_new_account_ramp`).
- Chạy lại sau khi nghỉ: tài khoản không crawl từ `fb_restart_idle_days` (3) ngày trở lên, hoặc chưa từng crawl, bắt đầu
  lại từ `fb_restart_daily_caps` (100, 150, 200, 300 request/ngày) rồi mới đủ hạn mức ngày. Tài khoản đang vừa là tài khoản
  mới vừa trong giai đoạn này dùng mức thấp hơn. Các số này là điểm xuất phát, chưa chứng minh là an toàn.
- Hết hạn mức ngày: tài khoản nghỉ tới `fb_cap_rest_until` (05:30).

**Nhịp** (dashboard › Settings › Proxy › "Nhịp job Facebook/Threads", từ 18/11/2026; trước đó là biến `.env`):
- Giờ yên lặng Meta `meta_quiet_hours` (23:30-05:30): không chạy job Facebook/Threads nào. Message Kafka nằm chờ, không mất:
  từ 18/11/2026 poll trong lúc dừng pause lại mọi partition mới nhận sau rebalance và seek về message lỡ đọc.
- Nghỉ `facebook_job_pause_min/max_seconds` (30-90 giây) giữa các job tìm kiếm, `facebook_comments_pause_*` (15-30) giữa job
  bình luận, `threads_job_pause_*` (10-30) cho Threads.
- Job tìm kiếm chờ quá `facebook_stale_job_hours` (6; làn bài mới 3, làn đào 2) thì bỏ, lượt sau xếp lại.

**Trang nguồn:** đọc không đăng nhập qua job `public_page` (proxy công khai); đường đọc bảng tin bằng tài khoản (`page_feed`) đã bỏ ngày 17/11/2026.

## 5. Proxy và IP

- Mọi proxy là proxy xoay; loại đang dùng tự đổi IP mỗi 60 giây (18/11/2026). Không ghim tài khoản vào proxy (bỏ 13/11/2026; cột
  `platform_accounts.assigned_proxy_id` còn trong DB nhưng không code nào đọc).
- Mỗi job có tài khoản (cào, đăng nhập, nuôi) lấy proxy dùng lâu nhất chưa dùng lại trong danh sách của nền tảng
  (`runtime/proxy_lease.py`) và **giữ chỗ cổng (host:port) đó** cho tài khoản tới khi job xong. Giữ theo cổng, không theo id
  dòng: hai dòng trỏ cùng cổng không bao giờ được giao cho hai tài khoản cùng lúc (trước 18/11/2026 khoá theo id nên có thể).
- Không có proxy trống thì job chờ và được xếp lại, **không bao giờ chạy bằng IP của VPS**, không mượn proxy nền tảng khác.
- Vì IP đổi theo phút, các request của cùng một phiên đi qua nhiều IP. IP thoát lúc đăng nhập được ghi lại
  (`account_egress`) để đối chiếu khi có checkpoint, không chặn.
- Đọc số liệu bài không cần đăng nhập (`post_metrics`) chỉ dùng proxy không thuộc danh sách Facebook/Threads và không trỏ cùng
  cổng với proxy nào của hai nền tảng đó.
- Kiểm tra proxy định kỳ: cron `social_crawler.health.proxy_health_check` mỗi 5 phút.

## 6. Trình duyệt

- Mọi chỗ mở trình duyệt dùng `browser_interaction.launch_chromium()`: bản Chromium đầy đủ ở chế độ headless mới
  (`BROWSER_CHROMIUM_CHANNEL=chromium`). Headless shell mặc định của Playwright lộ 9 dấu bot.
- Máy chạy cần `python -m patchright install chromium`. Thiếu thì lùi về headless shell và ghi log
  `chromium_channel_launch_failed`.
- UA và Client Hints của trình duyệt khớp với dấu TLS của đường gửi lại request (`core/fingerprint.py`).

## 7. Sự cố và cách xử lý

| Log / dấu hiệu | Ý nghĩa | Hệ thống tự làm | Người vận hành |
|---|---|---|---|
| `account_warning_detected` | Meta hiện cảnh báo "hành vi tự động" / "tạm thời bị chặn" | Cho nghỉ `META_WARNING_REST_HOURS` (12) giờ, báo Telegram | Theo dõi. Lặp lại thì giảm tải |
| `nurture_checkpoint`, `account_disabled_checkpoint_suspected` | Checkpoint (xác minh danh tính, selfie, Google) | Tắt tài khoản | Selfie/giấy tờ/Google không có mail: xoá tài khoản. "Xác nhận đây là bạn": đăng nhập tay |
| `nurture_session_dead`, `logged_out_session_detected` | Phiên bị Facebook đăng xuất | Đánh dấu `dead`. Auto-login đang tắt nên không tự đăng nhập lại | Đăng nhập lại trên browser thật, dán cookie mới, gỡ `dead` |
| `facebook_consent_page_seen` / `facebook_stuck_on_consent_page` | Facebook đưa phiên sang trang đồng ý cookie toàn màn hình (hay gặp với phiên vừa nhập cookie) | Tự bấm "Cho phép tất cả cookie" rồi quay lại trang định mở; không qua được thì báo Telegram, **không** đánh dấu chết | Mở tay qua noVNC hoặc trên máy mình, bấm đồng ý |
| `proxy_port_busy_other_account` | Cổng đang bị tài khoản khác giữ | Bỏ lượt/thử lại sau | Không cần làm gì. Lặp nhiều thì tách proxy |
| `facebook_lane_session_refresh_failed` | Làn không dựng được phiên cho tài khoản | Cho tài khoản nghỉ 2 giờ | Xem log ngay trước đó |
| `crawl_job_skipped ... daily search cap reached` | Hết phần hạn mức của khung giờ | Bỏ job, lượt sau xếp lại | Không cần làm gì |
| `request_skipped_already_done` | Kafka giao lại job đã chạy xong | Bỏ qua | Không cần làm gì |

**Không có đăng nhập tự động.** Auto-login và nút "Đăng nhập tay" đã xoá ngày 04/11/2026 (đăng nhập mật khẩu qua trình duyệt tự động bị từ chối
từ tháng 9). Phiên chỉ đến từ cookie của trình duyệt thật. `bootstrap --manual` vẫn dùng được từ dòng lệnh.

**Một trình duyệt, một tài khoản.** Cookie `datr` là mã thiết bị. Đăng nhập nhiều tài khoản trên cùng một trình duyệt
(cùng Safari, cùng hồ sơ Chrome) thì Facebook thấy chúng dùng chung một thiết bị: một tài khoản bị checkpoint kéo theo
cả nhóm. Mỗi tài khoản dùng một cửa sổ riêng tư mới hoặc một hồ sơ Chrome riêng.

## 8. Cron trên VPS (user `app`)

```
*/5 * * * *  ... .venv/bin/python -m social_crawler.health.proxy_health_check
15 * * * *   (cinemark-api) scripts.visobert_label
```

Đã gỡ ngày 31/10/2026, **không thêm lại**:
- Lượt làm mới token định kỳ: mở Facebook bằng tài khoản bất kỳ mỗi 4 giờ, kể cả trong giờ yên lặng.
- `scripts/daily_run.sh`: đã xoá khỏi repo.
- Dò sức khoẻ Facebook/Threads bằng tìm kiếm mỗi 20 phút: đã xoá khỏi `social_crawler.health.health_check` ngày 04/11/2026.

## 9. Biến môi trường đáng chú ý

> Hạn mức request Facebook/Threads (tên khoá `fb_*`, `threads_hourly_request_cap`) không còn là biến `.env` từ 17/11/2026: sửa ở dashboard (Settings › Proxy › "Hạn mức request Facebook/Threads"), lưu ở `proxy_settings`, cinemark-api là nơi ghi mặc định.

| Biến | Mặc định | Ghi chú |
|---|---|---|
| `FACEBOOK_LANES` | 2 | Không lớn hơn số partition của `crawl_requests` |
| `TIKTOK_LANES` | 1 | Lane TikTok song song; mỗi lane giữ một cổng riêng của nguồn `tiktok_synthetic_provider`. Không lớn hơn số partition của `crawl_requests` và số cổng chưa ghim của nguồn đó |
| `META_WARNING_REST_HOURS` | 12 | |
| `FACEBOOK_BROWSER_DIRECT` | tắt | 1 = bootstrap mở trình duyệt không proxy, chỉ để thử trên máy người vận hành |
| `FB_PAGE_FEED_TIME_FILTER` | 1 | 0 = lật bảng tin trang như cũ |
| `BROWSER_CHROMIUM_CHANNEL` | chromium | Rỗng = headless shell (lộ dấu bot) |
