# Vận hành tài khoản Facebook

Tài liệu cho người vận hành: một tài khoản Facebook đi từ lúc nhập vào tới lúc cào, nuôi, gặp sự cố ra sao. Cập nhật
31/10/2026. Giá trị trong ngoặc là mặc định trong code. Ghi đè bằng biến môi trường trong `/opt/spider-hub/.env`
(spider-hub) hoặc `/opt/cinemark-api/.env` (cinemark-api).

## 1. Nguyên tắc

1. **Mỗi tài khoản một IP dân dụng Việt Nam, cố định.** Meta nối các tài khoản Facebook/Threads với nhau qua IP. Không
   dùng IP datacenter, cũng không dùng proxy tự đổi IP theo phút.
2. **Tài khoản mới phải được nuôi trước khi cào.** Nuôi là lướt bảng tin, like, mở bài như người thật. Việc đầu tiên một
   cookie vừa chuyển sang máy/IP khác làm không bao giờ được là tìm kiếm.
3. **Tải thấp và rải đều.** Có hạn mức request theo ngày và theo khung giờ, nghỉ giữa các job, không chạy trong giờ yên
   lặng của Meta.
4. **Không có tác vụ Facebook nào chạy ngoài consumer.** Mọi việc dùng tài khoản đều đi qua hàng đợi Kafka
   `crawl_requests`, do bộ lập lịch của cinemark-api xếp. Không có cron nào trên VPS mở Facebook
   (`scripts/refresh_token.sh` đã ngừng dùng từ 31/10/2026).

## 2. Thêm tài khoản mới

1. **Proxy trước.** Thêm proxy dân dụng IP cố định trên dashboard, nền tảng Facebook.
2. **Đăng nhập trên máy của người vận hành, không đăng nhập trên VPS.** Dùng Safari cửa sổ riêng tư hoặc một hồ sơ
   Chrome mới, mạng nhà, không VPN. Lướt 2-3 phút, xác nhận nếu Facebook hỏi.
3. **Lấy cookie và User-Agent.**
   - Cookie: extension Cookie-Editor → Export → Header String, hoặc chép header `Cookie` của một request facebook.com.
   - UA: `chrome://version`, hoặc header `User-Agent`.
   - Cookie cần có `c_user`, `xs`, `datr`. `sb` và `fr` không bắt buộc.
4. **Nhập vào dashboard.** Form thêm/sửa tài khoản có ô Cookie và ô User-Agent. Nhập hàng loạt dùng "Nhập tài
   khoản/proxy bằng AI" với định dạng đủ số cột, ví dụ `ID | Pass | 2FA | Cookie | Token | Mail | Pass Mail | BO_QUA |
   BO_QUA2 | Ten`. Cột lạ bị bỏ qua, nhưng dòng nào có nhiều trường hơn định dạng sẽ bị báo lỗi.
5. **Ghim proxy ngay.** Dùng ô chọn proxy trên dòng tài khoản. Nếu không ghim, lần dùng đầu tài khoản tự vào proxy
   Facebook đang ít tài khoản nhất, có thể là proxy đã có tài khoản khác.
6. **Không đăng xuất trên máy đã lấy cookie**, và không mở tài khoản đó trên máy mình song song với hệ thống.

Cách UA được xử lý (`spiders/facebook/auth/cookies.extract_user_agent`, `core/fingerprint.upgrade_user_agent`):
- Chrome/Edge/Opera bản khác trên Windows/macOS: nâng lên đúng bản Chromium đang chạy, **giữ hệ điều hành**.
- UA điện thoại: bị bỏ qua, dùng hồ sơ thiết bị theo tài khoản (`device_profile`).
- Safari: không giả được. Khi nhập, ghi UA là Chrome cùng hệ điều hành.

Cookie dạng JSON xuất từ extension (EditThisCookie, Cookie-Editor) cũng nhập được. Thuộc tính cookie được dựng khớp
Set-Cookie thật của facebook.com: `datr/sb/fr/xs` là HttpOnly, SameSite=None.

## 3. Nuôi tài khoản

**Lịch** (cinemark-api, `app/services/crawl/scheduler._nurture_tick`):
- `FACEBOOK_NURTURE_TIMES` (`08:00,20:00`), cộng độ lệch ngẫu nhiên 0-`FACEBOOK_NURTURE_JITTER_MINUTES` (50) phút. Độ
  lệch cố định trong ngày và khác nhau giữa các ngày.
- Chạy **kể cả khi lịch cào Facebook đang tắt**.
- Cờ "nuôi trước/sau lượt cào" của lịch cào vẫn còn tác dụng. Chạy trùng không sao, vì có hạn mức theo buổi (bên dưới).

**Một lượt nuôi** (spider-hub, `social_crawler/accounts/nurture.py`, job `type=nurture`):
- Tối đa `NURTURE_POOL_LIMIT` (6) tài khoản mỗi lượt, tài khoản đang nuôi trước. Chờ 45-240 giây trước tài khoản đầu,
  nghỉ 2-5 phút giữa các tài khoản.
- Bỏ qua tài khoản đang bị đánh dấu chết, tài khoản đang thuộc một làn cào (để không chạy 2 phiên song song), và tài khoản
  đã nuôi trong buổi/ngày. Tài khoản đang nuôi được 2 phiên/ngày (sáng/chiều), tài khoản đã ấm 1 phiên/ngày.
- Mỗi phiên: kiểm tra proxy tới được Facebook → mở bảng tin bằng phiên đã lưu (hoặc dựng từ cookie trong DB) → lướt →
  like 1-3 bài → mở 3 bài → từ ngày nuôi thứ 2 thì theo dõi một trang rạp và tìm một câu tự nhiên → lưu phiên.
  Không bình luận.
- Tài khoản bị bỏ vì cổng proxy đang bận được **thử lại một lần** ở cuối lượt.
- Chỉ phiên thành công mới được tính ngày nuôi (`warmup.record_nurture_day`).

**Giai đoạn nuôi** (`social_crawler/accounts/warmup.py`):
- Tài khoản thêm từ 27/10/2026 cần **1 ngày** nuôi thành công rồi mới được làn cào nhận.
- `ACCOUNT_WARMUP_DAYS=0` **tắt hẳn** giai đoạn nuôi. Không đặt biến này trong `.env`.
- Cờ `nurture_only` trên tài khoản: chỉ nuôi, không bao giờ cào.

## 4. Cào

**Công tắc** (cinemark-api): lịch cào Facebook (`crawl_schedules`) bật thì có:
- lượt bài mới theo giờ chạy của lịch;
- lượt đào bài cũ mỗi giờ;
- lượt cào bình luận (cần cả lịch bình luận Facebook);
- bài booking theo dõi.

Tắt lịch cào là tắt cả bốn. Nuôi không bị ảnh hưởng.

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

**Hạn mức** (`social_crawler/runtime/search_budget.py`):
- `FACEBOOK_DAILY_REQUEST_CAP` (400) request/tài khoản/ngày, chia `FACEBOOK_FRESH_SHARE` (0.4) cho bài mới và phần còn lại
  cho đào bài cũ.
- Phần bài mới mở dần theo `FACEBOOK_FRESH_PERIODS` (06, 09, 12, 15, 18, 21h), phần đào bài cũ mở dần mỗi giờ. Job bị bỏ
  qua với lý do "daily search cap reached" khi phần của khung giờ đã dùng hết. Đó là hành vi đúng.
- Tài khoản mới: 3 ngày cào đầu chỉ được 20%, 40%, 70% hạn mức (`FACEBOOK_NEW_ACCOUNT_RAMP`).
- Hết hạn mức ngày: tài khoản nghỉ tới `FACEBOOK_CAP_REST_UNTIL` (05:30).

**Nhịp:**
- Giờ yên lặng Meta `META_QUIET_HOURS` (23:30-05:30): không chạy job Facebook/Threads nào.
- Nghỉ 30-90 giây giữa các job tìm kiếm.

**Trang nguồn** (`spiders/facebook/features/page_feed`): xin Facebook chỉ trả bài đăng sau lần cào xong gần nhất của
trang, lùi 6 giờ phòng bài đăng trễ (`afterTime/beforeTime`, `FB_PAGE_FEED_TIME_FILTER`). Trang không có bài mới chỉ tốn 1
request.

## 5. Proxy và IP

- Mỗi tài khoản ghim một proxy (`platform_accounts.assigned_proxy_id`). Nuôi, cào và đăng nhập đều đi qua đúng proxy đó.
  Không có proxy dùng được thì bỏ lượt, **không bao giờ chạy bằng IP của VPS**.
- Cổng webtui (có API đổi IP): nhiều tài khoản dùng chung một cổng được, vì mỗi lần cổng chuyển sang tài khoản khác thì IP
  được đổi trước, và IP từng thuộc tài khoản khác trong 7 ngày sẽ không được dùng lại (`runtime/ip_switch.py`). Cổng
  không có API đổi IP: chỉ một tài khoản.
- Đọc số liệu bài không cần đăng nhập (`post_metrics`) chỉ dùng proxy không chở tài khoản Meta nào.
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
| `nurture_session_dead`, `logged_out_session_detected` | Phiên bị Facebook đăng xuất | Đánh dấu `dead`. Auto-login thử đăng nhập lại qua proxy đã ghim | Với tài khoản mua: bật `needs_manual_login` để chặn auto-login, đăng nhập lại trên máy mình, dán cookie mới, gỡ `dead` |
| `facebook_consent_page_seen` / `facebook_stuck_on_consent_page` | Facebook đưa phiên sang trang đồng ý cookie toàn màn hình (hay gặp với phiên vừa nhập cookie) | Tự bấm "Cho phép tất cả cookie" rồi quay lại trang định mở; không qua được thì báo Telegram, **không** đánh dấu chết | Mở tay qua noVNC hoặc trên máy mình, bấm đồng ý |
| `proxy_port_busy_other_account` | Cổng đang bị tài khoản khác giữ | Bỏ lượt/thử lại sau | Không cần làm gì. Lặp nhiều thì tách proxy |
| `facebook_lane_session_refresh_failed` | Làn không dựng được phiên cho tài khoản | Cho tài khoản nghỉ 2 giờ | Xem log ngay trước đó |
| `crawl_job_skipped ... daily search cap reached` | Hết phần hạn mức của khung giờ | Bỏ job, lượt sau xếp lại | Không cần làm gì |
| `request_skipped_already_done` | Kafka giao lại job đã chạy xong | Bỏ qua | Không cần làm gì |

Đăng nhập tay trên VPS: nút "Đăng nhập tay" trên dashboard, xem qua noVNC. Bước nhận proxy và đổi IP mất tới 1-2 phút,
màn hình noVNC đen trong lúc đó. Cổng đang bận với tài khoản khác thì không mở được.

**Một trình duyệt, một tài khoản.** Cookie `datr` là mã thiết bị. Đăng nhập nhiều tài khoản trên cùng một trình duyệt
(cùng Safari, cùng hồ sơ Chrome) thì Facebook thấy chúng dùng chung một thiết bị: một tài khoản bị checkpoint kéo theo
cả nhóm. Mỗi tài khoản dùng một cửa sổ riêng tư mới hoặc một hồ sơ Chrome riêng.

## 8. Cron trên VPS (user `app`)

```
*/5 * * * *  ... .venv/bin/python -m social_crawler.health.proxy_health_check
15 * * * *   (cinemark-api) scripts.visobert_label
```

Đã gỡ ngày 31/10/2026, **không thêm lại**:
- `scripts/refresh_token.sh`: mở Facebook bằng tài khoản bất kỳ mỗi 4 giờ, kể cả trong giờ yên lặng.
- `scripts/daily_run.sh`: đã xoá khỏi repo.
- `social_crawler.health.health_check`: tìm kiếm Facebook mỗi 20 phút bằng phiên đang dùng.

## 9. Biến môi trường đáng chú ý

| Biến | Mặc định | Ghi chú |
|---|---|---|
| `ACCOUNT_WARMUP_DAYS`, `ACCOUNT_WARMUP_DAYS_STRICT` | 1, 1 | **Không đặt 0** |
| `FACEBOOK_DAILY_REQUEST_CAP` | 400 | 0 = tắt hạn mức ngày (không khuyến khích) |
| `FACEBOOK_NEW_ACCOUNT_RAMP` | 0.2,0.4,0.7 | |
| `FACEBOOK_LANES` | 2 | Không lớn hơn số partition của `crawl_requests` |
| `META_QUIET_HOURS` | 23:30-05:30 | |
| `META_WARNING_REST_HOURS` | 12 | |
| `NURTURE_POOL_LIMIT` | 6 | Tài khoản mỗi lượt nuôi |
| `FB_PAGE_FEED_TIME_FILTER` | 1 | 0 = lật bảng tin trang như cũ |
| `BROWSER_CHROMIUM_CHANNEL` | chromium | Rỗng = headless shell (lộ dấu bot) |
| `FACEBOOK_NURTURE_TIMES` (cinemark-api) | 08:00,20:00 | |
| `FACEBOOK_NURTURE_JITTER_MINUTES` (cinemark-api) | 50 | |
