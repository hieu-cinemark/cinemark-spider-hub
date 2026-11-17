# Luồng của spider-hub: cách đọc và cách sửa

Đọc file này TRƯỚC khi sửa code. Mỗi thư mục có README riêng; file này nối chúng lại thành một luồng.

## 1. Bức tranh toàn cảnh

```
                cinemark-api (scheduler / dashboard)
                      |  Kafka "crawl_requests"          Redis: crawl_priority:<nền tảng>, crawl_idle:facebook,
                      |  (message có "type", "keyword"…)        crawl_public_pages:{facebook,threads}, crawl_youtube:{search,comments}
                      v
      social_crawler/consumer/main.py  ->  loop.py  (mỗi nền tảng một vòng + các worker Redis)
                      |
                      v
              consumer/dispatch.py        bỏ qua nếu job đã xong / bị huỷ / đang drain; chọn job theo request["type"]
                      |
                      v
              consumer/jobs/<job>.py      xin tài khoản + proxy (runtime/), đảm bảo phiên (sessions.py)
                      |
        +-------------+--------------------+
        v                                  v
  tiến trình con `scrapy crawl`      chạy trong tiến trình (YouTube, public_page,
  (consumer/process.py)              threads_channel, post_metrics)
        |                                  |
        +----------------+-----------------+
                         v
   spiders/<nền tảng>/...  --KafkaPublisher-->  Kafka "raw_posts" / "raw_comments"  -->  cinemark-api (ingest_consumer)
```

Spider-hub KHÔNG ghi vào D1 và không phân loại nội dung: nó chỉ cào và gửi. Mọi quyết định "bài này liên quan phim nào, cảm
xúc gì" nằm ở cinemark-api ([../../cinemark-api/docs/FLOW.md](../../cinemark-api/docs/FLOW.md)).

## 2. Một request đi qua những file nào

1. **Nhận**: `consumer/loop.py` `_run_platform_consumer` đọc Kafka (Facebook chạy `FACEBOOK_LANES` làn song song cùng group).
   Giờ yên lặng của Meta, hạn mức giờ, hết tài khoản... làm vòng đọc tạm dừng/nhường partition ngay tại đây.
2. **Định tuyến**: `consumer/dispatch.py` đọc `request["type"]` (bảng loại -> job ở [../social_crawler/consumer/jobs/README.md](../social_crawler/consumer/jobs/README.md)).
3. **Điều kiện**: job kiểm tra drain/huỷ, xin tài khoản (`runtime/account_lease.py`, `runtime/pool.py`) và proxy (`runtime/proxy_lease.py`), áp hạn mức
   (`runtime/search_budget.py`, `facebook_pacing.py`, `facebook_run_budget.py`), đảm bảo phiên đăng nhập (`consumer/sessions.py`).
4. **Chạy**: `consumer/process.py` `_run_subprocess` mở `scrapy crawl <tên spider>`; log được chuyển tiếp, huỷ theo `run_id`.
5. **Cào**: spider gọi client của nền tảng (`spiders/<nền tảng>/auth|client`), `extract.py` biến phản hồi thô thành bản ghi phẳng,
   `KafkaPublisher.publish` đẩy `raw_posts` / `raw_comments`, bản ghi đã gửi vào tập chống trùng Redis.
6. **Kết quả**: tiến trình con thoát với mã thoát; `consumer/common._raise_for_returncode` đổi thành lỗi có nghĩa; `dispatch` ghi
   `crawl_run_done:<run_id>` để message giao lại không chạy lặp.

### Mã thoát của tiến trình con

| Mã | Hằng số | Ý nghĩa | Consumer làm gì |
|---|---|---|---|
| 0 | | Xong | Tính là hoàn thành |
| 75 | `PROXY_EXHAUSTED_EXIT_CODE` | Hết proxy dùng được | Xếp lại, chờ proxy |
| 76 | `SEARCH_PAUSED_EXIT_CODE` | Tìm từ khoá TikTok đang nghỉ (IP bị phạt) | Bỏ qua, phần hashtag vẫn chạy |
| 91 | `CRAWL_OUTCOME_FAILED_EXIT_CODE` | Spider báo thất bại (exception trong `start()`) | Báo lỗi |
| 92 | `CRAWL_OUTCOME_RETRYABLE_EXIT_CODE` | Không gửi được Kafka | Thử lại cả job (bài đã gửi được bỏ qua ở lần sau) |
| 93 | `CRAWL_OUTCOME_CANCELLED_EXIT_CODE` | Bị huỷ giữa chừng | Bỏ qua |

Mã 0 KHÔNG đủ để kết luận thành công: kết quả có cấu trúc nằm ở `core/crawl_outcome.py` (extension Scrapy ghi file kết quả).

## 3. Tài nguyên dùng chung

| Cái gì | Ở đâu | Đọc/ghi bởi |
|---|---|---|
| Tài khoản (`platform_accounts`), proxy (`platform_proxies`), cài đặt (`proxy_settings`), nguồn proxy (`proxy_providers`) | Postgres (Supabase) | `db/` (cinemark-api ghi phía dashboard) |
| Hàng đợi, khoá, cache phiên, tập chống trùng | Redis, tiền tố `social_crawler:` | `clients/redis.py` `RedisCache` |
| Bài, bình luận | Kafka `raw_posts`, `raw_comments` | `clients/kafka.py` `KafkaPublisher` |
| Lệnh cào | Kafka `crawl_requests` (+ `crawl_requests.facebook_public_comments`) | `consumer/loop.py` |

Cài đặt chỉnh được từ dashboard (`db/proxy_settings.py`, đọc bằng `get_setting("khoá")`) được cache 60 giây: đổi trên dashboard có
hiệu lực sau tối đa một phút, không cần restart.

`CRAWLER_PLATFORMS` (biến môi trường) quyết định tiến trình này phục vụ nền tảng nào (ví dụ VPS không chạy `facebook` có tài khoản
mà chạy trên Mac, xem [facebook-local.md](facebook-local.md)).

## 4. Đọc code theo thứ tự nào

1. `consumer/dispatch.py` (ngắn, thấy hết loại việc) -> `consumer/jobs/README.md`.
2. Một job tiêu biểu: `consumer/jobs/search.py` (TikTok/Threads/Facebook tìm kiếm) hoặc `consumer/jobs/youtube.py` (trong tiến trình).
3. Spider của nền tảng bạn quan tâm: `spiders/<nền tảng>/README.md` -> `features/<tính năng>/`.
4. `runtime/README.md` khi gặp "tại sao job này không được giao tài khoản / bị nghỉ".
5. `consumer/loop.py` cuối cùng (dài, nhiều trường hợp đặc biệt: giờ yên lặng, hạn mức giờ, khôi phục hàng đợi sau restart).

## 5. Công thức sửa thường gặp

| Muốn | Làm |
|---|---|
| Thêm loại việc mới | `consumer/jobs/<tên>.py` + nhánh `request.get("type") == ...` trong `dispatch.py` + cinemark-api gửi `type` đó (`publish_crawl_request`) |
| Thêm nền tảng | `spiders/<nền tảng>/` (client, features, items) + `constants/<nền tảng>.py` + job + `CRAWLER_PLATFORMS`; phía cinemark-api thêm mapper ở `app/services/crawl/platforms.py` |
| Cào thêm một trường của bài | `spiders/<nền tảng>/features/*/extract.py` + `items.py`; phía cinemark-api thêm vào mapper và (nếu lưu) bảng D1 |
| Đổi hạn mức / nghỉ | `runtime/search_budget.py` / `facebook_pacing.py`; nếu muốn chỉnh từ dashboard thì thêm khoá vào `db/proxy_settings.py` |
| Thêm cài đặt dashboard | Chỉ thêm ở cinemark-api (`ProxySettings` trong `app/schemas/settings.py`) + form dashboard; deploy cinemark-api trước, rồi đọc ở đây bằng `get_setting("khoá")`. Cập nhật `tests/fixtures/proxy_settings.json` |
| Spider bị chặn / trả rỗng | Xem log `*_empty_response`, `blocked_retrying_*`; TikTok tìm từ khoá bị phạt theo IP (`spiders/tiktok/search_pause.py`) |
| Đổi chữ ký TikTok | `spiders/tiktok/signature/`; so với bản cũ bằng đầu vào cố định (xem README thư mục đó) |

## 6. Kiểm thử và chạy local

```bash
pytest -q                                   # guard trong tests/conftest.py: không Postgres/Redis/Telegram thật
TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= python -m social_crawler.consumer.main   # chạy local KHÔNG gửi Telegram
scrapy crawl <tên spider> -a ...            # chạy một spider đơn lẻ (xem README gốc, mục Usage)
```

## 7. Bẫy đã gặp

- **Spider Scrapy nạp theo `name`**: công cụ tìm code chết sẽ báo lớp spider là "không dùng" - đừng xoá.
- **Chạy local gửi Telegram thật** nếu `.env` có token; test thì đã chặn, còn chạy consumer tay thì phải xoá biến như lệnh trên.
- **Cổng proxy "xoay vòng" có thể giữ nguyên IP** (đo 16/11/2026 trên proxy TikTok): đừng mặc định mỗi lần thử là một IP mới.
- **Chữ ký TikTok sai một bit** = phản hồi 200 body rỗng, không có lỗi nào khác.
- **Hai vòng gửi bài giống nhau** (`jobs/public_page.py`, `jobs/threads_channel.py`) khác nhau có chủ đích, xem README `consumer/jobs`.
- **Đăng nhập Facebook/Threads có đoạn trùng nhau** (`auth/bootstrap.py`) nhưng chỉ kiểm chứng được trên tài khoản thật: sửa một bên phải xem bên kia.

## 8. Log và cấu hình (từ 17/11/2026)

- **Log mặc định tiếng Việt**: mã `event` snake_case cố định + câu tiếng Việt ở `social_crawler/core/log_messages.py` (xem README `core/`).
  Thêm sự kiện = thêm một dòng vào `MESSAGES`; `tests/test_log_messages.py` bắt thiếu.
- **Không còn mặc định/cấu hình riêng ở đây**: `db/proxy_settings.py` chỉ đọc dòng `proxy_settings` do cinemark-api ghi. Các biến `.env` của proxy công
  khai (`FB_PUBLIC_PAGES_*`, `THREADS_CHANNEL_*`, `FB_LOGGED_OUT_*`, `FACEBOOK_PUBLIC_*`, `YOUTUBE_PROXY_REST_SECONDS`) đã chuyển vào đó và không còn
  được đọc. Còn một số núm `.env` khác (hạn mức Facebook, nhịp nghỉ, giờ yên lặng...) chưa chuyển.

## 9. Hàng đợi (từ 17/11/2026)

Cinemark-api giao việc qua 2 topic Kafka (`crawl_requests`, `crawl_requests.facebook_public_comments`) và 5 hàng đợi Redis. Mọi hàng đợi Redis dùng chung `runtime/job_queue.JobQueue` (claim -> ack; `requeue` khi chưa chạy được; `recover` khi khởi động lại), không còn viết lại từng kênh. Tên khoá khai bên cinemark-api ở `app/clients/crawl_queues.py` (bảng đối chiếu hai phía); phải khớp từng ký tự.
