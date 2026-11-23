# social_crawler

Package Python của spider-hub: nhận lệnh cào từ cinemark-api, chạy spider, đẩy bài và bình luận lên Kafka cho
cinemark-api xử lý. Không có HTTP server. Điểm vào duy nhất của production là `python -m social_crawler.consumer.main`
(service systemd `spider-hub-crawl-consumer`). Tài liệu luồng đầy đủ: [../docs/FLOW.md](../docs/FLOW.md).

## Các file

| File | Việc nó làm |
|---|---|
| `settings.py` | Cấu hình Scrapy (tên bot, reactor, FEEDS, extension, throttle) và vài biến môi trường dùng chung (Telegram, mức log). |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`accounts/`](accounts/README.md) | Nuôi, kiểm tra và bảo vệ tài khoản trong pool |
| [`clients/`](clients/README.md) | Client ra ngoài: Kafka, Redis, AI, Telegram, nguồn proxy |
| [`constants/`](constants/README.md) | Hằng số theo nền tảng: URL, khoá Redis, mã thoát, ngưỡng |
| [`consumer/`](consumer/README.md) | Nhận lệnh cào từ Kafka/Redis và chạy job |
| [`core/`](core/README.md) | Nền móng: log, nạp .env, kết quả tiến trình con, fingerprint |
| [`db/`](db/README.md) | Truy cập D1: tài khoản, proxy, cài đặt |
| [`health/`](health/README.md) | Kiểm tra sức khoẻ chủ động của đường cào và proxy |
| [`runtime/`](runtime/README.md) | Tài khoản, proxy, làn chạy song song, hạn mức chống checkpoint |
| [`spiders/`](spiders/README.md) | Cách cào từng nền tảng: Facebook, Threads, TikTok, YouTube |

## Luồng

```
cinemark-api --Kafka crawl_requests--> consumer/loop.py --> consumer/dispatch.py --> consumer/jobs/<job>.py
                                                                                        |
                                              chạy spider (tiến trình con Scrapy, hoặc trong tiến trình)
                                                                                        |
                          spiders/<nền tảng>/... --KafkaPublisher--> Kafka raw_posts / raw_comments --> cinemark-api
```

Trạng thái dùng chung nằm ở Redis (hàng đợi, khoá, cache phiên, dedupe) và D1 (tài khoản, proxy, cài đặt
sửa từ dashboard). Spider không tự quyết dùng tài khoản/proxy nào: xin qua `runtime/`.

## Muốn sửa / thêm thì

- Thêm một loại việc mới: viết `consumer/jobs/<tên>.py`, đăng ký loại request trong `consumer/dispatch.py`.
- Thêm/sửa cách cào một nền tảng: `spiders/<nền tảng>/features/<tính năng>/`.
- Thêm cài đặt chỉnh được từ dashboard: `db/proxy_settings.py` (mặc định) + `ProxySettings` bên cinemark-api + form dashboard.
- Thêm biến môi trường: `.env.example` + chỗ đọc (`settings.py` hoặc module dùng nó).
