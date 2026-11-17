# tests

Chạy `pytest -q` từ gốc repo. `conftest.py` có các guard tự bật cho MỌI test: không Postgres thật, không Redis thật, không Telegram
thật, không ngủ theo thời gian nghỉ thật - test nào cần thì tự mock. Test cho một module đặt tên `test_<module>.py`.

## Các file

| File | Việc nó làm |
|---|---|
| `conftest.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_account_session_policy.py` | Cổng cookie, chính sách IP và dòng thời gian (04/11/2026). |
| `test_account_warmup.py` | services/warmup.py: tài khoản mới phải nuôi đủ ACCOUNT_WARMUP_DAYS ngày trước khi crawler giao ra. |
| `test_account_warning.py` | accounts/account_warning.py: dò cảnh báo "hành vi tự động" / "tạm thời bị chặn" của Meta (30/10/2026). |
| `test_browser_launch.py` | browser_interaction.launch_chromium: mọi trình duyệt chặn WebRTC lộ IP thật qua UDP (02/11/2026). |
| `test_comments_extract.py` | Test hồi quy cho social_crawler.spiders.facebook.features.comments.extract, chạy trên một response danh sách comment thật bắt được (tests/fixtures/facebook_comments_response.json). |
| `test_consumer_platforms.py` | CRAWLER_PLATFORMS: mỗi máy chỉ chạy đúng vòng lặp được giao. |
| `test_consumer_reliability.py` | consumer/loop + dispatch (review 02/11/2026): job chỉ được coi là xong khi đã có kết cục - huỷ giữa chừng, xếp lại hỏng, job hết hạn và message cũ hơn lần Dừng đều không được mất dấu. |
| `test_cookie_import.py` | Nhập cookie Facebook (30/10/2026): định dạng extension, thuộc tính cookie thật, nâng UA giữ hệ điều hành. |
| `test_crawl_lanes.py` | Lịch crawl Facebook theo hai phần hạn mức (27/10/2026): search_budget (phần fresh/deep, trần theo giờ), deep_cursor (đào bài cũ lùi dần), tham số job theo lane và giới hạn trong client. |
| `test_crawl_outcome.py` | core/crawl_outcome + consumer/process: mã thoát 0 của Scrapy không đủ - consumer đọc kết quả có cấu trúc của lượt crawl. |
| `test_extract.py` | Test hồi quy cho social_crawler.spiders.facebook.features.search.extract, chạy trên một response tìm kiếm Facebook thật bắt được (tests/fixtures/facebook_search_response.json). |
| `test_facebook_enabled_accounts.py` | Tài khoản Facebook đang bật không cần làm ấm, cờ nuôi hay khai báo client. |
| `test_facebook_http_refresh.py` | Khôi phục qua HTTP không bao giờ mở trình duyệt hay phát lại phiên đã bị từ chối rõ ràng. |
| `test_facebook_http_transport_safety.py` | Các phát hiện review ngày 04/11/2026: header và cookie gửi đi, cách ly CDN, dò một phần và lỗi thiết lập. |
| `test_facebook_native_safety.py` | Danh tính Facebook gốc, kiểm tra sức khoẻ thụ động và kiểm soát request đóng khi lỗi. |
| `test_facebook_public_comments.py` | Giao thức pipeline ẩn danh, phân trang, cách ly transport và định tuyến consumer. |
| `test_facebook_public_page.py` | Đọc page không đăng nhập: phân loại, không dùng proxy gán tài khoản, chặn thì nghỉ proxy, chỉ gửi bài mới. |
| `test_facebook_request_pacing.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_facebook_sdk_fallback.py` | Chạy template đã ghim thật và runtime V8 mà không cần Facebook/Redis/DB. |
| `test_facebook_session_continuity.py` | Hồi quy vòng đời phiên. Không dùng Redis, database, proxy hay lưu lượng Facebook thật. |
| `test_facebook_sweep_windows.py` | facebook_search: khoảng ngày của lượt quét - các ngày gần nhất mỗi ngày một khoảng, phần cũ hơn gộp. |
| `test_facebook_video_comments.py` | Bài video/reel: FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery stream danh sách comment trong một chunk bị defer (@defer) dưới một đường dẫn khác với bài thường (đã xác nhận 2026-09-28). |
| `test_fingerprint_consistency.py` | Dấu vân tay trình duyệt phải KHỚP giữa đường trình duyệt và đường replay HTTP. |
| `test_hashtag_ai_fallback.py` | clients/ai.classify_hashtag_relevance: hỏi chat (nhà cung cấp AI đang chọn), tên phim khớp thì khỏi hỏi. |
| `test_kafka_publisher.py` | KafkaPublisher.publish (social_crawler/clients/kafka.py): mọi lỗi gửi đều trả False thay vì ném ra, để chỗ gọi gỡ dấu "đã thấy" và crawl tiếp - lần value_serializer sai 2026-10-06 từng làm sập cả spider. |
| `test_keyword_filter.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_lanes.py` | services.lanes: mỗi làn Facebook giữ một tài khoản riêng, không trùng; tài khoản hỏng thì làn đổi tài khoản. |
| `test_login_safety.py` | Review đăng nhập 02/11/2026: Facebook/Threads không tự chọn đường mạng khác, một tài khoản một thao tác, mọi đường xác thực dùng cùng trình duyệt, task nền không đăng nhập vào tài khoản đang nghỉ, mỗi POST thật đều bị đếm. |
| `test_migrate_proxy_providers.py` | scripts/migrate_proxy_providers.py - kế hoạch điền provider_key/provider_ref, dry-run không ghi. |
| `test_no_real_telegram.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_open_account_browser.py` | Script mở Chrome nuôi tay: đúng hồ sơ, đúng proxy, không mở hai tài khoản cùng IP. |
| `test_post_log.py` | social_crawler/spiders/common/post_log.py - một dòng log cho mỗi bài crawl được. |
| `test_post_metrics.py` | Job post_metrics: đọc đúng số của bài (kể cả trang reel lẫn reel gợi ý), phân loại bị chặn, cooldown proxy và rơi về page_feed cho bài không lấy được số. |
| `test_priority_lane.py` | consumer.loop: làn ưu tiên Redis là hàng đợi tin cậy - job dở được trả lại khi khởi động, cạn proxy thì xếp lại. |
| `test_proxy_provider.py` | clients/proxy_provider - cơ chế chung: link reset + key, xác nhận bằng IP đầu ra đo được. |
| `test_proxy_rotate.py` | CometGraphQLClient._rotate_proxy_after_network_error: reset IP cổng proxy từ lần lỗi mạng thứ 2, có giãn cách. |
| `test_quiet_hours.py` | consumer.loop: giờ yên lặng 23:30-05:30 (giờ VN) cho Facebook / Threads. |
| `test_rejected_session.py` | comet_graphql_client: phiên bị từ chối -> xoá cache + con trỏ active, cho tài khoản nghỉ, không phát lại. |
| `test_restart_ramp.py` | Hạn mức ngày tăng dần khi Facebook chạy lại sau một thời gian nghỉ. |
| `test_run_dedupe.py` | consumer/dispatch: message Kafka giao lại với run_id đã chạy xong thì bỏ qua (31/10/2026). |
| `test_search_budget.py` | services.search_budget: hạn mức request Facebook mỗi tài khoản mỗi ngày. |
| `test_sweep_days.py` | consumer.jobs.search._sweep_days_for: quét hằng ngày ngắn, đủ 60 ngày mỗi tuần một lần cho mỗi từ khoá. |
| `test_threads_comments_extract.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_threads_profiles.py` | features/profiles của Threads: parse response hovercard + worker hàng đợi (client/Redis giả, không gọi Threads thật). |
| `test_threads_public_channel.py` | Kênh Threads không đăng nhập: client gửi đủ cờ provider, lật trang dừng đúng mốc, job gửi bài source=page. |
| `test_threads_search_overrides.py` | threads graphql_client._search_overrides: biến "recent" (tab "Gần đây") giữ đúng kiểu giá trị của mẫu đã bắt. |
| `test_tiktok_combined_search.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_tiktok_identity.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_tiktok_lanes.py` | Lane TikTok: mỗi lane giữ port proxy riêng; một lane (mặc định) giữ hành vi cũ. |
| `test_tiktok_paging.py` | TikTok: thử lại lần gọi có ký đầu tiên, cỡ trang comment, đổi danh tính khi lật trang video kênh. |
| `test_tiktok_reply_isolation.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_tiktok_search_pause.py` | (chưa có mô tả đầu file - đọc trực tiếp) |
| `test_youtube.py` | YouTube (logic chép từ data-miner): đọc số, thời gian tương đối, Shorts, token trang, định tuyến job. |
| `test_youtube_transcripts.py` | (chưa có mô tả đầu file - đọc trực tiếp) |

## Muốn sửa / thêm thì

- Test cần Redis giả: monkeypatch `RedisCache` hoặc dùng `fakeredis`; cần Postgres giả: monkeypatch hàm trong `social_crawler/db/`.

## Bản chụp cấu hình cào

`fixtures/proxy_settings.json` là giá trị mặc định của `ProxySettings` (cinemark-api) - test đọc bản này thay vì Postgres.
Khi cinemark-api thêm/đổi khoá, chụp lại:

```bash
cd ../cinemark-api && .venv/bin/python -c "import json; from app.schemas.settings import ProxySettings; \
print(json.dumps(ProxySettings().model_dump(), indent=1))" > ../spider-hub/tests/fixtures/proxy_settings.json
```
