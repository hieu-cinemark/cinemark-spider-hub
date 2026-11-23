# Chuẩn log và lỗi

Một quy tắc chung cho cinemark-api, spider-hub và Worker cinemark: **log tiếng Việt, lỗi tiếng Anh, cùng một cấu trúc**.
`tests/test_logging_standard.py` quét mã nguồn `social_crawler/`, nên lệch chuẩn là CI đỏ. Bản đầy đủ và bản gốc của chuẩn này nằm ở
`cinemark-api/docs/logging.md`; hai repo giữ cùng nội dung.

## 1. Log (tiếng Việt)

Mỗi sự kiện là một dòng, gồm **mã sự kiện** cố định bằng tiếng Anh và **câu tiếng Việt** cho người đọc:

```python
logger.warning("proxy_degraded", platform=platform, proxy_url=url, error=exc)
```
```
2026-11-23T04:39:52Z [warning] proxy_degraded  Proxy suy giảm, tạm nghỉ  platform=facebook proxy_url=… error=timeout error_type=ReadTimeout service=spider-hub
```

| Trường | Ý nghĩa |
|---|---|
| `timestamp`, `level`, `service`, `logger` | Tự thêm. Mọi dòng đều có. |
| `event` | Mã sự kiện, `snake_case`, tiếng Anh, **literal** (không f-string, không biến). |
| `msg` | Câu tiếng Việt, tự thêm từ `social_crawler/core/log_messages.py`. Ở console nằm ngay sau mã sự kiện. |
| `request_id` / `run_id` | Tự thêm theo ngữ cảnh. |
| `error`, `error_type`, `error_code` | Chỉ xuất hiện khi có lỗi, xem mục 3. |
| còn lại | Dữ liệu thay đổi (`platform`, `movie_id`, `attempt`...) đặt vào key=value, **không ghép vào câu**. |

**Đặt mã sự kiện:** `<đối tượng>_<hành động ở dạng đã xong>`, hậu tố cố định: `_started`, `_finished`, `_failed`, `_skipped`,
`_blocked`, `_timeout`, `_updated`, `_deleted`. Phần thay đổi theo ngữ cảnh (tên job, tên nhịp) đặt vào trường
(`health_check_failed check=redis`), không đưa vào mã.

**Câu tiếng Việt trong catalog:** có dấu, không chấm cuối, tối đa 90 ký tự. Mở đầu bằng tiền tố miền (`Facebook:`, `Omar:`, `D1:`) hoặc
`Đã` / `Không` / `Bỏ`. Câu của sự kiện `_failed` phải nói rõ lỗi (`Không ... được`, `... thất bại`). Catalog chia nhóm
`# --- tên nhóm ---` và xếp theo chữ cái trong nhóm.

**Thêm một sự kiện:** viết `logger.info("ten_su_kien", ...)`, thêm một dòng vào `MESSAGES` (đúng nhóm, đúng thứ tự), chạy test.

**Mức log:**

| Mức | Dùng khi |
|---|---|
| `debug` | Chẩn đoán, bản ghi hỏng bị bỏ qua (`json_decode_skipped`), lỗi từng phần không ảnh hưởng kết quả. |
| `info` | Mốc bình thường: bắt đầu, xong, đã cập nhật. |
| `warning` | Lỗi tạm thời đã retry hoặc phần việc suy giảm nhưng hệ thống vẫn chạy. |
| `error` | Thao tác hỏng, cần người xử lý (kèm `telegram=True` khi cần báo). |

## 2. Lỗi (tiếng Anh)

Mọi exception và message lỗi **bằng tiếng Anh**: câu đầy đủ, viết hoa đầu, không chấm cuối, có định danh khi hữu ích.

```python
raise D1Error(f"Missing environment variable {name} (CONFIG_DB_BACKEND=d1)")
raise SettingsUnavailable(f"proxy_settings is missing key {key!r} - deploy/restart cinemark-api so it writes the new key")
```

- Lỗi của spider-hub chỉ đi vào log, exit code hoặc `last_error` hiển thị trên dashboard; không có body HTTP. Tên class lỗi nghiệp vụ
  (`CrawlJobSkipped`, `CheckpointRequiredError`, `ProxyPoolExhaustedError`...) là mã máy đọc được, message là tiếng Anh.
- Chuỗi hiển thị cho người vận hành không phải exception (ví dụ trạng thái `last_status` của nguồn proxy) không thuộc quy tắc này.

## 3. Ghi lỗi vào log

Luôn truyền **chính exception** vào `error=`:

```python
except SomeError as exc:
    logger.warning("some_thing_failed", platform=platform, error=exc)
```

Bộ xử lý log tự điền `error` (nội dung, cắt ở 300 ký tự), `error_type` (tên class) và `error_code` (thuộc tính `code` của lỗi ứng dụng, nếu
có). Vì vậy **không** viết `error=str(exc)`, `error=str(exc)[:200]`, `error=repr(exc)`, và không dùng `exc=`, `err=`, `message=`, `detail=`.
Trường `reason` chỉ dành cho lý do nghiệp vụ không phải exception (`reason="content_too_short"`).

## 4. Không nuốt lỗi im lặng

`except ...: pass` / `continue` phải có chú thích lý do ngay cạnh, hoặc thay bằng log/raise:

```python
except WebSocketDisconnect:
    pass  # client đóng WebSocket: kết thúc bình thường
```

Đọc JSON từ Redis dùng `social_crawler.core.jsonutil.loads_or_none(raw, source="...")`: bản hỏng được log `json_decode_skipped` (debug)
thay vì biến mất.

## 5. Ngoại lệ có lý do

- `print()` chỉ trong hàm CLI `main` hoặc khối `if __name__ == "__main__"` (in kết quả ra stdout), và ở `_telegram_worker`
  (`core/logger.py`): worker này chuyển mọi log lên Telegram nên không thể gọi `logger` (sẽ đệ quy vào chính hàng đợi).
- `logging.getLogger` chỉ để gắn handler/bộ lọc vào logger của thư viện bên thứ ba (`core/crawl_outcome.py` bắt lỗi engine Scrapy) hoặc shim cho
  mã port (`spiders/youtube/_compat.py`).
- Sự kiện đặt tên bằng cấu hình (`saved_log_event="saved_token_cache"` trong `spiders/*/auth/bootstrap.py`) vẫn phải có trong catalog; test kiểm.

Danh sách ngoại lệ nằm ở đầu `tests/test_logging_standard.py`; thêm ngoại lệ mới phải sửa test cùng lý do.

**Telegram:** mọi dòng `warning`/`error`/`critical` (hoặc `telegram=True`) được chuyển sang Telegram, nên chọn mức log theo bảng ở mục 1: lỗi tạm thời
đã retry là `warning`, chỉ lỗi cần người xử lý mới là `error`.
