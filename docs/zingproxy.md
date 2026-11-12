# ZingProxy (cùng cơ chế với mọi nguồn khác)

> ⚠️ Tài liệu này được viết lại sau khi **đơn giản hoá toàn bộ nguồn proxy** (12/11/2026):
> nay **mọi** nguồn đều là một link reset + một key, không còn adapter riêng cho ZingProxy.
> `social_crawler/clients/zingproxy.py` đã bị xoá.

ZingProxy dùng luồng `runtime/ip_switch.claim` hiện có: lần nhận cổng đầu tiên hoặc khi
chuyển tài khoản sẽ đổi IP trước khi giao cổng. Cùng tài khoản dùng lại cổng thì giữ IP.
Không có lịch tự xoay giữa phiên crawl. Cổng đang được tài khoản khác dùng sẽ bị từ chối.

## Cấu hình

Lưu một nguồn qua phần cấu hình proxy hiện có (`/settings/proxy/providers` của
cinemark-api), ví dụ:

| Trường | Giá trị |
| --- | --- |
| `key` | `zingproxy_facebook` |
| `api_url` | `https://api.zingproxy.com/open/change-ip/{ref}` |
| `token` | mã truy cập của nhà cung cấp (nếu link cần) |

`{ref}` được thay bằng **mã cổng trong hệ thống nhà cung cấp** — điền vào
`provider_ref` của dòng proxy tương ứng (xem `platform_proxies.provider_ref`).
Nếu link đổi IP của nhà cung cấp đã chứa sẵn mã cổng thì không cần placeholder nào.

Không đưa link/key thật vào git. Dùng cổng HTTP của gói, không dùng cổng SOCKS5 cho
cổng này. `HOST:HTTP_PORT` phải khớp chính xác `platform_proxies.proxy_url` mà tài
khoản Facebook đang ghim; không trộn tên miền ở một nơi với IP ở nơi còn lại.

Giữ `META_ROTATE_ON_ACCOUNT_SWITCH=1` (mặc định). Chỉ thêm nguồn chưa đủ: tài khoản
phải được gán đúng proxy và thông tin xác thực tương ứng trong cấu hình proxy hiện có.

## Xác nhận và lỗi

- Đo IP công khai qua chính proxy bằng HTTPS tới `api.ipify.org` trước khi gọi link.
- Sau khi gọi, chờ tối đa 120 giây để đo được IPv4 công khai khác IP cũ. API báo thành
  công nhưng IP chưa đổi vẫn được coi là thất bại; không giao cổng cho tài khoản mới.
- Cooldown được nhận từ thông báo số giây của nhà cung cấp. Chỉ thử lại một lần trong
  giới hạn `provider_max_cooldown_wait_seconds` (mặc định 120 giây). Cooldown dài hơn
  thì từ chối lượt nhận cổng để xử lý sau.
- Lỗi mạng khi gọi reset không tự gọi lại vì kết quả reset có thể chưa rõ.
- Không log link, key hay mật khẩu. Không có bảo đảm tránh checkpoint từ Facebook.

Nguồn cần được cấu hình riêng ở môi trường chạy; thay đổi source không tự lưu key
vào DB, không deploy và không gọi link đổi IP thật.

Kiểm thử offline:

```sh
.venv/bin/python -m pytest -q tests/test_proxy_provider.py tests/test_ip_switch.py
```

Các kiểm thử giả lập toàn bộ HTTP và Redis, không sử dụng tài khoản Facebook.
