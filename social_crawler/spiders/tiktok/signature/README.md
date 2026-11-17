# social_crawler/spiders/tiktok/signature

Tính chữ ký ở local. Hai bộ ký dùng chung `cipher.py`. SAI MỘT BIT là TikTok trả 200 body rỗng - sửa xong phải so với bản cũ bằng đầu vào cố định.

## Các file

| File | Việc nó làm |
|---|---|
| `cipher.py` | Bộ mã hoá dùng chung của hai bộ ký TikTok (gnarly.py = X-Gnarly, dynosaur.py = X-Dynosaur). |
| `dynosaur.py` | Tính tham số X-Dynosaur (băm FNV của chuỗi truy vấn kèm đường dẫn referer, mã hoá bằng `cipher.ChachaCipher`). |
| `gnarly.py` | Tính header X-Gnarly (md5 trên chuỗi truy vấn + X-Dynosaur + msToken, mã hoá bằng `cipher.ChachaCipher`). |
