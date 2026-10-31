#!/bin/bash
# NGỪNG DÙNG (31/10/2026) - không làm gì nữa, chỉ để cron cũ còn trỏ tới đây không lỗi.
#
# Trước đây cron chạy script này mỗi 4 giờ: mở Facebook bằng một tài khoản bất kỳ (next_account) rồi gõ một lượt tìm
# kiếm để làm mới cache token. Nó chạy cả trong giờ yên lặng của Meta (00:00, 04:00), cả khi lịch crawl Facebook đang
# tắt, và từng chọn trúng tài khoản vừa nhập cookie (31/10 08:00: tài khoản bị đưa sang trang đồng ý cookie rồi bị đánh
# dấu đăng xuất). Consumer đã tự làm mới token ngay trước job cần nó (consumer/sessions._ensure_facebook_session), với
# đúng tài khoản của làn - không cần lượt làm mới định kỳ nào nữa.
#
# Xoá dòng cron: crontab -u app -e  (dòng "0 */4 * * * ... scripts/refresh_token.sh")
echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] refresh_token.sh is retired - nothing to do (see the header)."
exit 0
