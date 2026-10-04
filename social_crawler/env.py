"""Nơi duy nhất gọi load_dotenv(). Cả settings.py lẫn accounts.py đều cần .env được nạp
trước khi đọc os.getenv(...) lúc import, nhưng không cái nào trông cậy được là cái kia
đã chạy trước (cái nào cũng có thể được import riêng lẻ). Import module này - từ một
trong hai, hoặc bất cứ đâu - nạp .env đúng một lần: Python chỉ chạy phần cấp cao nhất
của module ở lần import đầu tiên, nên cái nào import trước thì chịu chi phí đọc/parse,
và mọi lần import sau chỉ nhận lại module đã cache."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()
