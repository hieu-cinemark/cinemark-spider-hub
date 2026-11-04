"""Kiểm tra cookie Facebook trước request đầu tiên (04/11/2026). Chỉ đọc dữ liệu đã có, không gửi gì lên Facebook.

Cookie hỏng mà vẫn được dùng thì tín hiệu từ chối rơi vào tài khoản; chặn ở đây không tốn request nào. cinemark-api có cùng các
quy tắc ở app/services/accounts/cookie_quality.py để từ chối ngay trên form. Sửa một bên thì sửa cả bên kia.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

REQUIRED_COOKIES = ("c_user", "xs", "datr")
# Điểm xuất phát, chưa chứng minh là ngưỡng đúng: phiên 376 ngày đã bị Facebook đăng xuất ngay request đầu (04/11/2026).
MAX_XS_AGE_DAYS = int(os.getenv("FACEBOOK_COOKIE_MAX_AGE_DAYS", "180"))
# Chrome từ bản 100 có build >= 4896. UA đi kèm tài khoản mua sẵn hay có build không tồn tại, ví dụ 137.0.1697.129.
FIRST_BUILD_OF_CHROME_100 = 4896
_CHROME_VERSION = re.compile(r"\bChrome/(\d+)\.\d+\.(\d+)\.\d+")


@dataclass
class CookieAssessment:
    problems: list[str] = field(default_factory=list)
    # Đưa vào dòng thời gian: không có giá trị cookie nào ở đây.
    facts: dict[str, Any] = field(default_factory=dict)

    @property
    def usable(self) -> bool:
        return not self.problems


def xs_age_days(xs: str, now: float | None = None) -> float | None:
    """Trường thứ 4 của xs là thời điểm tạo phiên. Định dạng không theo tài liệu nào nên đọc không được chỉ là "không biết"."""
    fields = unquote(xs or "").split(":")
    if len(fields) < 4 or not re.fullmatch(r"\d{10}", fields[3]):
        return None
    return round(((now if now is not None else time.time()) - int(fields[3])) / 86400, 1)


def _user_agent_system(user_agent: str) -> str:
    for marker, system in (("Windows", "windows"), ("Macintosh", "mac"), ("Linux", "linux"), ("Android", "android")):
        if marker in user_agent:
            return system
    return "other"


def assess(
    cookies: dict[str, str], user_agent: str | None, account_id: str | None = None, *, now: float | None = None
) -> CookieAssessment:
    problems = [f"missing_{name}" for name in REQUIRED_COOKIES if not cookies.get(name)]
    c_user = str(cookies.get("c_user") or "")
    if c_user and not c_user.isdigit():
        problems.append("c_user_not_numeric")
    if account_id and account_id.isdigit() and c_user and c_user != account_id:
        problems.append("c_user_mismatch")
    age = xs_age_days(str(cookies.get("xs") or ""), now)
    if age is not None and age > MAX_XS_AGE_DAYS:
        problems.append("xs_too_old")
    chrome = _CHROME_VERSION.search(user_agent or "")
    if not user_agent:
        problems.append("missing_user_agent")
    elif chrome and int(chrome[1]) >= 100 and int(chrome[2]) != 0 and int(chrome[2]) < FIRST_BUILD_OF_CHROME_100:
        problems.append("user_agent_impossible_chrome_build")
    return CookieAssessment(
        problems=problems,
        facts={
            "cookie_count": sum(1 for name in cookies if name != "useragent"),
            "has_sb": bool(cookies.get("sb")),
            "xs_age_days": age,
            "ua_system": _user_agent_system(user_agent or ""),
            "ua_chrome": f"{chrome[1]}.{chrome[2]}" if chrome else None,
        },
    )
