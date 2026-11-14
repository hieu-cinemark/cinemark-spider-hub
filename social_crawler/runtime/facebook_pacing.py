"""Khoảng nghỉ cố định theo từng tài khoản; mỗi request thật đều ghi lại lúc xong."""

from __future__ import annotations

import hashlib
import math

COOKIE_DOMAIN = ".facebook.com"


def rest_range(account_id: str, minimum: float, jitter: float) -> tuple[float, float]:
    if not math.isfinite(minimum) or minimum <= 0 or not math.isfinite(jitter) or jitter < 0:
        raise ValueError("Facebook request rest requires a positive minimum and nonnegative jitter")
    digest = hashlib.sha256(f"facebook-rest:v1:{account_id}".encode()).digest()
    # Ổn định qua các worker/lần khởi động lại và qua bí danh UID/email khi phiên có c_user.
    offset = int.from_bytes(digest[:2]) / 65535 * minimum * 0.4
    width = jitter * (1 + int.from_bytes(digest[2:4]) / 65535 * 0.3)
    lower = round(minimum + offset, 2)
    return lower, round(lower + width, 2)


class PacedSession:
    """Giữ nguyên cách gửi; vẫn ghi lúc xong kể cả khi lỗi mạng."""

    def __init__(self, session, finished):
        self._wrapped = session
        self._finished = finished
        self._seeded = set()

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def _send(self, method, *args, **kwargs):
        # Nạp cookie trong cache vào jar đúng một lần rồi để curl quản lý: cookies= theo request sẽ đè lên jar, tức gửi lại giá
        # trị cũ, kể cả cookie server vừa xoá.
        for name, value in (kwargs.pop("cookies", None) or {}).items():
            if name not in self._seeded:
                self._wrapped.cookies.set(name, value, domain=COOKIE_DOMAIN, path="/")
                self._seeded.add(name)
        return getattr(self._wrapped, method)(*args, **kwargs)

    def removed_cookies(self) -> set[str]:
        """Cookie đã nạp từ cache mà server đã xoá, để cache không phát lại chúng ở tiến trình sau."""
        return self._seeded - set(self._wrapped.cookies.get_dict())

    def get(self, *args, **kwargs):
        try:
            return self._send("get", *args, **kwargs)
        finally:
            self._finished()

    def post(self, *args, **kwargs):
        try:
            return self._send("post", *args, **kwargs)
        finally:
            self._finished()
