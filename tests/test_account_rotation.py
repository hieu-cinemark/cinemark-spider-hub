"""consumer.sessions._rotation_due: gỡ con trỏ tài khoản active sau ACCOUNT_ROTATE_MINUTES khi còn tài khoản khác."""

from __future__ import annotations

from social_crawler.consumer import sessions as crc


class _Cache:
    def __init__(self) -> None:
        self.data: dict = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


def test_rotates_after_window_when_other_accounts(monkeypatch) -> None:
    cache = _Cache()
    cache.data["facebook:active_account"] = "a@x"
    monkeypatch.setattr(crc, "ACCOUNT_ROTATE_MINUTES", 30.0)
    monkeypatch.setattr(crc, "get_accounts", lambda platform: [{"account_id": "a@x"}, {"account_id": "b@x"}])
    clock = [1000.0]
    monkeypatch.setattr(crc.time, "time", lambda: clock[0])

    assert not crc._rotation_due(cache, "facebook", "a@x", "facebook:active_account")  # lần đầu: ghi mốc
    clock[0] += 29 * 60
    assert not crc._rotation_due(cache, "facebook", "a@x", "facebook:active_account")
    clock[0] += 2 * 60
    assert crc._rotation_due(cache, "facebook", "a@x", "facebook:active_account")
    assert "facebook:active_account" not in cache.data


def test_keeps_single_account(monkeypatch) -> None:
    cache = _Cache()
    cache.data["threads:active_account"] = "only"
    monkeypatch.setattr(crc, "ACCOUNT_ROTATE_MINUTES", 30.0)
    monkeypatch.setattr(crc, "get_accounts", lambda platform: [{"account_id": "only"}])
    clock = [0.0]
    monkeypatch.setattr(crc.time, "time", lambda: clock[0])
    crc._rotation_due(cache, "threads", "only", "threads:active_account")
    clock[0] += 3600
    assert not crc._rotation_due(cache, "threads", "only", "threads:active_account")
    assert cache.data["threads:active_account"] == "only"
