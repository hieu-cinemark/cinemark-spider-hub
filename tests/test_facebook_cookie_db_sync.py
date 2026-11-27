"""Cookie Facebook xoay phải về DB để máy khác (Mac/VPS) dựng phiên từ bản mới, không phát lại cookie gốc đã cũ."""

from __future__ import annotations

from social_crawler.accounts import cookie_db_sync

STORED = "c_user=61596549849541; xs=old:1; datr=D1; useragent=TW96aWxsYS81LjA="


def test_build_header_updates_values_keeps_user_agent_and_adds_new_cookies():
    header = cookie_db_sync.build_header(STORED, {"c_user": "61596549849541", "xs": "new:2", "datr": "D1", "wd": "1280x720"})
    assert header == "c_user=61596549849541; xs=new:2; datr=D1; useragent=TW96aWxsYS81LjA=; wd=1280x720"


def test_build_header_drops_cookies_facebook_deleted():
    header = cookie_db_sync.build_header(STORED, {"c_user": "61596549849541", "xs": "x:3", "datr": "deleted"})
    assert "datr" not in header and "useragent=" in header


def test_build_header_refuses_dead_sessions():
    assert cookie_db_sync.build_header(STORED, {"c_user": "61596549849541", "xs": "deleted"}) is None
    assert cookie_db_sync.build_header(STORED, {"c_user": "deleted", "xs": "x:1"}) is None
    assert cookie_db_sync.build_header(STORED, {"xs": "x:1"}) is None


class _Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl=None):
        self.data[key] = value


def _patch(monkeypatch, row_cookie=STORED, writes=None):
    writes = [] if writes is None else writes
    monkeypatch.setattr(cookie_db_sync.db_accounts, "get_account_pk", lambda platform, key: 7)
    monkeypatch.setattr(
        cookie_db_sync.db_accounts, "get_account_by_key", lambda platform, key: {"id": "61596549849541", "cookie": row_cookie}
    )
    monkeypatch.setattr(
        cookie_db_sync.db_accounts, "update_account_cookie", lambda platform, pk, cookie: writes.append((pk, cookie)) or True
    )
    return writes


def test_sync_writes_once_then_waits_for_the_interval(monkeypatch):
    writes = _patch(monkeypatch)
    cache = _Cache()
    first = {"c_user": "61596549849541", "xs": "new:2", "datr": "D1"}
    assert cookie_db_sync.sync(cache, "acc@x.com", first, now=1000.0) is True
    assert writes[0][0] == 7 and "xs=new:2" in writes[0][1] and "useragent=" in writes[0][1]
    # Cùng nội dung: không ghi lại. Nội dung đổi nhưng chưa tới hạn: chờ.
    assert cookie_db_sync.sync(cache, "acc@x.com", first, now=1001.0) is False
    assert cookie_db_sync.sync(cache, "acc@x.com", {**first, "xs": "new:3"}, now=1100.0) is False
    assert cookie_db_sync.sync(cache, "acc@x.com", {**first, "xs": "new:3"}, now=1000.0 + cookie_db_sync.SYNC_INTERVAL_SECONDS + 1) is True
    assert len(writes) == 2


def test_sync_ignores_another_identity_and_dead_sessions(monkeypatch):
    writes = _patch(monkeypatch)
    assert cookie_db_sync.sync(_Cache(), "acc@x.com", {"c_user": "999", "xs": "x:1"}) is False
    assert cookie_db_sync.sync(_Cache(), "acc@x.com", {"c_user": "61596549849541", "xs": "deleted"}) is False
    assert writes == []


def test_sync_never_raises(monkeypatch):
    def boom(*_args):
        raise RuntimeError("D1 down")

    monkeypatch.setattr(cookie_db_sync.db_accounts, "get_account_pk", boom)
    assert cookie_db_sync.sync(_Cache(), "acc@x.com", {"c_user": "61596549849541", "xs": "x:1"}) is False
