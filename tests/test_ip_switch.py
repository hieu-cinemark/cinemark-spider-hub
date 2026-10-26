"""services.ip_switch: mỗi IP một tài khoản Meta - đổi tài khoản trên cổng thì xoay IP, cổng bận thì không giao."""

from social_crawler.clients import proxy_provider
from social_crawler.services import ip_switch

PROXY = {"id": 12, "url": "42.1.92.100:1071"}


class FakeCache:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.values[key] = value

    def set_nx(self, key, value, ttl_seconds):
        if key in self.values:
            return False
        self.values[key] = value
        return True

    def delete(self, key):
        self.values.pop(key, None)


def _rotations(monkeypatch, result=True):
    calls = []

    def fake_rotate(url):
        calls.append(url)
        return result

    monkeypatch.setattr(proxy_provider, "rotate_port_for_account_switch", fake_rotate)
    monkeypatch.setattr(ip_switch, "ENABLED", True)
    return calls


def _release(cache):
    cache.delete(ip_switch.ACTIVE_KEY_TMPL.format(proxy_id=12))  # hết IDLE_SECONDS


def test_same_account_keeps_ip_and_switch_rotates(monkeypatch):
    calls = _rotations(monkeypatch)
    cache = FakeCache()
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)  # chưa biết ai dùng IP này -> xoay cho chắc
    assert ip_switch.claim("facebook", PROXY, "ACC-A", cache)  # cùng tài khoản -> giữ IP
    assert len(calls) == 1
    _release(cache)
    assert ip_switch.claim("facebook", PROXY, "acc-b", cache)  # đổi tài khoản -> xoay
    assert len(calls) == 2


def test_busy_port_is_refused_without_rotating(monkeypatch):
    calls = _rotations(monkeypatch)
    cache = FakeCache()
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)
    assert not ip_switch.claim("facebook", PROXY, "acc-b", cache)  # acc-a còn đang dùng
    assert len(calls) == 1
    ip_switch.touch("facebook", PROXY, "acc-a", cache)
    assert not ip_switch.claim("threads", PROXY, "acc-c", cache)


def test_failed_rotation_refuses_and_frees_port(monkeypatch):
    _rotations(monkeypatch, result=False)
    cache = FakeCache()
    assert not ip_switch.claim("facebook", PROXY, "acc-a", cache)
    assert cache.get(ip_switch.ACTIVE_KEY_TMPL.format(proxy_id=12)) is None
    assert cache.get(ip_switch.LAST_KEY_TMPL.format(proxy_id=12)) is None


def test_port_without_rotate_api_and_other_platforms_pass(monkeypatch):
    calls = _rotations(monkeypatch, result=None)
    cache = FakeCache()
    assert ip_switch.claim("threads", PROXY, "acc-a", cache)  # cổng tự xoay phía nhà cung cấp
    assert ip_switch.claim("tiktok", {"id": 10, "url": "x:1"}, "acc-t", cache)  # không áp cho TikTok
    assert calls == ["42.1.92.100:1071"]
