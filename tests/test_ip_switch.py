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


def _rotations(monkeypatch, result=True, ips=None):
    """result: True/False/None như rotate_port_for_account_switch; ips: lần lượt IP mới trả về khi True."""
    calls = []
    ip_iter = iter(ips or [f"113.189.102.{n}" for n in range(1, 100)])

    def fake_rotate(url):
        calls.append(url)
        if result is True:
            return True, next(ip_iter)
        return result, None

    monkeypatch.setattr(proxy_provider, "rotate_port_for_account_switch", fake_rotate)
    monkeypatch.setattr(ip_switch, "ENABLED", True)
    monkeypatch.setattr(ip_switch, "_sleep", lambda seconds: None)
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


def test_new_ip_used_by_other_account_is_rotated_again(monkeypatch):
    calls = _rotations(monkeypatch, ips=["1.1.1.1", "2.2.2.2", "1.1.1.1", "3.3.3.3"])
    cache = FakeCache()
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)  # acc-a dùng 1.1.1.1
    _release(cache)
    assert ip_switch.claim("facebook", PROXY, "acc-b", cache)  # acc-b dùng 2.2.2.2
    _release(cache)
    # acc-c: lần xoay đầu ra lại 1.1.1.1 (của acc-a) -> xoay tiếp, nhận 3.3.3.3
    assert ip_switch.claim("facebook", PROXY, "acc-c", cache)
    assert len(calls) == 4
    assert cache.get(ip_switch.OWNER_KEY_TMPL.format(ip="3.3.3.3")) == "acc-c"
    assert cache.get(ip_switch.OWNER_KEY_TMPL.format(ip="1.1.1.1")) == "acc-a"


def test_same_account_may_get_its_own_old_ip_back(monkeypatch):
    calls = _rotations(monkeypatch, ips=["1.1.1.1", "2.2.2.2", "1.1.1.1"])
    cache = FakeCache()
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)
    _release(cache)
    assert ip_switch.claim("facebook", PROXY, "acc-b", cache)
    _release(cache)
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)  # IP cũ của chính acc-a: không cần xoay thêm
    assert len(calls) == 3


def test_reuse_after_max_rotations_refuses(monkeypatch):
    monkeypatch.setattr(ip_switch, "MAX_ROTATIONS", 2)
    _rotations(monkeypatch, ips=["1.1.1.1", "1.1.1.1", "1.1.1.1"])
    cache = FakeCache()
    assert ip_switch.claim("facebook", PROXY, "acc-a", cache)
    _release(cache)
    assert not ip_switch.claim("facebook", PROXY, "acc-b", cache)  # kho chỉ có 1.1.1.1
    assert cache.get(ip_switch.ACTIVE_KEY_TMPL.format(proxy_id=12)) is None
