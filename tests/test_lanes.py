"""services.lanes: mỗi làn Facebook giữ một tài khoản riêng, không trùng; tài khoản hỏng thì làn đổi tài khoản."""

from social_crawler.services import lanes


class FakeCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def set_nx(self, key, value, ttl_seconds):
        if key in self.data:
            return False
        self.data[key] = value
        return True

    def delete(self, key):
        self.data.pop(key, None)


def setup(monkeypatch, ready, proxies=None):
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache: list(ready))
    monkeypatch.setattr(lanes, "_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(lanes, "account_proxy_map", lambda platform: proxies or {})


def test_two_lanes_get_different_accounts(monkeypatch):
    setup(monkeypatch, ["a", "b", "c"])
    cache = FakeCache()
    first, second = lanes.lane_account(0, cache), lanes.lane_account(1, cache)
    assert first and second and first != second
    assert lanes.lane_account(0, cache) == first  # giữ cố định


def test_lane_switches_when_account_unusable(monkeypatch):
    ready = ["a", "b"]
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache: list(ready))
    monkeypatch.setattr(lanes, "_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(lanes, "account_proxy_map", lambda platform: {})
    cache = FakeCache()
    assert lanes.lane_account(0, cache) == "a"
    ready.remove("a")  # vd. bị từ chối phiên -> cooldown
    assert lanes.lane_account(0, cache) == "b"
    assert cache.get("facebook:lane_lock:a") is None


def test_no_account_left(monkeypatch):
    setup(monkeypatch, ["a"])
    cache = FakeCache()
    assert lanes.lane_account(0, cache) == "a"
    assert lanes.lane_account(1, cache) is None


def test_release_account(monkeypatch):
    setup(monkeypatch, ["a", "b"])
    cache = FakeCache()
    lanes.lane_account(0, cache)
    lanes.release_account("a", cache)
    assert cache.get("facebook:lane:0:account") is None and cache.get("facebook:lane_lock:a") is None


def test_lanes_never_share_an_ip(monkeypatch):
    setup(monkeypatch, ["a", "b", "c"], proxies={"a": 12, "b": 12, "c": 14})
    cache = FakeCache()
    assert lanes.lane_account(0, cache) == "a"
    assert lanes.lane_account(1, cache) == "c"  # b cùng proxy 12 với làn 0 -> bỏ qua
