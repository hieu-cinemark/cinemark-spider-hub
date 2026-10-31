"""services.lanes: mỗi làn Facebook giữ một tài khoản riêng, không trùng; tài khoản hỏng thì làn đổi tài khoản."""

from social_crawler.runtime import lanes


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
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache, share=None: list(ready))
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
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache, share=None: list(ready))
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


def _budget_setup(monkeypatch):
    """_ready_accounts thật, với tài khoản / hạn mức giả (27/10/2026: làn đổi tài khoản khi hết phần / chạm trần giờ)."""
    from social_crawler.accounts import warmup
    from social_crawler.runtime import search_budget

    monkeypatch.setattr(
        lanes, "get_accounts", lambda platform: [{"id": "1", "email": "a@x"}, {"id": "2", "email": "b@x"}]
    )
    monkeypatch.setattr(lanes, "get_accounts_by_check_status", lambda platform, status: [])
    monkeypatch.setattr(warmup, "unwarmed_account_ids", lambda platform, cache=None: set())
    monkeypatch.setattr(lanes, "_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(lanes, "account_proxy_map", lambda platform: {})
    monkeypatch.setattr(search_budget, "DAILY_CAP", 10)
    monkeypatch.setattr(search_budget, "PERIODS", [])
    monkeypatch.setattr(search_budget, "FRESH_SHARE", 0.4)
    monkeypatch.setattr(search_budget, "HOURLY_CAP", 3)
    return search_budget


class CountingCache(FakeCache):
    def incr(self, key):
        self.data[key] = int(self.data.get(key) or 0) + 1
        return self.data[key]

    def expire(self, key, ttl):
        pass


def test_lane_switches_account_when_its_share_is_used_up(monkeypatch):
    budget = _budget_setup(monkeypatch)
    monkeypatch.setattr(budget, "HOURLY_CAP", 0)
    cache = CountingCache()
    first = lanes.lane_account(0, cache, share="fresh")
    other = {"a@x", "b@x"} - {first}
    for _ in range(4):  # hết phần fresh (4/10) của tài khoản đầu
        budget.count_request(first, cache, "fresh")
    assert lanes.lane_account(0, cache, share="fresh") in other
    # tài khoản đầu vẫn còn phần deep -> vẫn được nhận cho job đào
    assert lanes.lane_account(1, cache, share="deep") == first


def test_lane_switches_on_hour_cap_and_pauses_only_when_all_capped(monkeypatch):
    budget = _budget_setup(monkeypatch)
    cache = CountingCache()
    first = lanes.lane_account(0, cache)
    for _ in range(3):
        budget.count_request(first, cache)
    second = lanes.lane_account(0, cache)
    assert second and second != first
    assert not lanes.only_hour_capped(cache)
    for _ in range(3):
        budget.count_request(second, cache)
    assert lanes.lane_account(0, cache) is None
    assert lanes.only_hour_capped(cache)


def test_lane_skips_account_whose_port_is_still_leased_by_another(monkeypatch):
    """31/10/2026: làn vừa nhả 63 không được nhận ngay 114 cùng cổng khi cổng còn bị 63 giữ (ip_switch lease)."""
    setup(monkeypatch, ["114", "63"], proxies={"114": 16, "63": 16})
    cache = FakeCache()
    cache.set("proxy_ip:active:16", "63")
    assert lanes.lane_account(0, cache) == "63"  # 114 bị bỏ qua: cổng #16 đang là của 63
    cache.delete("proxy_ip:active:16")
    lanes.release(0, "63", cache)
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache, share=None: ["114"])
    assert lanes.lane_account(0, cache) == "114"  # cổng đã nhả
