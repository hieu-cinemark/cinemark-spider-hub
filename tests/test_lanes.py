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

    def delete_if_equals(self, key, value):
        if key in self.data and self.data[key] == value:
            del self.data[key]
            return True
        return False


def setup(monkeypatch, ready, proxies=None):
    monkeypatch.setattr(lanes, "_ready_accounts", lambda cache, share=None: list(ready))
    monkeypatch.setattr(lanes, "_ttl_seconds", lambda: 3600)


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
    assert cache.get(lanes._lane_key(0)) is None and cache.get("facebook:lane_lock:a") is None


def test_two_instances_with_same_lane_number_never_share_an_account(monkeypatch):
    setup(monkeypatch, ["a", "b"])
    cache = FakeCache()
    monkeypatch.setattr(lanes, "INSTANCE_ID", "vps")
    first = lanes.lane_account(0, cache)
    monkeypatch.setattr(lanes, "INSTANCE_ID", "local")
    second = lanes.lane_account(0, cache)
    assert first and second and first != second
    # Không nhả được khoá của instance khác, kể cả cùng số làn.
    lanes.release_account(first, cache)
    assert cache.get(f"facebook:lane_lock:{first}") == "vps:0"
    lanes.release(0, first, cache)
    assert cache.get(f"facebook:lane_lock:{first}") == "vps:0"
    monkeypatch.setattr(lanes, "INSTANCE_ID", "vps")
    assert lanes.lane_account(0, cache) == first  # restart cùng máy: nhận lại đúng tài khoản


def test_legacy_plain_lane_lock_is_adopted_by_same_lane(monkeypatch):
    """Khoá số trơn ghi trước lần deploy này (TTL tới sáng hôm sau) không được làm cả làn mất tài khoản."""
    setup(monkeypatch, ["a"])
    cache = FakeCache()
    cache.set("facebook:lane_lock:a", 0)
    assert lanes.lane_account(1, cache) is None  # khoá của làn 0
    assert lanes.lane_account(0, cache) == "a"
    assert cache.get("facebook:lane_lock:a") == lanes._owner(0)


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


def test_other_lane_has_account(monkeypatch):
    setup(monkeypatch, ["a"])
    cache = FakeCache()
    assert not lanes.other_lane_has_account(0, cache)
    assert lanes.lane_account(1, cache) == "a"
    assert lanes.other_lane_has_account(0, cache)
    assert not lanes.other_lane_has_account(1, cache)
