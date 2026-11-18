"""Giữ chỗ proxy theo ENDPOINT (host:port), không theo id dòng (18/11/2026).

Proxy tự đổi IP mỗi 60 giây: IP không cố định, nên thứ phải độc quyền trong lúc job chạy là cổng ra. Hai dòng platform_proxies
trỏ cùng một cổng (nhân bản theo nền tảng, nhập trùng) trước đây là hai khoá khác nhau - hai tài khoản đi cùng lúc qua một IP.
"""

from social_crawler.runtime import proxy_lease


def _row(row_id: int, url: str) -> dict:
    return {
        "id": row_id,
        "platform": "facebook",
        "proxy_url": url,
        "username": "u",
        "password": "p",
        "login_use_proxy": True,
        "ip_type": "rotating",
        "provider_key": None,
        "provider_ref": None,
    }


class _Cache:
    def __init__(self):
        self.d = {}

    def get(self, k):
        return self.d.get(k)

    def set_nx(self, k, v, ttl):
        if k in self.d:
            return False
        self.d[k] = v
        return True

    def expire(self, k, ttl):
        pass

    def expire_if_equals(self, k, v, ttl):
        return self.d.get(k) == v

    def delete_if_equals(self, k, v):
        if self.d.get(k) == v:
            del self.d[k]


def test_endpoint_normalises_scheme_credentials_and_case():
    assert proxy_lease.endpoint("http://user:pw@Proxy.Example.com:8080") == "proxy.example.com:8080"
    assert proxy_lease.endpoint("proxy.example.com:8080") == "proxy.example.com:8080"
    assert proxy_lease.endpoint("1.2.3.4") == "1.2.3.4"


def test_two_rows_on_the_same_endpoint_are_one_lease(monkeypatch):
    rows = [_row(12, "42.1.92.100:8001"), _row(16, "http://42.1.92.100:8001"), _row(18, "14.225.42.25:34394")]
    monkeypatch.setattr(proxy_lease, "_candidates", lambda platform: rows)
    monkeypatch.setattr(proxy_lease, "_mark_used", lambda proxy_id: None)
    cache = _Cache()

    first = proxy_lease.claim("facebook", "acc1", cache)
    second = proxy_lease.claim("facebook", "acc2", cache)

    assert first["id"] == 12
    # #16 cùng cổng với #12 đang được acc1 giữ: acc2 phải sang cổng khác.
    assert second["id"] == 18
    assert proxy_lease.claim("facebook", "acc3", cache) is None
    proxy_lease.release(first, "acc1", cache)
    assert proxy_lease.claim("facebook", "acc3", cache)["id"] == 12


def test_same_host_different_ports_are_different_endpoints(monkeypatch):
    rows = [_row(1, "gw.provider.net:10001"), _row(2, "gw.provider.net:10002")]
    monkeypatch.setattr(proxy_lease, "_candidates", lambda platform: rows)
    monkeypatch.setattr(proxy_lease, "_mark_used", lambda proxy_id: None)
    cache = _Cache()

    assert proxy_lease.claim("facebook", "acc1", cache)["id"] == 1
    assert proxy_lease.claim("facebook", "acc2", cache)["id"] == 2


def test_touch_and_release_use_the_endpoint_key():
    cache = _Cache()
    proxy = {"id": 12, "url": "42.1.92.100:8001"}
    cache.d["proxy_lease:42.1.92.100:8001"] = "acc1"
    proxy_lease.release(proxy, "someone-else", cache)
    assert "proxy_lease:42.1.92.100:8001" in cache.d  # chỉ người đang giữ mới nhả được
    proxy_lease.release(proxy, "ACC1", cache)
    assert "proxy_lease:42.1.92.100:8001" not in cache.d


def test_recording_a_proxy_outcome_mid_job_keeps_the_lease(monkeypatch):
    """Client Comet ghi kết quả proxy sau response đầu tiên; trước 18/11/2026 việc đó nhả luôn chỗ giữ - tài khoản khác nhận
    đúng cổng đó trong lúc job vẫn chạy."""
    from social_crawler.runtime import pool

    rows = [_row(12, "42.1.92.100:8001")]
    monkeypatch.setattr(proxy_lease, "_candidates", lambda platform: rows)
    monkeypatch.setattr(proxy_lease, "_mark_used", lambda proxy_id: None)
    monkeypatch.setattr(pool, "record_proxy_outcome", lambda *a, **k: None)
    cache = _Cache()
    monkeypatch.setattr(proxy_lease, "RedisCache", lambda: cache)

    proxy = proxy_lease.claim("facebook", "acc1", cache)
    pool.release_proxy(proxy, success=True, keep_lease=True)
    assert proxy_lease.claim("facebook", "acc2", cache) is None

    proxy_lease.release(proxy, "acc1", cache)
    assert proxy_lease.claim("facebook", "acc2", cache) is not None
