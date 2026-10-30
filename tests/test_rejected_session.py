"""comet_graphql_client: phiên bị từ chối -> xoá cache + con trỏ active, cho tài khoản nghỉ, không phát lại."""

from social_crawler.spiders.common import comet_graphql_client as cgc


class FakeRedis:
    def __init__(self, data):
        self.data = dict(data)

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.data.pop(key, None)


def test_drop_rejected_session(monkeypatch):
    import social_crawler.db.accounts as accounts

    calls = {}
    monkeypatch.setattr(accounts, "rest_account_until", lambda p, k, until: calls.setdefault("rest", (p, k)) or True)
    monkeypatch.setattr(accounts, "get_account_by_key", lambda p, k: {"id": "123"})
    monkeypatch.setattr(accounts, "record_cookie_check", lambda p, aid, status, note: calls.setdefault("dead", (aid, status)))
    client = object.__new__(cgc.CometGraphQLClient)
    client.PLATFORM = "facebook"
    client.CACHE_REDIS_KEY_TMPL = "facebook:session_cache:{account}"
    client.ACTIVE_ACCOUNT_REDIS_KEY = "facebook:active_account"
    client._account = "a@x"
    client._redis = FakeRedis({"facebook:session_cache:a@x": {"x": 1}, "facebook:active_account": "a@x"})
    client._drop_rejected_session("HTTP 401")
    assert client._redis.data == {}
    assert calls == {"rest": ("facebook", "a@x"), "dead": ("123", "dead")}
