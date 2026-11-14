"""Hồi quy vòng đời phiên. Không dùng Redis, database, proxy hay lưu lượng Facebook thật."""

import copy
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import redis
from curl_cffi import requests

from social_crawler.clients.redis import RedisCache
from social_crawler.constants import facebook as fb
from social_crawler.runtime import facebook_run_budget as runs
from social_crawler.runtime import pool
from social_crawler.runtime.facebook_pacing import PacedSession
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient


class Cache:
    def __init__(self, data=None):
        self.data = copy.deepcopy(data or {})
        self.fail_commit = False

    def get(self, key):
        return copy.deepcopy(self.data.get(key))

    def set(self, key, value, **kwargs):
        self.data[key] = copy.deepcopy(value)

    def set_many(self, entries):
        if self.fail_commit:
            raise redis.RedisError("offline")
        self.data.update({key: copy.deepcopy(value) for key, (value, _) in entries.items()})

    def set_nx(self, key, value, **kwargs):
        if key in self.data:
            return False
        self.set(key, value)
        return True


def setup_client(monkeypatch, cache=None, send=None):
    c = object.__new__(FacebookGraphQLClient)
    c._account = "offline-test"
    c._redis = cache or Cache()
    key = fb.CACHE_REDIS_KEY_TMPL.format(account=c._account)
    c._cache = c._redis.get(key) or {
        "cookies": {"c_user": "123", "xs": "old"},
        "body_static": {},
        "headers": {},
        "captured_at": 100,
        "http_refreshed_at": 200,
    }
    c._redis.set(key, c._cache)
    c._proxy_cfg = None
    c._throttle = Mock(side_effect=c._ensure_lease)
    c._headers = Mock(return_value={})
    c._adjust_interval = Mock()
    c._record_proxy_outcome_once = Mock()
    transport = SimpleNamespace(cookies=requests.Cookies(), post=send or Mock(return_value=response({"data": {}})))
    c._session = PacedSession(transport, c._mark_request_finished)
    monkeypatch.setattr(core.proxy_lease, "touch", Mock())
    monkeypatch.setattr(core.time, "sleep", Mock())
    return c, transport


def response(payload, status=200):
    return SimpleNamespace(status_code=status, headers={}, text=json.dumps(payload))


def run(client):
    return client._run(doc_id="1", friendly_name="OfflineQuery", template={}, template_source="test", overrides={})


def test_renewed_cookie_survives_the_next_job_without_renewing_token_age(monkeypatch):
    c, transport = setup_client(monkeypatch)
    cache = c._redis
    state_key = fb.STATE_REDIS_KEY_TMPL.format(account=c._account)
    xs = {
        "name": "xs",
        "value": "old",
        "domain": ".facebook.com",
        "path": "/",
        "secure": True,
        "httpOnly": True,
        "expires": 1999999999,
    }
    origins = [{"origin": "https://www.facebook.com", "localStorage": [{"name": "test", "value": "mock"}]}]
    cache.set(state_key, {"cookies": [xs], "origins": origins})
    for template in fb.CACHE_REDIS_KEY_TMPL, fb.COMMENTS_REDIS_KEY_TMPL, fb.REPLIES_REDIS_KEY_TMPL:
        cache.set(template.format(account=c._account), c._cache)
    sent = []

    def send(*args, **kwargs):
        sent.append(transport.cookies.get_dict()["xs"])
        transport.cookies.set("xs", "new", domain=".facebook.com", path="/")
        return response({"data": {}})

    transport.post = send
    run(c)
    run(c)
    next_client, next_transport = setup_client(monkeypatch, cache)
    next_transport.post = lambda *a, **kw: (
        sent.append(next_transport.cookies.get_dict()["xs"]) or response({"data": {}})
    )
    run(next_client)
    assert sent == ["old", "new", "new"]
    for template in fb.CACHE_REDIS_KEY_TMPL, fb.COMMENTS_REDIS_KEY_TMPL, fb.REPLIES_REDIS_KEY_TMPL:
        recipe = cache.get(template.format(account=c._account))
        assert recipe["cookies"]["xs"] == "new"
        assert (recipe["captured_at"], recipe["http_refreshed_at"]) == (100, 200)
    state = cache.get(state_key)
    assert next(cookie for cookie in state["cookies"] if cookie["name"] == "xs") == {**xs, "value": "new"}
    assert state["origins"] == origins


def test_snapshot_write_failure_stops_before_any_next_request(monkeypatch):
    c, transport = setup_client(monkeypatch)
    c._redis.fail_commit = True

    def send(*a, **kw):
        transport.cookies.set("xs", "new", domain=".facebook.com", path="/")
        return response({"data": {}})

    transport.post = Mock(side_effect=send)
    with pytest.raises(core.NetworkError, match="snapshot"):
        run(c)
    with pytest.raises(core.SessionExpiredError):
        run(c)
    assert transport.post.call_count == 1
    assert c._redis.get(fb.CACHE_REDIS_KEY_TMPL.format(account=c._account))["cookies"]["xs"] == "old"


@pytest.mark.parametrize("status", [200, 400, 500, 503])
@pytest.mark.parametrize(
    "payload,exception,handler",
    [
        ({"message": "checkpoint_required", "status": "fail"}, core.CheckpointRequiredError, "_raise_checkpoint"),
        ({"error": "1357053"}, core.CheckpointRequiredError, "_raise_checkpoint"),
        ({"error": 1357001}, core.SessionExpiredError, "_drop_rejected_session"),
        ({"errors": [{"code": 368}]}, core.AccountRestrictedError, "_rest_restricted_account"),
    ],
)
def test_explicit_security_errors_stop_on_first_response_at_any_status(
    monkeypatch, status, payload, exception, handler
):
    c, transport = setup_client(monkeypatch)

    def stop(*a):
        c._session_dead = True
        raise exception("stopped")

    setattr(c, handler, Mock(side_effect=stop))
    transport.post = Mock(return_value=response(payload, status))
    with pytest.raises(exception):
        run(c)
    with pytest.raises(core.SessionExpiredError):
        run(c)
    assert transport.post.call_count == 1


def test_security_words_inside_user_content_do_not_stop_collection(monkeypatch):
    payload = {"data": {"comment": "checkpoint_required /checkpoint/ rate limit exceeded"}}
    c, _ = setup_client(monkeypatch, send=Mock(return_value=response(payload)))
    assert run(c) == payload


def test_unknown_html_pauses_subsequent_jobs_without_changing_cookies_or_logging_body(monkeypatch):
    c, transport = setup_client(monkeypatch)
    transport.post = Mock(
        return_value=SimpleNamespace(status_code=200, headers={}, text="<html>mock-sensitive-content</html>")
    )
    with pytest.raises(core.NetworkError) as caught:
        run(c)
    assert "mock-sensitive-content" not in str(caught.value)
    snapshot = c._redis.get(fb.CACHE_REDIS_KEY_TMPL.format(account=c._account))
    next_client, next_transport = setup_client(monkeypatch, c._redis)
    assert isinstance(caught.value.__cause__, pool.ProxyPoolExhaustedError)
    with pytest.raises(core.NetworkError, match="temporarily paused"):
        run(next_client)
    next_transport.post.assert_not_called()
    assert snapshot["cookies"]["xs"] == "old"


def test_cookie_snapshot_uses_one_redis_transaction():
    cache = object.__new__(RedisCache)
    cache._prefix = "test:"
    pipe = Mock()
    cache._client = SimpleNamespace(pipeline=Mock(return_value=SimpleNamespace(__enter__=None)))
    from unittest.mock import MagicMock

    manager = MagicMock()
    manager.__enter__.return_value = pipe
    cache._client.pipeline.return_value = manager
    cache.set_many({"state": ({"cookies": []}, None), "recipe": ({"cookies": {}}, 60)})
    cache._client.pipeline.assert_called_once_with(transaction=True)
    assert pipe.set.call_count == 2
    pipe.execute.assert_called_once()


@pytest.mark.parametrize("reason", ["requests", "duration"])
def test_account_run_rest_survives_jobs_and_resumes_same_work(monkeypatch, reason):
    monkeypatch.setattr(runs, "MAX_REQUESTS", 2 if reason == "requests" else 100)
    monkeypatch.setattr(runs, "MAX_SECONDS", 600)
    monkeypatch.setattr(runs, "REST_SECONDS", 60)
    clock = [1000.0]
    monkeypatch.setattr(runs.time, "time", lambda: clock[0])
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(runs.time, "sleep", sleep)
    cache, lease, resume = Cache(), Mock(), Mock()
    runs.reserve("account", cache, lease, resume)
    runs.reserve("account", cache, lease, resume)
    if reason == "duration":
        state = cache.get(runs.KEY.format(account="account"))
        cache.set(runs.KEY.format(account="account"), {**state, "started_at": 300})
    runs.reserve("account", cache, lease, resume)
    assert sum(sleeps) == 60
    resume.assert_called_once()
    assert cache.get(runs.KEY.format(account="account"))["used"] == 1
    assert lease.call_count >= 5


def test_lost_lease_during_run_rest_never_reserves_next_request(monkeypatch):
    monkeypatch.setattr(runs, "MAX_REQUESTS", 1)
    monkeypatch.setattr(runs, "REST_SECONDS", 60)
    cache = Cache()
    runs.reserve("account", cache, Mock(), Mock())
    resume = Mock()
    with pytest.raises(RuntimeError, match="lease lost"):
        runs.reserve("account", cache, Mock(side_effect=RuntimeError("lease lost")), resume)
    resume.assert_not_called()
    assert cache.get(runs.KEY.format(account="account"))["used"] == 1
