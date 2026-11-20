from unittest.mock import Mock

import pytest
import redis
import requests

from social_crawler.runtime.facebook_pacing import PacedSession, rest_range
from social_crawler.spiders.common import comet_graphql_client as core
from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient


class Cache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, **kwargs):
        self.data[key] = value


def client(cache):
    c = object.__new__(FacebookGraphQLClient)
    c._account = "test"
    c._cache = {"cookies": {"c_user": "123"}}
    c._redis = cache
    c._ensure_lease = Mock()
    c._count_request = Mock()
    c._current_interval = Mock(return_value=5)
    c.LONG_PAUSE_CHANCE = 0
    c._last_request_at = None
    return c


def test_profiles_are_stable_distinct_and_never_faster_than_configured_floor():
    profiles = {rest_range(str(i), 5, 5) for i in range(100)}
    assert len(profiles) == 100
    assert rest_range("123", 5, 5) == rest_range("123", 5, 5)
    assert all(5 <= lower <= 7 and upper >= lower + 5 for lower, upper in profiles)


def test_rest_is_measured_from_response_and_survives_next_client(monkeypatch):
    cache = Cache()
    cache.set("facebook:last_request_at:test", 90)
    cache.set("facebook:last_response_at:test", 100)
    c = client(cache)
    monkeypatch.setattr(core.time, "time", lambda: 102)
    monkeypatch.setattr(core.random, "uniform", Mock(return_value=6))
    sleep = Mock()
    monkeypatch.setattr(core.time, "sleep", sleep)
    c._throttle()
    sleep.assert_called_once_with(4)
    c._count_request.assert_called_once()
    c._ensure_lease.assert_called()
    assert core.random.uniform.call_args.args == rest_range("123", 5, 5)


def test_processing_time_counts_toward_rest_and_backoff_is_preserved(monkeypatch):
    c = client(Cache())
    c._redis.set("facebook:last_response_at:test", 100)
    c._current_interval.return_value = 10
    monkeypatch.setattr(core.time, "time", lambda: 200)
    sample, sleep = Mock(return_value=12), Mock()
    monkeypatch.setattr(core.random, "uniform", sample)
    monkeypatch.setattr(core.time, "sleep", sleep)
    c._throttle()
    lower, upper = rest_range("123", 5, 5)
    sample.assert_called_once_with(lower * 2, upper * 2)
    sleep.assert_not_called()


def test_lost_lease_during_wait_prevents_next_request(monkeypatch):
    c = client(Cache())
    c._redis.set("facebook:last_response_at:test", 100)
    c._ensure_lease.side_effect = [None, core.NetworkError("lease lost")]
    monkeypatch.setattr(core.time, "time", lambda: 101)
    monkeypatch.setattr(core.time, "sleep", Mock())
    with pytest.raises(core.NetworkError):
        c._throttle()
    assert c._redis.get("facebook:last_request_at:test") is None


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("failed", [False, True])
def test_physical_transport_records_completion_even_on_network_error(method, failed):
    session = Mock()
    finish = Mock()
    getattr(session, method).side_effect = requests.ConnectionError("test") if failed else None
    paced = PacedSession(session, finish)
    if failed:
        with pytest.raises(requests.ConnectionError):
            getattr(paced, method)("https://www.facebook.com/")
    else:
        getattr(paced, method)("https://www.facebook.com/")
    finish.assert_called_once()
    assert paced.cookies is session.cookies


def test_failed_completion_storage_stops_client(monkeypatch):
    c = client(Cache())
    c._redis.set = Mock(side_effect=redis.RedisError("test"))
    with pytest.raises(core.NetworkError):
        c._mark_request_finished()
