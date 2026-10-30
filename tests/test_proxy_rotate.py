"""CometGraphQLClient._rotate_proxy_after_network_error: reset IP cổng proxy từ lần lỗi mạng thứ 2, có giãn cách."""

from __future__ import annotations

from social_crawler.spiders.common import comet_graphql_client as cgc


def test_rotates_from_second_network_error_and_throttles(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cgc.proxy_provider, "reset_provider_for_proxy_url", lambda url: calls.append(url) or "p")
    cgc._last_rotate_at.clear()
    client = cgc.CometGraphQLClient.__new__(cgc.CometGraphQLClient)
    client.PLATFORM = "threads"
    client._proxy_cfg = {"url": "gw.example:1"}

    client._rotate_proxy_after_network_error(1)
    assert calls == []
    client._rotate_proxy_after_network_error(2)
    assert calls == ["gw.example:1"]
    client._rotate_proxy_after_network_error(3)  # trong _ROTATE_MIN_GAP_SECONDS: không reset lại
    assert calls == ["gw.example:1"]


def test_no_proxy_no_rotation(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cgc.proxy_provider, "reset_provider_for_proxy_url", lambda url: calls.append(url))
    client = cgc.CometGraphQLClient.__new__(cgc.CometGraphQLClient)
    client.PLATFORM = "facebook"
    client._proxy_cfg = None
    client._rotate_proxy_after_network_error(4)
    assert calls == []


def _failing_client(monkeypatch, outcomes: list[bool]):
    """Client mà mọi POST đều lỗi mạng; trả (client, danh sách lần gọi)."""
    calls: list[int] = []

    class _Session:
        def post(self, *args, **kwargs):
            calls.append(1)
            raise cgc.curl_requests.RequestsError("curl: (28) Operation too slow")

    monkeypatch.setattr(cgc.time, "sleep", lambda _s: None)
    monkeypatch.setattr(cgc.proxy_provider, "reset_provider_for_proxy_url", lambda url: "p")
    cgc._last_rotate_at.clear()
    client = cgc.CometGraphQLClient.__new__(cgc.CometGraphQLClient)
    client.PLATFORM = "facebook"
    client.GRAPHQL_URL = "https://example.invalid/api/graphql/"
    client.MAX_RETRIES = 5
    client.RETRY_BACKOFF_BASE_SECONDS = 2.0
    client.RETRY_BACKOFF_JITTER_SECONDS = 0.0
    client._proxy_cfg = {"url": "gw.example:1"}
    client._session = _Session()
    client._adjust_interval = lambda stressed: None
    client._record_proxy_outcome_once = lambda success: outcomes.append(success)
    return client, calls


def test_gives_up_after_consecutive_network_errors_as_proxy_exhausted(monkeypatch) -> None:
    outcomes: list[bool] = []
    client, calls = _failing_client(monkeypatch, outcomes)
    try:
        client._post_with_retry({}, {}, {})
    except cgc.NetworkError as exc:
        assert isinstance(exc.__cause__, cgc.pool.ProxyPoolExhaustedError)
    else:
        raise AssertionError("expected NetworkError")
    assert len(calls) == cgc.CometGraphQLClient.NETWORK_GIVE_UP_ATTEMPTS < client.MAX_RETRIES
    assert outcomes == [False]  # proxy ghi lỗi -> cooldown, lượt sau không treo tiếp


def test_network_give_up_exits_with_requeue_code(monkeypatch) -> None:
    import pytest

    from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE

    client, _ = _failing_client(monkeypatch, [])
    with pytest.raises(cgc.NetworkError) as info:
        client._post_with_retry({}, {}, {})
    with pytest.raises(SystemExit) as exit_info:
        cgc.pool.abort_spider_for_network_error(info.value)
    assert exit_info.value.code == PROXY_EXHAUSTED_EXIT_CODE
