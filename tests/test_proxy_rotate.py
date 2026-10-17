"""CometGraphQLClient._rotate_proxy_after_network_error: reset IP cổng proxy từ lần lỗi mạng thứ 2, có giãn cách."""

from __future__ import annotations

from social_crawler.spiders import comet_graphql_client as cgc


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
