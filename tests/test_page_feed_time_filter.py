"""graphql_client.page_feed: lọc bài theo ngày phía Facebook bằng afterTime/beforeTime (30/10/2026, ý từ rewindfb)."""

from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient


def _client(friendly_name):
    client = FacebookGraphQLClient.__new__(FacebookGraphQLClient)
    recipe = {"doc_id": "1", "fb_api_req_friendly_name": friendly_name, "variables_template": {"id": "x", "count": 3}}
    client._get_page_feed_cache = lambda: {"pagination": recipe}
    sent = {}
    client._run = lambda **kwargs: sent.update(kwargs) or {}
    return client, sent


def test_time_filter_added_for_refetch_query():
    client, sent = _client("ProfileCometTimelineFeedRefetchQuery")
    client.page_feed("123", None, after_ts=1_700_000_000.5, before_ts=1_700_100_000)
    assert sent["template"]["afterTime"] == 1_700_000_000
    assert sent["template"]["beforeTime"] == 1_700_100_000
    assert sent["template"]["omitPinnedPost"] is True
    assert sent["overrides"]["id"] == "123"


def test_time_filter_skipped_for_other_queries():
    client, sent = _client("CometModernPageFeedPaginationQuery")
    client.page_feed("123", None, after_ts=1_700_000_000)
    assert "afterTime" not in sent["template"]


def test_no_filter_by_default():
    client, sent = _client("ProfileCometTimelineFeedRefetchQuery")
    client.page_feed("123")
    assert "afterTime" not in sent["template"]
