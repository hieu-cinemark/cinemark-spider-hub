"""Bản mẫu truy vấn Facebook được bổ sung các cờ relay provider còn thiếu trước khi gửi (lỗi 1675012 ngày 19/11/2026)."""

from social_crawler.spiders.facebook.auth.graphql_client import RELAY_PROVIDER_ADDITIONS, complete_relay_providers

SEARCH_FLAG = "__relay_internal__pv__SearchCometRelatedSearchThumbnailDesignrelayprovider"


def test_missing_providers_are_added_without_touching_the_original():
    template = {"count": 5, "__relay_internal__pv__IsWorkUserrelayprovider": False}
    completed = complete_relay_providers(template)
    assert set(RELAY_PROVIDER_ADDITIONS) <= set(completed)
    assert completed["count"] == 5 and completed["__relay_internal__pv__IsWorkUserrelayprovider"] is False
    assert template == {"count": 5, "__relay_internal__pv__IsWorkUserrelayprovider": False}


def test_existing_values_are_never_overridden():
    assert complete_relay_providers({SEARCH_FLAG: False})[SEARCH_FLAG] is False


def test_search_template_from_the_library_gets_exactly_the_three_flags_captured_from_a_real_request():
    from social_crawler.spiders.facebook.auth import sdk_fallback

    template = sdk_fallback.templates()["search"].VARIABLES
    added = set(complete_relay_providers(template)) - set(template)
    assert added == set(RELAY_PROVIDER_ADDITIONS)
    assert SEARCH_FLAG in added


def test_other_clients_keep_their_template_unchanged():
    from social_crawler.spiders.common.comet_graphql_client import CometGraphQLClient

    template = {"a": 1}
    assert CometGraphQLClient._complete_template(object(), template) is template
