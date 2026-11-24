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


def test_search_template_covers_every_provider_of_the_browser_request_2026_11_24():
    """Request tìm kiếm thật bắt từ trình duyệt ngày 24/11/2026 (SearchCometResultsPaginatedResultsQuery, doc_id
    29201009449557424): thiếu bất kỳ cờ nào trong số này là Facebook trả lỗi 1675012 missing_required_variable_value."""
    from facebook_gql_scraper import facebookGraphql as gql

    browser_providers = {
        "SearchCometRelatedSearchThumbnailDesign",
        "GHLShouldChangeAdIdFieldName",
        "GHLShouldChangeSponsoredDataFieldName",
        "CometFeedStory_enable_reactor_facepile",
        "CometFeedStory_enable_social_bubbles",
        "CometFeedStory_enable_post_permalink_white_space_click",
        "CometUFICommentActionLinksRewriteEnabled",
        "IsWorkUser",
        "CometUFICommentAvatarStickerAnimatedImage",
        "CometUFICommentActionLinkStableIDs",
        "ShowWorkplaceAgentAttribution",
        "TestPilotShouldIncludeDemoAdUseCase",
        "FBReels_deprecate_short_form_video_context_gk",
        "CometUFI_dedicated_comment_routable_dialog_gk",
        "FBReels_enable_view_dubbed_audio_type_gk",
        "CometFeedShareMedia_shouldPrefetchShareImage",
        "CometImmersivePhotoCanUserDisable3DMotion",
        "WorkCometIsEmployeeGKProvider",
        "IsMergQAPolls",
        "FBReelsMediaFooter_comet_enable_reels_ads_gk",
        "CometUFIReactionsEnableShortName",
        "CometUFICommentAutoTranslationType",
        "GeminiUFISaveToKnowledgeHubGK",
        "CometUFIShareActionMigration",
        "CometUFISingleLineUFI",
        "relay_provider_comet_ufi_ssr_seo_defer",
        "FBReelsIFUTileContent_reelsIFUPlayOnHover",
        "GroupsCometGYSJFeedItemHeight",
        "StoriesShouldEnablePhotosensitiveContentWarning",
        "ShouldEnableBakedInTextStories",
        "StoriesShouldIncludeFbNotes",
    }
    completed = complete_relay_providers(gql.SearchCometResultsPaginatedResultsQueryGraphQL.VARIABLES)
    missing = {name for name in browser_providers if f"__relay_internal__pv__{name}relayprovider" not in completed}
    assert not missing, missing
