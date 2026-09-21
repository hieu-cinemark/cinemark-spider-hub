from __future__ import annotations

from social_crawler.spiders.threads.features.comments.extract import (
    extract_replies_from_json,
    extract_replies_from_text_feed,
    find_text_feed_page_info,
)


def test_extract_replies_from_text_feed_reads_thread_items():
    response = {
        "reply_threads": [
            {
                "thread_items": [
                    {
                        "post": {
                            "pk": "111",
                            "code": "abc",
                            "taken_at": 1700000000,
                            "like_count": 3,
                            "caption": {"text": "hello"},
                            "user": {
                                "pk": "99",
                                "username": "bob",
                                "full_name": "Bob",
                                "profile_pic_url": "https://example.com/b.jpg",
                            },
                            "text_post_app_info": {"direct_reply_count": 2},
                        }
                    }
                ]
            }
        ],
        "paging_tokens": {"downward": "cursor-1"},
        "downwards_thread_will_continue": True,
    }
    replies = extract_replies_from_text_feed(response)
    assert len(replies) == 1
    assert replies[0]["reply_id"] == "111"
    assert replies[0]["parent_reply_id"] is None
    assert replies[0]["message"] == "hello"
    assert replies[0]["author_username"] == "bob"
    assert replies[0]["reply_count"] == 2
    page_info = find_text_feed_page_info(response)
    assert page_info["has_next_page"] is True
    assert page_info["end_cursor"] == "cursor-1"


def test_find_text_feed_page_info_stops_without_cursor():
    assert find_text_feed_page_info({"downwards_thread_will_continue": True, "paging_tokens": {}}) == {
        "has_next_page": False,
        "end_cursor": None,
    }


def test_find_text_feed_page_info_accepts_downwards_alias():
    page_info = find_text_feed_page_info(
        {"paging_tokens": {"downwards": "c2"}, "downwards_thread_will_continue": True}
    )
    assert page_info == {"has_next_page": True, "end_cursor": "c2"}


def test_find_text_feed_page_info_continues_when_flag_omitted():
    page_info = find_text_feed_page_info({"paging_tokens": {"downward": "c3"}})
    assert page_info == {"has_next_page": True, "end_cursor": "c3"}


def test_find_text_feed_page_info_continues_when_thread_flag_is_false():
    """Live text_feed pages set downwards_thread_will_continue=false while
    paging_tokens.downwards still points at the next sibling-reply page."""
    page_info = find_text_feed_page_info(
        {"paging_tokens": {"downwards": "c4"}, "downwards_thread_will_continue": False}
    )
    assert page_info == {"has_next_page": True, "end_cursor": "c4"}


def test_extract_replies_from_json_still_reads_graphql_edges():
    response = {
        "data": {
            "media": {
                "text_post_app_info": {
                    "direct_replies": {
                        "edges": [
                            {
                                "node": {
                                    "posts": {
                                        "edges": [
                                            {
                                                "node": {
                                                    "pk": "222",
                                                    "code": "xyz",
                                                    "taken_at": 1,
                                                    "caption": {"text": "graphql"},
                                                    "user": {"pk": "1", "username": "ann", "full_name": "Ann"},
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    replies = extract_replies_from_json(response)
    assert [r["reply_id"] for r in replies] == ["222"]
    assert replies[0]["parent_reply_id"] is None
    assert replies[0]["message"] == "graphql"


def test_extract_text_feed_chain_sets_parent_on_nested_item():
    response = {
        "target_post_id": "root",
        "reply_threads": [
            {
                "thread_items": [
                    {"post": {"pk": "a", "caption": {"text": "top"}, "user": {"username": "ann"}}},
                    {"post": {"pk": "b", "caption": {"text": "nested"}, "user": {"username": "bob"}}},
                ]
            }
        ],
    }
    replies = extract_replies_from_text_feed(response)
    assert [(r["reply_id"], r["parent_reply_id"]) for r in replies] == [("a", None), ("b", "a")]


def test_apply_feed_parent_points_nested_feed_at_expanded_reply():
    from social_crawler.spiders.threads.features.comments.extract import apply_feed_parent

    remapped = apply_feed_parent(
        [
            {"reply_id": "child", "parent_reply_id": None, "reply_count": 0},
            {"reply_id": "grand", "parent_reply_id": "child", "reply_count": 0},
        ],
        feed_id="parent",
        root_post_id="root",
    )
    assert [(r["reply_id"], r["parent_reply_id"]) for r in remapped] == [
        ("child", "parent"),
        ("grand", "child"),
    ]


def test_replies_needing_expand_skips_fully_collected_parents():
    from social_crawler.spiders.threads.features.comments.extract import replies_needing_expand

    replies = [
        {"reply_id": "a", "reply_count": 2},
        {"reply_id": "b", "reply_count": 1},
        {"reply_id": "c", "reply_count": 0},
    ]
    assert replies_needing_expand(replies, {"a": 2, "b": 0}) == ["b"]
