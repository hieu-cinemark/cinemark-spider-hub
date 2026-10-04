"""Bài video/reel: FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery stream danh sách
comment trong một chunk bị defer (@defer) dưới một đường dẫn khác với bài thường (đã xác
nhận 2026-09-28). Payload synthetic có dạng thật, cắt gọn còn các trường mà extract_comment
đọc."""

from __future__ import annotations

import json

from social_crawler.spiders.comet_graphql_client import _parse_graphql_response
from social_crawler.spiders.facebook.features.comments.extract import extract_comments, find_comments_page_info


def _comment(n: int) -> dict:
    return {
        "node": {
            "id": f"comment-{n}",
            "legacy_fbid": str(1000 + n),
            "body": {"text": f"bình luận {n}"},
            "created_time": 1790000000 + n,
            "author": {"name": f"Người {n}", "id": str(n)},
            "feedback": {"replies_fields": {"total_count": n}, "comment_rendering_instance": {}},
        }
    }


def _video_response(comments: list[dict]) -> str:
    initial = {"data": {"video": {"id": "v1", "creation_story": {"id": "s1"}}}, "extensions": {}}
    affiliate = {"label": "Q$defer$Affiliate", "path": ["video", "creation_story"], "data": {"affiliate": None}}
    feedback = {
        "label": "Q$defer$FBUnifiedVideoFeedbackComments",
        "path": ["video", "creation_story"],
        "data": {
            "reels_feedback_renderer": {
                "story": {
                    "feedback": {
                        "comment_list_renderer": {
                            "feedback": {
                                "comment_rendering_instance_for_feed_location": {
                                    "comments": {
                                        "edges": comments,
                                        "page_info": {"has_next_page": True, "end_cursor": "CURSOR"},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    }
    return "for (;;);" + "\n".join(json.dumps(o, ensure_ascii=False) for o in (initial, affiliate, feedback))


def test_deferred_chunks_are_merged_at_their_path() -> None:
    parsed = _parse_graphql_response(_video_response([_comment(1)]))
    story = parsed["data"]["video"]["creation_story"]
    assert story["id"] == "s1"  # giữ payload ban đầu
    assert "affiliate" in story and "reels_feedback_renderer" in story


def test_video_post_comments_are_extracted() -> None:
    parsed = _parse_graphql_response(_video_response([_comment(1), _comment(2)]))
    comments = extract_comments(parsed)
    assert [c["message"] for c in comments] == ["bình luận 1", "bình luận 2"]
    assert comments[0]["legacy_comment_id"] == "1001"
    assert find_comments_page_info(parsed) == {"has_next_page": True, "end_cursor": "CURSOR"}


def test_single_line_response_unchanged() -> None:
    body = {"data": {"node": {"comment_rendering_instance_for_feed_location": {"comments": {"edges": [_comment(3)]}}}}}
    parsed = _parse_graphql_response(json.dumps(body))
    assert parsed == body
    assert len(extract_comments(parsed)) == 1


def test_unparseable_trailing_chunk_is_ignored() -> None:
    raw = json.dumps({"data": {"node": {"id": "x"}}}) + "\n{not json"
    assert _parse_graphql_response(raw) == {"data": {"node": {"id": "x"}}}


def test_comment_post_id_decodes_owner() -> None:
    import base64

    from social_crawler.spiders.facebook.features.comments.extract import comment_post_id

    cid = base64.b64encode(b"comment:1107826854923656_1400546332182567").decode().rstrip("=")
    assert comment_post_id(cid) == "1107826854923656"
    assert comment_post_id("not-base64!") is None
    assert comment_post_id(None) is None


def test_bootstrap_rejects_media_viewer_comments_query() -> None:
    from urllib.parse import urlencode

    import pytest

    from social_crawler.spiders.facebook.auth.request_capture import pick_comments_request

    class _Req:
        def __init__(self, name: str, variables: dict) -> None:
            self.post_data = urlencode({"fb_api_req_friendly_name": name, "variables": json.dumps(variables)})

    video = ("FBUnifiedVideoFeedbackRightRailWithCommentPreloadingQuery", {"initial_node_id": "1"})
    root = ("CommentListComponentsRootQuery", {"id": "ZmVlZGJhY2s6MQ=="})
    named = [(_Req(n, v), n) for n, v in (video, root)]
    assert pick_comments_request(named) is named[1][0]
    with pytest.raises(RuntimeError):
        pick_comments_request(named[:1])
