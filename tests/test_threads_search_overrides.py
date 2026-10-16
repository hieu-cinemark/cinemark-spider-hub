"""threads graphql_client._search_overrides: biến "recent" (tab "Gần đây") giữ đúng kiểu giá trị của mẫu đã bắt."""

from __future__ import annotations

from social_crawler.spiders.threads.auth.graphql_client import _search_overrides


def test_not_recent_leaves_template_value_alone() -> None:
    assert "recent" not in _search_overrides("q", None, 10, False, {"recent": 0})


def test_recent_matches_int_template() -> None:
    assert _search_overrides("q", "c", 10, True, {"recent": 0})["recent"] == 1


def test_recent_matches_bool_template() -> None:
    assert _search_overrides("q", None, 10, True, {"recent": False})["recent"] is True


def test_recent_without_template_key_is_ignored() -> None:
    assert "recent" not in _search_overrides("q", None, 10, True, {})
