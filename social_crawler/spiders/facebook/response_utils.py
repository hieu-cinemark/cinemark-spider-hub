"""Small dict/list-safe helpers for digging through Facebook's deeply
nested GraphQL responses, shared across every feature under spiders/facebook."""

from __future__ import annotations

from typing import Any


def get_path(node: Any, *keys: Any) -> Any:
    """dict/list-safe nested lookup: get_path(x, "a", 0, "b") == x["a"][0]["b"]."""
    for key in keys:
        if isinstance(key, int):
            if not isinstance(node, list) or key >= len(node):
                return None
            node = node[key]
        else:
            if not isinstance(node, dict):
                return None
            node = node.get(key)
    return node
