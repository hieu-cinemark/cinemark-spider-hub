"""Small dict/list-safe helpers for digging through Facebook's deeply
nested GraphQL responses, shared across every feature under spiders/facebook."""

from __future__ import annotations

from typing import Any, Callable, Iterator


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


def iter_matching(node: Any, predicate: Callable[[dict], bool]) -> Iterator[dict]:
    """Recursively walk a dict/list tree, yielding every dict for which
    predicate(node) is true. The one tree-walk this project needs whenever a
    field's real path isn't guaranteed to stay stable across Facebook
    deploys - shared instead of every caller hand-rolling its own copy."""
    if isinstance(node, dict):
        if predicate(node):
            yield node
        for value in node.values():
            yield from iter_matching(value, predicate)
    elif isinstance(node, list):
        for value in node:
            yield from iter_matching(value, predicate)


def find_first(node: Any, predicate: Callable[[dict], bool]) -> dict | None:
    """Like iter_matching, but stops at (and returns) the first match, or
    None if nothing matches."""
    for match in iter_matching(node, predicate):
        return match
    return None
