"""Các helper nhỏ an toàn với dict/list để đào qua các response GraphQL lồng sâu của
Facebook, dùng chung cho mọi tính năng dưới spiders/facebook."""

from __future__ import annotations

from typing import Any, Callable, Iterator


def get_path(node: Any, *keys: Any) -> Any:
    """Tra lồng an toàn với dict/list: get_path(x, "a", 0, "b") == x["a"][0]["b"]."""
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
    """Duyệt đệ quy một cây dict/list, yield mọi dict mà predicate(node) là true. Phép duyệt cây
    duy nhất project này cần mỗi khi đường dẫn thật của một trường không chắc giữ ổn định qua
    các lần deploy của Facebook - dùng chung thay vì mỗi chỗ gọi tự viết một bản."""
    if isinstance(node, dict):
        if predicate(node):
            yield node
        for value in node.values():
            yield from iter_matching(value, predicate)
    elif isinstance(node, list):
        for value in node:
            yield from iter_matching(value, predicate)


def find_first(node: Any, predicate: Callable[[dict], bool]) -> dict | None:
    """Giống iter_matching, nhưng dừng ở (và trả về) kết quả khớp đầu tiên, hoặc None nếu không
    có gì khớp."""
    for match in iter_matching(node, predicate):
        return match
    return None
