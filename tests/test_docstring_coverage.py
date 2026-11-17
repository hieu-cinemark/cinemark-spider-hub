"""Chặn hàm/lớp công khai dài mà thiếu docstring: người đọc sau phải biết nó làm gì mà không cần đọc hết thân hàm.

Ngưỡng 40 dòng; hàm ngắn hơn hoặc tên bắt đầu bằng "_" được bỏ qua. Thêm một dòng mô tả (đủ để biết hàm nhận gì, làm gì, trả gì) là qua.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIRS = ["social_crawler", "scripts"]
MIN_LINES = 40


def _undocumented() -> list[str]:
    found = []
    for folder in DIRS:
        for path in sorted((ROOT / folder).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            stack = [(tree, "")]
            while stack:
                node, prefix = stack.pop()
                for child in ast.iter_child_nodes(node):
                    if not isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                        continue
                    name = prefix + child.name
                    nested_in_func = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
                    length = (child.end_lineno or child.lineno) - child.lineno + 1
                    is_public = not nested_in_func and not child.name.startswith("_")
                    if is_public and length >= MIN_LINES and not ast.get_docstring(child):
                        found.append(f"{path.relative_to(ROOT)}::{name} ({length} dòng)")
                    if isinstance(child, ast.ClassDef):
                        stack.append((child, name + "."))
    return found


def test_long_public_members_have_docstrings():
    missing = _undocumented()
    assert not missing, "Thiếu docstring:\n" + "\n".join(missing)
