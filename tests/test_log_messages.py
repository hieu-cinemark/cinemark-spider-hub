"""Mọi sự kiện log trong mã nguồn phải là mã snake_case VÀ có câu tiếng Việt trong core/log_messages.py (log mặc định tiếng Việt)."""

import ast
import re
from pathlib import Path

from social_crawler.core.log_messages import MESSAGES, describe

ROOT = Path(__file__).resolve().parent.parent / "social_crawler"
LEVELS = {"debug", "info", "warning", "error", "critical", "exception"}
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")


def _events() -> dict[str, str]:
    found = {}
    for path in ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in LEVELS
                and node.args
            ):
                continue
            owner = getattr(node.func.value, "id", None) or getattr(node.func.value, "attr", None)
            first = node.args[0]
            if (
                owner in ("logger", "log", "_logger")
                and isinstance(first, ast.Constant)
                and isinstance(first.value, str)
            ):
                found.setdefault(first.value, f"{path.relative_to(ROOT.parent)}:{node.lineno}")
    return found


def test_every_event_is_snake_case_and_has_a_vietnamese_message():
    events = _events()
    bad = {e: w for e, w in events.items() if not SNAKE.match(e)}
    assert not bad, f"Mã sự kiện phải là snake_case (dữ liệu để trong key=value): {bad}"
    missing = {e: w for e, w in events.items() if e not in MESSAGES}
    assert not missing, "Thiếu câu tiếng Việt trong social_crawler/core/log_messages.py:\n" + "\n".join(
        f"  {e}  ({w})" for e, w in sorted(missing.items())
    )


def test_catalog_has_no_stale_or_english_entries():
    events = _events()
    stale = sorted(set(MESSAGES) - set(events))
    assert not stale, f"Sự kiện trong danh mục nhưng không còn trong mã: {stale[:10]}"
    assert all(re.search(r"[À-ỹ]|[a-z]", text) and text != key for key, text in MESSAGES.items())


def test_describe_falls_back_to_the_code():
    assert describe("proxy_degraded").startswith("Proxy")
    assert describe("khong_co_trong_danh_muc") == "khong_co_trong_danh_muc"
