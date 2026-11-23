"""Ép chuẩn log và lỗi (docs/logging.md, cùng chuẩn với cinemark-api): log tiếng Việt qua mã sự kiện cố định, lỗi tiếng Anh, một dạng trường lỗi duy nhất.

Quét toàn bộ social_crawler/ bằng AST nên lệch chuẩn là CI đỏ ngay, không cần chờ người review.
"""

import ast
import io
import re
import tokenize
from pathlib import Path

from social_crawler.core.log_messages import MESSAGES

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "social_crawler"
LEVELS = {"debug", "info", "warning", "error", "critical", "exception"}
LOGGER_NAMES = {"logger", "log", "_logger"}
VI = re.compile(r"[À-ỹ]")
# Trường lỗi cũ/khác tên: lỗi luôn đặt vào `error` (reason chỉ dành cho lý do nghiệp vụ như "content_too_short").
BANNED_KWARGS = {"exc", "err", "e", "cause", "message", "msg", "detail", "details", "error_message", "error_msg"}
# Ngoại lệ có lý do. `print` được phép trong hàm `main` / khối `if __name__ == "__main__"` (CLI in kết quả ra stdout) và ở
# `_telegram_worker` (không gọi logger được vì sẽ đệ quy vào chính hàng đợi Telegram). `logging.getLogger` chỉ để gắn handler/bộ lọc
# vào logger của thư viện bên thứ ba hoặc shim cho mã port.
PRINT_ALLOWED_FUNCTIONS = {"main", "_telegram_worker"}
STDLIB_LOGGING_ALLOWED = {"social_crawler/core/crawl_outcome.py", "social_crawler/spiders/youtube/_compat.py"}


def _files():
    for path in sorted(APP.rglob("*.py")):
        if "__pycache__" not in path.parts:
            yield path, ast.parse(path.read_text(encoding="utf-8"))


def _is_log_call(node: ast.AST) -> bool:
    if not (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in LEVELS and node.args
    ):
        return False
    owner = getattr(node.func.value, "id", None) or getattr(node.func.value, "attr", None)
    return owner in LOGGER_NAMES


def _literal_events(node: ast.expr) -> list[str] | None:
    """Mã sự kiện nếu là literal, hoặc `"a" if cond else "b"` với hai nhánh literal; None nếu là f-string/biến."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Attribute) and node.attr.endswith("_log_event"):
        return []  # tên sự kiện nằm trong cấu hình (xem test_configured_log_events_are_in_catalog)
    if isinstance(node, ast.IfExp):
        body, orelse = _literal_events(node.body), _literal_events(node.orelse)
        return body + orelse if body is not None and orelse is not None else None
    return None


def test_events_are_literal_snake_case_and_in_catalog():
    problems = []
    for path, tree in _files():
        for node in ast.walk(tree):
            if not _is_log_call(node):
                continue
            events = _literal_events(node.args[0])
            where = f"{path.relative_to(ROOT)}:{node.lineno}"
            if events is None:
                problems.append(
                    f"{where}: mã sự kiện phải là literal (không f-string/biến); đưa phần đổi vào trường key=value"
                )
                continue
            problems += [f"{where}: {e!r} thiếu trong core/log_messages.py" for e in events if e not in MESSAGES]
    assert not problems, "\n".join(problems)


def test_error_field_is_always_called_error_and_passed_as_exception():
    problems = []
    for path, tree in _files():
        for node in ast.walk(tree):
            if not _is_log_call(node):
                continue
            for kw in node.keywords:
                where = f"{path.relative_to(ROOT)}:{node.lineno}"
                if kw.arg in BANNED_KWARGS:
                    problems.append(f"{where}: dùng error=... thay cho {kw.arg}=...")
                if kw.arg == "error":
                    value = kw.value
                    inner = value.value if isinstance(value, ast.Subscript) else value
                    if isinstance(inner, ast.Call) and getattr(inner.func, "id", "") in {"str", "repr"}:
                        problems.append(
                            f"{where}: truyền thẳng error=exc (bộ xử lý log tự cắt độ dài và điền error_type)"
                        )
    assert not problems, "\n".join(problems)


def _print_context_ok(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    """print chỉ hợp lệ trong hàm CLI `main` / khối `if __name__ == "__main__"` hoặc `_telegram_worker`."""
    while node in parents:
        node = parents[node]
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name in PRINT_ALLOWED_FUNCTIONS:
            return True
        if isinstance(node, ast.If) and "__name__" in ast.unparse(node.test):
            return True
    return False


def test_no_print_and_no_stdlib_logging_in_app_code():
    problems = []
    for path, tree in _files():
        rel = str(path.relative_to(ROOT))
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "print"
                and not _print_context_ok(node, parents)
            ):
                problems.append(f"{rel}:{node.lineno}: dùng logger thay cho print (chỉ CLI main được in ra stdout)")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "getLogger"
                and getattr(node.func.value, "id", "") == "logging"
                and rel not in STDLIB_LOGGING_ALLOWED
            ):
                problems.append(
                    f"{rel}:{node.lineno}: dùng social_crawler.core.logger.get_logger thay cho logging.getLogger"
                )
    assert not problems, "\n".join(problems)


def test_error_messages_are_english():
    problems = []
    for path, tree in _files():
        # mọi lớp lỗi được `raise` đều tính, kể cả lớp không có đuôi Error (SettingsUnavailable, CrawlJobSkipped...)
        raised = {id(n.exc) for n in ast.walk(tree) if isinstance(n, ast.Raise) and isinstance(n.exc, ast.Call)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", "")
            if not (
                name.endswith(("Error", "Exception", "Stop", "NotReady"))
                or name == "HTTPException"
                or id(node) in raised
            ):
                continue
            literals = [
                n.value
                for arg in [*node.args, *(k.value for k in node.keywords)]
                for n in ast.walk(arg)
                if isinstance(n, ast.Constant) and isinstance(n.value, str)
            ]
            if any(VI.search(text) for text in literals):
                problems.append(f"{path.relative_to(ROOT)}:{node.lineno}: message lỗi phải bằng tiếng Anh")
    assert not problems, "\n".join(problems)


def test_swallowed_exceptions_state_why():
    """`except ...: pass/continue` phải có chú thích lý do (cùng dòng hoặc dòng ngay trên), hoặc log/raise thay vì nuốt."""
    problems = []
    for path in sorted(APP.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        comment_lines = {
            t.start[0] for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type == tokenize.COMMENT
        }
        for node in ast.walk(ast.parse(source)):
            if (
                isinstance(node, ast.ExceptHandler)
                and len(node.body) == 1
                and isinstance(node.body[0], ast.Pass | ast.Continue)
            ):
                stmt = node.body[0]
                if not ({stmt.lineno, stmt.lineno - 1, node.lineno} & comment_lines):
                    problems.append(f"{path.relative_to(ROOT)}:{node.lineno}: nuốt lỗi không có lý do")
    assert not problems, "\n".join(problems)


def test_catalog_style():
    problems = [f"{e}: câu phải có tiếng Việt có dấu" for e, m in MESSAGES.items() if not VI.search(m)]
    problems += [f"{e}: bỏ dấu chấm cuối" for e, m in MESSAGES.items() if m.rstrip().endswith(".")]
    problems += [f"{e}: câu dài quá 90 ký tự" for e, m in MESSAGES.items() if len(m) > 90]
    problems += [
        f"{e}: mã sự kiện phải là tiếng Anh snake_case" for e in MESSAGES if not re.fullmatch(r"[a-z][a-z0-9_]*", e)
    ]
    assert not problems, "\n".join(problems)


def test_catalog_is_sorted_within_each_group():
    """Trong mỗi nhóm `# --- ... ---` các mã xếp theo chữ cái để dễ tìm và không trùng."""
    text = (APP / "core" / "log_messages.py").read_text(encoding="utf-8")
    groups: list[list[str]] = []
    for line in text.splitlines():
        if re.match(r"\s*#", line):
            groups.append([])
        elif groups and (m := re.match(r'\s*"([a-z0-9_]+)":', line)):
            groups[-1].append(m.group(1))
    unsorted = [group[:3] for group in groups if group != sorted(group)]
    assert not unsorted, f"nhóm chưa xếp theo chữ cái: {unsorted}"


def test_configured_log_events_are_in_catalog():
    """Sự kiện đặt tên bằng cấu hình (`saved_log_event="..."`) cũng phải có câu tiếng Việt."""
    problems = []
    for path, tree in _files():
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.keyword)
                and node.arg
                and node.arg.endswith("_log_event")
                and isinstance(node.value, ast.Constant)
                and node.value.value not in MESSAGES
            ):
                problems.append(f"{path.relative_to(ROOT)}: {node.value.value!r} thiếu trong core/log_messages.py")
    assert not problems, "\n".join(problems)
