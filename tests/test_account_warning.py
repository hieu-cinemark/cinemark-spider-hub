"""accounts/account_warning.py: dò cảnh báo "hành vi tự động" / "tạm thời bị chặn" của Meta (30/10/2026)."""

from social_crawler.accounts import account_warning


def test_find_marker_english_and_vietnamese():
    assert account_warning.find_marker("We suspect automated behaviour on your account") == "suspect automated behavio"
    assert account_warning.find_marker("You’re Temporarily Blocked\nIt looks like you were misusing this feature")
    assert account_warning.find_marker("Chúng tôi nghi ngờ có hành vi tự động trên tài khoản của bạn")
    assert account_warning.find_marker("Bạn tạm thời bị chặn")


def test_find_marker_ignores_normal_text():
    assert account_warning.find_marker("Lịch chiếu phim tuần này - đặt vé ngay") is None
    assert account_warning.find_marker("") is None


class _Locator:
    def __init__(self, texts, count=0):
        self._texts = texts
        self._count = count

    def all(self):
        return [_Text(t) for t in self._texts]

    def count(self):
        return self._count


class _Text:
    def __init__(self, text):
        self._text = text

    def inner_text(self, timeout=None):
        return self._text


class _Page:
    def __init__(self, dialogs, body, articles):
        self._dialogs, self._body, self._articles = dialogs, body, articles

    def locator(self, selector):
        if "dialog" in selector:
            return _Locator(self._dialogs)
        return _Locator([], self._articles)

    def inner_text(self, selector, timeout=None):
        return self._body


def test_detect_warning_reads_body_only_without_posts():
    # Bài viết trên bảng tin nói về "hoạt động bất thường trên tài khoản" không phải cảnh báo của Meta.
    feed = _Page([], "Cảnh giác: hoạt động bất thường trên tài khoản ngân hàng", articles=5)
    assert account_warning.detect_warning(feed) is None
    blocked = _Page([], "Bạn tạm thời bị chặn", articles=0)
    assert account_warning.detect_warning(blocked) == "bạn tạm thời bị chặn"
    dialog = _Page(["We suspect automated behavior"], "", articles=5)
    assert account_warning.detect_warning(dialog) == "suspect automated behavio"
