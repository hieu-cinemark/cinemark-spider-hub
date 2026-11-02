"""browser_interaction.launch_chromium: mọi trình duyệt chặn WebRTC lộ IP thật qua UDP (02/11/2026)."""

from social_crawler.spiders.facebook.auth import browser_interaction


class _Chromium:
    def __init__(self):
        self.calls = []

    def launch(self, **kwargs):
        self.calls.append(kwargs)
        return "browser"


class _PW:
    def __init__(self):
        self.chromium = _Chromium()


def test_webrtc_policy_added_and_caller_args_kept():
    pw = _PW()
    browser_interaction.launch_chromium(pw, headless=True, proxy=None, args=["--window-size=1920,1200"])
    args = pw.chromium.calls[0]["args"]
    assert "--webrtc-ip-handling-policy=disable_non_proxied_udp" in args
    assert "--window-size=1920,1200" in args


def test_webrtc_policy_not_duplicated():
    pw = _PW()
    flag = "--webrtc-ip-handling-policy=disable_non_proxied_udp"
    browser_interaction.launch_chromium(pw, headless=True, args=[flag])
    assert pw.chromium.calls[0]["args"].count(flag) == 1


class _Browser:
    def __init__(self):
        self.kwargs = None

    def new_context(self, **kwargs):
        self.kwargs = kwargs

        class _Ctx:
            def new_page(self, *a, **k):
                return None

            def on(self, *a, **k):
                pass

        return _Ctx()


def test_mac_profile_gets_retina_and_screen_taller_than_viewport():
    browser = _Browser()
    mac_ua = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    )
    browser_interaction.new_context(browser, account_key="a@x", user_agent=mac_ua)
    kw = browser.kwargs
    assert kw["device_scale_factor"] == 2
    assert kw["screen"]["width"] == kw["viewport"]["width"]
    assert kw["screen"]["height"] > kw["viewport"]["height"]


def test_caller_display_kwargs_win():
    browser = _Browser()
    browser_interaction.new_context(browser, account_key="a@x", device_scale_factor=1)
    assert browser.kwargs["device_scale_factor"] == 1
