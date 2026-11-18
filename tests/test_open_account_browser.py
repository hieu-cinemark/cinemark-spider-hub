"""Script mở Chrome đăng nhập tay: đúng hồ sơ, giữ chỗ một proxy trống của nền tảng, không mở hai tài khoản cùng cổng."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import open_account_browser as script


def test_profile_keeps_existing_facebook_folders_and_separates_threads():
    assert script.profile_dir("facebook", 126).name == "126"
    assert script.profile_dir("threads", 6).name == "threads-6"


def test_chrome_args_use_the_proxy_and_block_webrtc_leak():
    args = script.chrome_args(Path("/p/126"), "1.2.3.4:80", "https://api.ipify.org")
    assert "--user-data-dir=/p/126" in args and "--proxy-server=http://1.2.3.4:80" in args
    assert "--force-webrtc-ip-handling-policy=disable_non_proxied_udp" in args
    assert args[-1] == "https://api.ipify.org"


def test_does_not_open_a_second_account_on_the_same_proxy():
    running = ["Chrome --user-data-dir=/p/threads-55 --proxy-server=http://1.2.3.4:80"]
    assert "cùng proxy" in script.busy_reason(Path("/p/threads-62"), "1.2.3.4:80", running)
    assert "đang mở" in script.busy_reason(Path("/p/threads-55"), "9.9.9.9:80", running)
    assert script.busy_reason(Path("/p/126"), "9.9.9.9:80", running) is None


def test_login_opens_ip_check_and_facebook_login_without_credentials_in_command():
    args = script.chrome_args(
        Path("/p/129"), "203.0.113.10:8159", "https://api.ipify.org", login_url="https://www.facebook.com/login/"
    )
    assert args[-2:] == ["https://api.ipify.org", "https://www.facebook.com/login/"]
    assert "--proxy-server=http://203.0.113.10:8159" in args
    assert "--user-data-dir=/p/129" in args


class _Cache:
    def __init__(self):
        self.d = {}

    def get(self, key):
        return self.d.get(key)

    def set_nx(self, key, value, ttl):
        if key in self.d:
            return False
        self.d[key] = value
        return True

    def delete_if_equals(self, key, value):
        if self.d.get(key) == value:
            del self.d[key]


ACCOUNT = {"id": 129, "platform": "facebook", "account_id": "test-user", "email": "A@x.com", "enabled": True}
PROXY = {"id": 15, "url": "203.0.113.10:8159", "username": "private-user", "password": "private-password"}


def _common(monkeypatch, cache):
    monkeypatch.setattr(script, "load_accounts", lambda ids: [dict(ACCOUNT)])
    monkeypatch.setattr(script, "RedisCache", lambda: cache)
    monkeypatch.setattr(script, "running_chrome_commands", list)
    monkeypatch.setattr(script, "exit_ip", Mock(return_value="203.0.113.1"))


def test_dry_run_claims_a_free_proxy_then_releases_it_and_never_prints_credentials(monkeypatch, capsys):
    cache = _Cache()
    _common(monkeypatch, cache)
    claim = Mock(return_value=dict(PROXY))
    release = Mock()
    monkeypatch.setattr(script.proxy_lease, "claim", claim)
    monkeypatch.setattr(script.proxy_lease, "release", release)
    launch = Mock()
    monkeypatch.setattr(script.subprocess, "run", launch)

    assert script.main(["129", "--login", "--dry-run"]) == 0

    # Giữ chỗ bằng đúng khoá của job cào (email, chữ thường) rồi nhả ngay vì chỉ xem kế hoạch.
    assert claim.call_args.args[:2] == ("facebook", "a@x.com")
    assert release.call_args.args[:2] == (PROXY, "a@x.com")
    launch.assert_not_called()
    output = capsys.readouterr().out
    assert "proxy #15" in output
    assert "private-user" not in output and "private-password" not in output


def test_opening_holds_the_lease_until_interrupted(monkeypatch):
    cache = _Cache()
    _common(monkeypatch, cache)
    monkeypatch.setattr(script.proxy_lease, "claim", Mock(return_value=dict(PROXY)))
    monkeypatch.setattr(script.subprocess, "run", Mock())
    hold = Mock()
    monkeypatch.setattr(script, "hold_until_interrupted", hold)

    assert script.main(["129", "--login"]) == 0

    held = hold.call_args.args[0]
    assert held == [(PROXY, "a@x.com")]


def test_no_free_proxy_skips_the_account(monkeypatch, capsys):
    cache = _Cache()
    _common(monkeypatch, cache)
    monkeypatch.setattr(script.proxy_lease, "claim", Mock(return_value=None))
    launch = Mock()
    monkeypatch.setattr(script.subprocess, "run", launch)

    assert script.main(["129"]) == 0
    launch.assert_not_called()
    assert "đang bận" in capsys.readouterr().out


def test_override_refuses_a_port_another_account_is_using(monkeypatch):
    cache = _Cache()
    cache.d["proxy_lease:203.0.113.10:8159"] = "someone-else"
    _common(monkeypatch, cache)
    monkeypatch.setattr(script, "proxy_by_port", lambda platform, port: dict(PROXY))

    with pytest.raises(SystemExit):
        script.main(["129", "--proxy-port", "8159"])


def test_override_does_not_launch_when_same_proxy_is_already_open(monkeypatch):
    cache = _Cache()
    _common(monkeypatch, cache)
    monkeypatch.setattr(script, "proxy_by_port", lambda platform, port: {**PROXY, "url": "h:8159"})
    monkeypatch.setattr(
        script, "running_chrome_commands", lambda: ["Chrome --user-data-dir=/p/other --proxy-server=http://h:8159"]
    )
    check_ip = Mock()
    monkeypatch.setattr(script, "exit_ip", check_ip)
    assert script.main(["129", "--proxy-port", "8159", "--login"]) == 0
    check_ip.assert_not_called()
    assert "proxy_lease:h:8159" not in cache.d  # không mở thì nhả chỗ ngay


@pytest.mark.parametrize("rows", [[], [{"id": 15}, {"id": 20}]])
def test_missing_or_ambiguous_proxy_does_not_fall_back(monkeypatch, rows):
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.execute.return_value.fetchall.return_value = rows
    monkeypatch.setattr(script, "connect", lambda: connection)
    with pytest.raises(ValueError, match="đúng một proxy"):
        script.proxy_by_port("facebook", 8159)


def test_proxy_override_requires_one_explicit_account():
    with pytest.raises(SystemExit) as exc:
        script.main(["--proxy-port", "8159"])
    assert exc.value.code == 2
