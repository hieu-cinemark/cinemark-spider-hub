"""Script mở Chrome nuôi tay: đúng hồ sơ, đúng proxy, không mở hai tài khoản cùng IP."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import open_account_browser as script


def test_profile_keeps_existing_facebook_folders_and_separates_threads():
    assert script.profile_dir("facebook", 126).name == "126"
    assert script.profile_dir("threads", 6).name == "threads-6"


def test_chrome_args_use_the_assigned_proxy_and_block_webrtc_leak():
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


def test_dry_run_can_open_unassigned_account_with_explicit_proxy_without_modifying_row(monkeypatch, capsys):
    account = {
        "id": 129,
        "platform": "facebook",
        "account_id": "test-user",
        "assigned_proxy_id": None,
        "proxy_url": None,
    }
    proxy = {"id": 15, "ip_type": "static", "proxy_url": "203.0.113.10:8159", "username": "private-user", "password": "private-password"}
    monkeypatch.setattr(script, "load_accounts", lambda ids: [account])
    load_proxy = Mock(return_value=proxy)
    monkeypatch.setattr(script, "load_proxy_for_account", load_proxy)
    monkeypatch.setattr(script, "running_chrome_commands", list)
    exit_ip = Mock(return_value="203.0.113.1")
    monkeypatch.setattr(script, "exit_ip", exit_ip)
    launch = Mock()
    monkeypatch.setattr(script.subprocess, "run", launch)
    assert script.main(["129", "--proxy-port", "8159", "--login", "--dry-run"]) == 0
    load_proxy.assert_called_once_with(8159, 129)
    assert exit_ip.call_args.args[0]["assigned_proxy_id"] == 15
    assert account["assigned_proxy_id"] is None and account["proxy_url"] is None
    launch.assert_not_called()
    output = capsys.readouterr().out
    assert "private-user" not in output and "private-password" not in output


def test_override_does_not_launch_when_same_proxy_is_already_open(monkeypatch):
    monkeypatch.setattr(
        script, "load_accounts", lambda ids: [{"id": 129, "platform": "facebook", "account_id": "test", "session_client": None}]
    )
    monkeypatch.setattr(
        script,
        "load_proxy_for_account",
        lambda p, a: {"id": 15, "ip_type": "static", "proxy_url": "h:8159", "username": "u", "password": "p"},
    )
    monkeypatch.setattr(
        script, "running_chrome_commands", lambda: ["Chrome --user-data-dir=/p/other --proxy-server=http://h:8159"]
    )
    check_ip = Mock()
    monkeypatch.setattr(script, "exit_ip", check_ip)
    assert script.main(["129", "--proxy-port", "8159", "--login"]) == 0
    check_ip.assert_not_called()


@pytest.mark.parametrize("rows", [[], [{"id": 15}, {"id": 20}]])
def test_missing_or_ambiguous_proxy_does_not_fall_back(monkeypatch, rows):
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.execute.return_value.fetchall.return_value = rows
    monkeypatch.setattr(script, "connect", lambda: connection)
    with pytest.raises(ValueError, match="đúng một proxy"):
        script.load_proxy_for_account(8159, 129)


def test_proxy_override_requires_one_explicit_account():
    with pytest.raises(SystemExit) as exc:
        script.main(["--proxy-port", "8159"])
    assert exc.value.code == 2
