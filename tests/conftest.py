import pytest


@pytest.fixture(autouse=True)
def _no_db_for_new_account_ramp(monkeypatch):
    """search_budget.ramp_factor tra danh sách tài khoản mới trong DB thật ở mỗi request - test không được chạm DB.
    Test của phần tăng tải tự đặt lại _new_account_keys."""
    from social_crawler.services import search_budget

    monkeypatch.setattr(search_budget, "_new_account_keys", lambda: set())
