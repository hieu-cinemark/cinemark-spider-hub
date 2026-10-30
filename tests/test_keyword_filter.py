from social_crawler.spiders.common.keyword_filter import OffTopicGuard, mentions_keyword


def test_mentions_keyword_matches_ingest_folding():
    assert mentions_keyword("Xem phim TRẠI BUÔN NGƯỜI hôm qua", "Trại Buôn Người")
    assert mentions_keyword("traibuonnguoi đỉnh", "Trại Buôn Người")  # bỏ khoảng trắng như ingest
    assert mentions_keyword("Đi xem #TraiBuonNguoi", "#TraiBuonNguoi")
    assert mentions_keyword("Hoài Linh trong Lên Hương", "Lên Hương+Hoài Linh")
    assert not mentions_keyword("Lên Hương", "Lên Hương+Hoài Linh")
    assert not mentions_keyword("Nike phantom low elite fg", "Quỷ Ăn Tạng 4")
    assert not mentions_keyword(None, "x")


def test_guard_stops_after_two_off_topic_pages():
    guard = OffTopicGuard("Mật Mã Đông Dương")
    assert guard.end_page(0, 10) is False
    assert guard.end_page(1, 20) is True  # 2 trang liền < 10%


def test_guard_ignores_small_pages_and_resets():
    guard = OffTopicGuard("x")
    assert guard.end_page(0, 3) is False  # trang ít bài không tính
    assert guard.end_page(0, 10) is False
    assert guard.end_page(5, 10) is False  # trang tốt -> đặt lại
    assert guard.end_page(0, 10) is False


def test_guard_keeps_posts_naming_the_film():
    guard = OffTopicGuard("đi xem Trại Buôn Người", '["Trại Buôn Người"]')
    assert guard.keep("Trại Buôn Người hay quá")  # không có "đi xem" nhưng nhắc tên phim
    assert not guard.keep("Nike phantom low elite")
    scotty = OffTopicGuard("Scotty review", ["SCOTTY: GIẢI CỨU HOÀNG THƯỢNG"])
    assert scotty.keep("Scotty giải cứu hoàng thượng dễ thương")
