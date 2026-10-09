"""Helper Playwright dùng chung cho các spider điều khiển trình duyệt thật để kích hoạt lô
kế tiếp của một feed cuộn vô hạn (hashtag_search/search/comments của TikTok, phân trang
reply của Threads) - mỗi cái cần cùng một cách sửa cho cùng một vấn đề gốc (xem
scroll_feed_to_bottom)."""

from __future__ import annotations

from social_crawler.logger import get_logger

logger = get_logger(__name__)

_FIND_SCROLL_CONTAINER_JS = """() => {
    let best = null, bestScore = 0;
    for (const el of document.querySelectorAll('*')) {
        // documentElement/body are excluded, not just deprioritized - their
        // clientHeight is the full viewport height by definition, so
        // whenever the page's own overall height overflows the viewport by
        // 200px+ (true of nearly every real page, just from header/footer
        // chrome), one of these two would otherwise beat any *nested*
        // content container - which is normally shorter, having a header/
        // nav carved out of it already - purely on being outermost, not on
        // being the actual feed. Confirmed live (2026-09-15, TikTok
        // hashtag_search): this picked <html> (clientHeight == the launched
        // viewport's own 900px) over the page's real feed region, so every
        // scroll_feed_to_bottom() call scrolled the wrong element and the
        // capture loop stalled out after its first (often only) page -
        // same root cause the module docstring already ruled out window/
        // body scrolling for, just re-appearing one level up here.
        if (el === document.documentElement || el === document.body) continue;
        const sh = el.scrollHeight, ch = el.clientHeight;
        if (sh > ch + 200 && ch > 300 && ch > bestScore) {
            bestScore = ch;
            best = el;
        }
    }
    if (!best) return false;
    best.scrollTop = best.scrollHeight;
    return true;
}"""


def scroll_feed_to_bottom(page, item_selector: str | None = None) -> None:
    """Thử lần lượt mọi cách mà project này đã xác nhận thực sự làm một feed TikTok/Threads bị
    kẹt chạy tiếp - xếp chồng thay vì chọn một, vì mỗi cách chỉ đôi khi có tác dụng và chưa
    cách nào được chứng minh là tự nó đủ (xem ba tầng bên dưới, mỗi tầng đã xác nhận bằng
    thử trực tiếp thực tế ngày 2026-09-15 với hashtag_search của TikTok):

    1. Tìm lại phần tử nào hiện là vùng cuộn feed/kết quả lồng bên trong của trang và đặt
       scrollTop của nó xuống đáy. documentElement/body bị loại khỏi danh sách ứng viên,
       không chỉ bị hạ ưu tiên - clientHeight của chúng theo định nghĩa là toàn bộ chiều cao
       viewport, nên bất cứ khi nào chiều cao tổng của trang vượt viewport từ 200px trở lên
       (gần như trang thật nào cũng vậy, chỉ riêng phần header/footer) thì một trong hai sẽ
       thắng mọi vùng chứa *lồng* bên trong - vốn thường ngắn hơn vì bị trừ phần header/nav
       - chỉ vì nằm ngoài cùng, không phải vì là feed thật. Đã xác nhận thực tế: đúng bug
       này đã chọn <html> thay cho vùng feed thật, làm việc bắt dữ liệu kẹt sau trang đầu
       tiên (thường là duy nhất). Tìm lại vùng chứa ở mỗi lần gọi thay vì cache một lần, vì
       phần tử nào đủ điều kiện có thể thay đổi khi feed mount thêm - đã xác nhận thực tế nó
       chuyển từ "có một vùng chứa lồng thật" sang "không có cái nào" ngay khi skeleton đang
       tải của trang unmount và nội dung thật thay vào.

    2. item_selector (từng chỗ gọi tự bật, ví dụ "a[href*='/video/']" cho TikTok - một mẫu
       link ổn định sống sót qua các lần thiết kế lại markup/CSS): khi (1) không tìm thấy
       gì, cuộn item feed *cuối cùng* đã render khớp mẫu vào tầm nhìn bằng
       scroll_into_view_if_needed của chính Playwright, vốn tự xác định chuỗi cuộn thật
       thay vì hàm này phải đoán. None (mặc định) bỏ qua tầng này - mọi chỗ gọi có từ trước
       khi có nó giữ nguyên hành vi cũ.

    3. Một lần lăn chuột thật, được *đặt đúng vị trí* ở giữa viewport. Dễ đánh giá nhầm là
       vô dụng: page.mouse.wheel() bắn tại vị trí con chuột ảo đang nằm, mặc định là (0, 0)
       (góc trên trái, ví dụ trên thanh nav) cho tới khi có gì đó di chuyển nó - đã xác nhận
       thực tế bắn từ đó thì window.scrollY vẫn đứng ở 0 dù documentElement có khoảng
       2000px tràn thật, đo được, nhưng di chuột lên đúng feed trước thì *cùng* lời gọi lăn
       đó đẩy window.scrollY bình thường, tới tận mức tối đa thật của phần tràn đó. Giữ làm
       phương án cuối, không phải tự nó là cách sửa: cũng đã xác nhận thực tế việc cuộn tới
       tối đa theo cách này tự nó không làm một feed bị đứng (hasMore=true, nhưng không tải
       thêm) chạy tiếp - dù cơ chế kích hoạt riêng của TikTok cho việc đó là gì, nó không đơn
       giản là "người dùng đã cuộn tới đáy". Thất bại lặng lẽ ở đây (bắt, không raise) là có
       chủ đích - cơ chế chống kẹt riêng của mỗi chỗ gọi vốn đã tự quyết định "cuộn mà không
       ra trang mới" có nghĩa gì với nó, bất kể một trang cụ thể hoá ra cần tầng nào trong
       ba tầng này.
    """
    try:
        moved = page.evaluate(_FIND_SCROLL_CONTAINER_JS)
    except Exception as exc:
        logger.warning("scroll_to_bottom_failed", error=str(exc))
        moved = False

    if moved:
        return

    if item_selector:
        try:
            items = page.locator(item_selector)
            count = items.count()
            if count:
                items.nth(count - 1).scroll_into_view_if_needed(timeout=5000)
        except Exception as exc:
            logger.warning("scroll_into_view_fallback_failed", error=str(exc), item_selector=item_selector)

    try:
        viewport = page.viewport_size or {"width": 1366, "height": 900}
        page.mouse.move(viewport["width"] / 2, viewport["height"] / 2)
        page.mouse.wheel(0, viewport["height"] * 1.5)
    except Exception as exc:
        logger.warning("wheel_fallback_failed", error=str(exc))
