"""
Client HTTP thường (không trình duyệt) cho các endpoint có ký của TikTok. Khác
Facebook/Threads, ở đây hoàn toàn không có bước bootstrap qua trình duyệt - xem docstring
module của constants/tiktok.py để biết vì sao không bao giờ đụng tới trình duyệt sau khi
danh tính của tài khoản (cookie/device_id/odin_id) đã được bắt một lần từ một phiên trình
duyệt thật, đã được tin cậy. Mọi request sau đó - kể cả mọi trang phân trang - đều được ký
mới, ở local, ngay tại đây (xem signature/gnarly.py), không cần cache doc_id hay token.

TikTokClient mang mọi thứ không phụ thuộc endpoint nào đang được gọi (nạp danh tính/proxy,
bóp nhịp, ký, thử lại/backoff) - một tính năng mới kế thừa nó và chỉ thêm method riêng,
như TikTokHashtagClient bên dưới. Xem docstring của _request() cho thứ duy nhất mà method
của mọi lớp con vẫn tự lo."""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import urlencode

from curl_cffi import requests as curl_requests

from social_crawler.clients import proxy_provider
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.tiktok import (
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    COMMENT_ITEM_LIST_URL,
    COMMENT_REPLY_LIST_URL,
    CURL_CFFI_IMPERSONATE_TARGET,
    CURL_CFFI_UA,
    HASHTAG_DETAIL_URL,
    HASHTAG_ITEM_LIST_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    SIGNED_QUERY_PARAM_ORDER,
    STATIC_PARAMS,
    STATIC_X_BOGUS,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.db.proxies import platform_has_any_proxy
from social_crawler.db.proxy_settings import get_setting
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.tiktok.auth.accounts import is_logged_in_cookie, next_account
from social_crawler.spiders.tiktok.auth.cookies import cookie_map
from social_crawler.spiders.tiktok.signature.dynosaur import get_X_Dynosaur
from social_crawler.spiders.tiktok.signature.gnarly import get_X_Gnarly

logger = get_logger(__name__)

# device_id/odinId của một danh tính synthetic chỉ cần trông giống của TikTok (id số lớn,
# cùng số chữ số với ví dụ "7685251565930628616") - đã xác nhận bằng thử trực tiếp thực tế
# (2026-09-17) rằng item_list chế độ khách không kiểm tra chúng với registry nào phía server,
# chỉ cần X-Gnarly của request khớp với chính query string của nó. Khoảng bắt đầu bằng một
# chữ số đầu hợp lý chỉ để cho đẹp, không quan trọng.
_SYNTHETIC_ID_MIN = 10**18
_SYNTHETIC_ID_MAX = 10**19 - 1


def _generate_synthetic_id() -> str:
    return str(random.randint(_SYNTHETIC_ID_MIN, _SYNTHETIC_ID_MAX))


# Danh tính synthetic lấy IP từ một gói slot xoay vòng riêng của nhà cung cấp
# (tiktok_synthetic_provider trong proxy_settings - mặc định "proxiestrust_tiktok_us", IP
# đầu ra ở Mỹ), tách khỏi gói mặc định dành cho VN - xem docstring của
# clients/proxy_provider.py để biết vì sao tuyệt đối không được dùng chung một token. Tạo
# lease mới cho mỗi client synthetic (xem __init__ bên dưới) thay vì lưu một cái trong
# platform_proxies: các lease này hết hạn sau khoảng 15-20 phút (time_seconds_to_die của
# nhà cung cấp), nên một dòng DB tĩnh sẽ âm thầm bắt đầu báo 407 khi lease đó hết hạn.

# Điều chỉnh kiểu AIMD cho khoảng bóp nhịp thích ứng theo thiết bị (xem
# TikTokClient._adjust_interval) - cùng dạng tăng/giảm với comet_graphql_client.py của
# Facebook/Threads: tăng nhanh khi có bất kỳ dấu hiệu căng thẳng nào (một lần thử lại là đủ
# để phản ứng), giảm chậm để một request sạch ngay sau một đợt khó khăn không xoá ngay sự
# thận trọng.
_ADAPTIVE_INTERVAL_GROWTH_FACTOR = 1.7
_ADAPTIVE_INTERVAL_DECAY_FACTOR = 0.85
# Một khoảng đã tăng sống được bao lâu mà không có tín hiệu căng thẳng mới trước khi
# _current_interval quay về đọc lại MIN_REQUEST_INTERVAL_SECONDS - một thiết bị đã khó khăn
# 10 phút từ một giờ trước thì giờ không nên còn bị bóp nhịp quá thận trọng.
_ADAPTIVE_INTERVAL_TTL_SECONDS = 1800


class TikTokBlockedError(RuntimeError):
    """TikTok trả về response rỗng/bị từ chối - cookie của tài khoản (ttwid/msToken/verifyFp)
    nhiều khả năng đã cũ, hoặc device_id/odin_id của nó mất tin cậy. Bắt lại danh tính của tài
    khoản từ một phiên trình duyệt thật và cập nhật dòng platform_accounts của nó
    (platform='tiktok')."""


class TikTokRateLimitedError(RuntimeError):
    """TikTok đang giới hạn rate danh tính/IP này kể cả sau khi thử lại với backoff. Không phải
    danh tính chết - bắt lại không giúp gì, hãy lùi lại và thử lại sau."""


class TikTokNetworkError(RuntimeError):
    """Mọi lần thử lại đều không nhận được response HTTP nào (proxy sập, lỗi DNS, lỗi bắt tay
    TLS, timeout) - TikTok thực ra chưa hề thấy request này, nên vấn đề không phải danh tính
    của tài khoản. Kiểm tra kết nối tới dòng platform_proxies đã cấu hình (platform='tiktok')
    thay vì bắt lại cookie/device_id/odin_id."""


class TikTokClient:
    """Bộ máy danh tính/session/ký/thử lại dùng chung cho mọi client endpoint của TikTok - không
    có gì ở đây riêng cho hashtag. Lớp con thêm các method endpoint gọi self._request(...); xem
    TikTokHashtagClient bên dưới để thấy dạng."""

    def __init__(
        self,
        redis_cache: RedisCache | None = None,
        *,
        exclude_ids: set[str] | None = None,
        require_login: bool = False,
        force_guest: bool = False,
        synthetic: bool = False,
    ):
        self._redis = redis_cache or RedisCache()
        self._synthetic = synthetic
        # Chỉ bao giờ được đặt thành một ProxyRow platform_proxies thật (không bao giờ là lease nhà
        # cung cấp tạm thời của nhánh synthetic - xem comment của nhánh đó) - _record_proxy_outcome_once
        # bên dưới không làm gì khi cái này còn None, y như cách của comet_graphql_client.py.
        self._proxy_cfg: dict[str, Any] | None = None
        self._proxy_outcome_recorded = False

        if synthetic:
            # Bất biến mà cả nhánh này tồn tại để giữ: device_id/odinId, IP proxy, và bộ cookie
            # ttwid/csrf/chain bên dưới phải được tạo cùng nhau, một lần, cho đúng một instance client
            # này - không bao giờ giữ danh tính mới trên IP cũ, hay chuyển danh tính cũ sang IP mới giữa
            # phiên (xem docstring của module này để biết vì sao chính một danh tính *dùng lại* mới là
            # thứ khiến TikTok nghi ngờ ngay từ đầu; ghép danh tính/IP không nhất quán là cùng loại tín
            # hiệu bất thường). Thử lại nghĩa là một TikTokClient(synthetic=True) hoàn toàn mới - không
            # bao giờ vá chỉ một trong ba thứ này lên client có sẵn.
            #
            # Danh tính chỉ-khách được tạo mới cho đúng instance client này, hoàn toàn không liên quan
            # dòng platform_accounts nào - đã xác nhận bằng A/B test trực tiếp thực tế (2026-09-17) rằng
            # một bộ device_id/odinId/cookie hoàn toàn mới, chưa từng được trình duyệt đụng tới, có quyền
            # truy cập khách tin cậy đầy đủ y như một tài khoản thật đã bắt, miễn X-Gnarly được ký đúng.
            # Kiểu lỗi mà cách này né được: một device_id *dùng lại* tích luỹ tín hiệu lạm dụng riêng
            # của TikTok từ lưu lượng tự động lặp lại (đã xác nhận thực tế cùng ngày - ba dòng
            # platform_accounts sống lâu khác nhau, ba proxy khác nhau, đều bắt đầu cần một X-Dynosaur
            # thật mà project này không có bản cài đặt local nào chạy được, trong khi một danh tính mới
            # cùng phiên không cần) - nên cách sửa không phải chữ ký tốt hơn, mà là không bao giờ dùng
            # lại một danh tính đủ lâu để bị nghi ngờ ngay từ đầu. Xem docstring module của
            # hashtag_search/search.py cho dấu vết điều tra đầy đủ hơn.
            self._device_id = _generate_synthetic_id()
            self._odin_id = _generate_synthetic_id()
            self._cookies: dict[str, str] = {}
            self._ms_token = ""
            self._verify_fp = ""
            self._is_logged_in = False
            account_email = None

            # Một lease IP mới đi cặp với danh tính mới ở trên - xem comment cấp module gần
            # _generate_synthetic_id để biết vì sao đây là gói riêng thay vì platform_proxies.
            # ip_allowlist của dòng provider (mặc định bật cho proxiestrust_tiktok_us): đã xác nhận thực
            # tế (2026-09-17) lease xác thực bằng username:password mặc định cứ xoay vòng qua cùng khoảng
            # 4 IP, phần lớn đã bị TikTok chặn; lease theo IP allowlist (proxy_ip_allow) lấy từ một pool
            # khác hẳn, hiện còn sạch - xem docstring của get_new_proxy cho cơ chế và một lưu ý của nó
            # (chỉ máy đã gọi get_new mới dùng được, đúng là chuyện xảy ra ở đây). Quay về pool proxy DB
            # thường khi provider đó không có token (ví dụ môi trường dev không có), như trước khi có danh
            # tính synthetic.
            lease = proxy_provider.get_new_proxy(provider_key=str(get_setting("tiktok_synthetic_provider")))
            if lease is not None:
                proxy_cfg = {
                    "url": f"{lease['host']}:{lease['port']}",
                    "username": lease["username"],
                    "password": lease["password"],
                }
                # Không phải dòng platform_proxies (self._proxy_cfg giữ None, bên dưới) - một lease API nhà
                # cung cấp mới, dùng một lần không bao giờ dùng lại, nên không có cooldown nào để theo dõi:
                # lease tồi chỉ có nghĩa là lần gọi get_new_proxy kế tiếp tạo một cái khác, không phải "chờ IP
                # này hồi phục".
            else:
                proxy_cfg = pool.acquire_proxy("tiktok")
                self._proxy_cfg = proxy_cfg
                if proxy_cfg is None and platform_has_any_proxy("tiktok"):
                    # Cùng quy tắc "không bao giờ chạy lưu lượng crawl thường ngày không proxy" như đường không
                    # synthetic bên dưới - xem docstring của ProxyPoolExhaustedError.
                    raise TikTokNetworkError(
                        "tiktok: no usable proxy available for a synthetic guest identity."
                    ) from pool.ProxyPoolExhaustedError(
                        "tiktok: no usable proxy available for a synthetic guest identity."
                    )
        else:
            account = next_account(self._redis, exclude_ids=exclude_ids, require_login=require_login)
            if account is None:
                if require_login:
                    raise RuntimeError(
                        "No enabled logged-in tiktok account (cookie needs sessionid). "
                        "Paste a logged-in Cookie header on the TikTok tab, then Try saved session."
                    )
                raise RuntimeError(
                    "No enabled tiktok row in platform_accounts. Capture cookie/device_id/odin_id "
                    "from a real browser session's DevTools first - see this module's docstring "
                    "(account_id -> device_id, token -> odin_id, cookie -> raw Cookie header)."
                )

            cookies = cookie_map(account["cookie"])
            missing = [name for name in ("ttwid",) if name not in cookies]
            if missing:
                raise RuntimeError(
                    f"tiktok platform_accounts row is missing required cookie(s) {missing} - "
                    "re-capture from a real browser session."
                )
            self._cookies = cookies
            self._ms_token = cookies.get("msToken", "")
            self._verify_fp = cookies.get("s_v_web_id", "")
            self._device_id = account["id"]
            self._odin_id = account["token"]
            account_email = account.get("email") or None
            # sessionid là cookie phiên đăng nhập thật của TikTok web - chỉ có khi trường cookie của tài
            # khoản này được bắt từ (hoặc được dán vào từ) một phiên trình duyệt đã đăng nhập thật, không
            # chỉ là một lần ghé với tư cách khách. Đã xác nhận bằng thử trực tiếp là trả về nhiều kết quả
            # hơn đáng kể cho mỗi hashtag so với danh tính chỉ-khách khi request được trình duyệt thật ký
            # - xem tham số user_is_login của _request() bên dưới.
            #
            # force_guest ghi đè thành False bất kể trạng thái đăng nhập thật của cookie - client này
            # (TikTokHashtagClient/phát lại comment) ký ở local bằng gnarly.py, thứ chỉ tạo ra chữ ký mà
            # TikTok chấp nhận cho request *khách* (user_is_login=false); request đã đăng nhập còn cần một
            # header X-Dynosaur thật mà chỉ JS của TikTok tính được (đã xác nhận bằng thử trực tiếp -
            # project này không có bản cài đặt local nào, xem comment POST_ITEM_LIST_URL trong
            # constants/tiktok.py cho dấu vết điều tra đầy đủ hơn), nên một request ký ở local mà khai là
            # đã đăng nhập chỉ nhận response rỗng kể cả với cookie sessionid hoàn toàn hợp lệ. Mọi tài
            # khoản tiktok đang bật tình cờ đều mang sessionid thật (bắt cho thử nghiệm đăng nhập bằng
            # trình duyệt) - không có phần ghi đè này, client này sẽ không còn tài khoản dùng được nào để
            # xoay sang.
            self._is_logged_in = False if force_guest else is_logged_in_cookie(account["cookie"])

            try:
                proxy_cfg = pool.acquire_proxy_for_account("tiktok", account["id"], required=True)
                self._proxy_cfg = proxy_cfg
            except pool.ProxyPoolExhaustedError as exc:
                # required=True: đây là lưu lượng crawl thường ngày - không bao giờ quay về chạy không proxy
                # (xem docstring của ProxyPoolExhaustedError). Raise lại thành TikTokNetworkError để nó đi qua
                # cùng phần xử lý thử lại/cảnh báo Telegram mà mọi spider TikTok vốn đã có cho "proxy sập"
                # (xem ví dụ `except TikTokNetworkError` trong hashtag_search/search.py).
                raise TikTokNetworkError(str(exc)) from exc

        proxy = None
        if proxy_cfg:
            # Một lease synthetic theo IP allowlist (xem nhánh ip_allowlist ở trên) hoàn toàn không mang
            # username/password - cấp quyền theo IP nguồn, không theo thông tin đăng nhập - nên dựng một
            # URL user:pass@ cho nó sẽ nhúng chuỗi literal "None:None@" vào.
            if proxy_cfg.get("username") and proxy_cfg.get("password"):
                proxy_url = f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}"
            else:
                proxy_url = f"http://{proxy_cfg['url']}"
            proxy = {"http": proxy_url, "https": proxy_url}
        logger.info(
            "tiktok_session_ready",
            device_id=self._device_id,
            email=account_email,
            proxy=proxy_cfg["url"] if proxy_cfg else None,
            logged_in=self._is_logged_in,
            synthetic=synthetic,
        )

        # impersonate=CURL_CFFI_IMPERSONATE_TARGET, không phải alias "chrome" trần - xem docstring
        # của CURL_CFFI_UA để biết vì sao hai cái phải luôn là một cặp khớp nhau (một bug đã xác
        # nhận thực tế, không phải thận trọng cho có: "chrome" trần cộng một UA khai một phiên bản
        # Chrome mà curl_cffi không có dấu vân tay TLS đã nhận response rỗng ở đúng từng request, bất
        # kể danh tính/IP).
        self._session = curl_requests.Session(impersonate=CURL_CFFI_IMPERSONATE_TARGET, proxies=proxy)
        self._last_request_at: float | None = None

        if synthetic:
            # Tạo ttwid/tt_csrf_token/tt_chain_token qua header Set-Cookie của một lệnh GET thường - đã
            # xác nhận thực tế (2026-09-17) việc này không cần JS gì cả, giả TLS Chrome của curl_cffi là
            # đủ. self._session (một Session curl_cffi, không phải request một lần) giữ chúng cho mọi lời
            # gọi sau của client này.
            try:
                mint_resp = self._session.get(
                    "https://www.tiktok.com/",
                    headers={"user-agent": CURL_CFFI_UA},
                    timeout=15,
                )
            except curl_requests.RequestsError as exc:
                raise TikTokNetworkError(f"Failed to mint a synthetic guest session: {exc}") from exc
            # Kéo vào một dict thường (thay vì để ngầm trong jar riêng của session) để mọi lời gọi bên
            # dưới rõ ràng về thứ nó đang gửi, giống self._cookies của đường không synthetic.
            self._cookies = dict(mint_resp.cookies)

    def _record_proxy_outcome_once(self, *, success: bool) -> None:
        """Cấp dữ liệu cho circuit breaker của pool.py (cooldown_until/consecutive_failures trên dòng
        platform_proxies) để một proxy cứ lỗi thực sự bị cooldown thay vì được giao ra lại ở lần
        gọi acquire_proxy_for_account kế tiếp - client này trước đây lấy proxy mà không bao giờ báo
        lại chuyện gì đã xảy ra với nó, nên breaker không bao giờ hoạt động cho lưu lượng riêng của
        TikTok (comet_graphql_client.py của Facebook/Threads vốn đã làm việc này - xem
        _record_proxy_outcome_once của nó). Cùng cơ chế "chỉ lời gọi đầu tiên trong đời client mới
        tính": một request thử lại 3 lần bên trong không được ghi 3 kết quả riêng cho một lời gọi
        logic. self._proxy_cfg là None với đường lease synthetic (không có gì để trả lại - xem
        comment của nó trong __init__), nên theo thiết kế hàm này không làm gì ở đó."""
        if self._proxy_cfg is None or self._proxy_outcome_recorded:
            return
        self._proxy_outcome_recorded = True
        pool.release_proxy(self._proxy_cfg, success=success)

    def _throttle_key(self) -> str:
        return THROTTLE_REDIS_KEY_TMPL.format(device_id=self._device_id)

    def _current_interval(self) -> float:
        """Khoảng giãn cách cơ sở dùng ngay lúc này - bình thường là MIN_REQUEST_INTERVAL_SECONDS,
        hoặc một giá trị cao hơn lưu trong Redis nếu thiết bị này gần đây gặp lỗi 429/5xx/mạng
        (xem _adjust_interval). Lưu bền (không chỉ trong bộ nhớ) vì một lần chạy `scrapy crawl` là
        tiến trình con sống ngắn - không có Redis thì một lượt chạy bị bóp ngay trước khi thoát sẽ
        không dạy được gì cho lượt sau."""
        stored = self._redis.get(self._throttle_key())
        if stored is None:
            return MIN_REQUEST_INTERVAL_SECONDS
        return max(MIN_REQUEST_INTERVAL_SECONDS, float(stored))

    def _adjust_interval(self, *, stressed: bool) -> None:
        """Được gọi sau khi mỗi request kết thúc: tăng khoảng đã lưu khi có bất kỳ tín hiệu
        429/5xx/mạng nào, giảm dần lại khi response sạch và lần gọi này chưa có tín hiệu nào. Xem
        các hằng _ADAPTIVE_INTERVAL_* cấp module cho hệ số tăng/giảm và TTL. Cố ý không lấy dữ liệu
        từ response TikTokBlockedError (body rỗng) - đó là tín hiệu tin cậy danh tính (xem
        docstring của lỗi đó), không phải tín hiệu nhịp độ, và chậm thêm cũng không làm một
        device_id/cookie đã cũ hợp lệ lại."""
        key = self._throttle_key()
        stored = self._redis.get(key)
        if stored is None:
            if not stressed:
                return  # đã ở mức cơ sở - không có gì để lưu
            current = MIN_REQUEST_INTERVAL_SECONDS
        else:
            current = max(MIN_REQUEST_INTERVAL_SECONDS, float(stored))

        if stressed:
            new_interval = min(ADAPTIVE_INTERVAL_MAX_SECONDS, current * _ADAPTIVE_INTERVAL_GROWTH_FACTOR)
        else:
            new_interval = max(MIN_REQUEST_INTERVAL_SECONDS, current * _ADAPTIVE_INTERVAL_DECAY_FACTOR)

        if new_interval <= MIN_REQUEST_INTERVAL_SECONDS:
            self._redis.delete(key)
            return

        logger.info(
            "adaptive_interval_adjusted",
            device_id=self._device_id,
            stressed=stressed,
            interval_seconds=round(new_interval, 2),
        )
        self._redis.set(key, new_interval, ttl_seconds=_ADAPTIVE_INTERVAL_TTL_SECONDS)

    def _throttle(self) -> None:
        base_interval = self._current_interval()
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _ordered_query_pairs(self, params: dict[str, str]) -> list[tuple[str, str]]:
        """Phát các cặp (key, value) theo SIGNED_QUERY_PARAM_ORDER. Bộ kiểm tra challenge item_list
        của TikTok nhạy với thứ tự của query string đã ký — xem comment của hằng đó. Key không biết
        giữ thứ tự chèn tương đối sau phần tiền tố đã biết."""
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for key in SIGNED_QUERY_PARAM_ORDER:
            if key in params:
                pairs.append((key, params[key]))
                seen.add(key)
        for key, value in params.items():
            if key not in seen:
                pairs.append((key, value))
        return pairs

    def _request(
        self,
        endpoint: str,
        extra_params: dict[str, str],
        referer: str,
        *,
        sign_dynosaur: bool = False,
    ) -> dict[str, Any]:
        """Ký và gửi một lệnh GET tới `endpoint`. `referer` là URL đầy đủ mà một trình duyệt thật lẽ
        ra đang ở khi bắn request này - ví dụ một trang hashtag (`/tag/<name>`) hoặc trang kết quả
        tìm kiếm (`/search?q=<query>`) - method của lớp con tự dựng cái này vì đó là thứ duy nhất
        thực sự khác theo endpoint; không có gì khác ở đây phải đổi để thêm endpoint mới.

        `sign_dynosaur`: /api/comment/list/ từ chối request chỉ có Gnarly bằng HTTP 200 + body
        rỗng; get_X_Dynosaur tính ở local mở được nó (đã xác nhận bằng A/B thực tế 2026-09-18).
        item_list của hashtag phải giữ False — nó chạy không cần Dynosaur và không được đổi dạng.
        """
        # Ưu tiên msToken vừa được Set-Cookie bởi response trước (hành vi jar của trình duyệt). Quay
        # về giá trị lúc tạo là ổn khi chưa có cái mới nào tới.
        jar_ms = ""
        try:
            jar_ms = self._session.cookies.get("msToken") or self._cookies.get("msToken") or ""
        except Exception:
            jar_ms = self._cookies.get("msToken") or ""
        if jar_ms:
            self._ms_token = jar_ms

        params = {
            **STATIC_PARAMS,
            **extra_params,
            "WebIdLastTime": str(int(time.time())),
            "device_id": self._device_id,
            "odinId": self._odin_id,
            "referer": referer,
            "root_referer": referer,
            "msToken": self._ms_token,
            "user_is_login": "true" if self._is_logged_in else "false",
        }
        # Đã xác nhận thực tế 2026-09-17: tham số query verifyFp= rỗng (thứ khách synthetic luôn tạo
        # ra) biến một request đang chạy thành HTTP 200 + body rỗng. Khách trình duyệt thật bỏ hẳn key
        # khi không có dấu vân tay — chỉ gửi khi không rỗng.
        if self._verify_fp:
            params["verifyFp"] = self._verify_fp

        # Sắp thứ tự trước khi ký — urlencode(dict) sẽ dùng thứ tự chèn từ phép trộn ở trên, đúng
        # dạng "prod_client_order" đã A/B ra rỗng so với một bản bắt từ trình duyệt giống hệt mọi thứ
        # khác (xem SIGNED_QUERY_PARAM_ORDER).
        ordered = self._ordered_query_pairs(params)
        query_string = urlencode(ordered)
        gnarly = get_X_Gnarly(query_string, "", CURL_CFFI_UA)
        ordered.append(("X-Bogus", STATIC_X_BOGUS))
        ordered.append(("X-Gnarly", gnarly))
        if sign_dynosaur:
            # Ký trên cùng query cơ sở mà Gnarly đã dùng (trước Bogus/Gnarly).
            ordered.append(("X-Dynosaur", get_X_Dynosaur(query_string, CURL_CFFI_UA, "")))

        url = f"{endpoint}?{urlencode(ordered)}"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,vi;q=0.8",
            "referer": referer,
            "user-agent": CURL_CFFI_UA,
        }

        logger.info(
            "sending_request",
            endpoint=endpoint,
            logged_in=self._is_logged_in,
            cookie_count=len(self._cookies),
            cookie_names=sorted(self._cookies),
            referer=referer,
            sign_dynosaur=sign_dynosaur,
            **extra_params,
        )
        self._throttle()
        resp = self._post_with_retry(url, headers)

        # Giữ cookie danh tính đồng bộ với những gì TikTok đã xoay ở response này (đặc biệt là
        # msToken — mỗi item_list thành công đều Set-Cookie một cái mới).
        try:
            for name, value in resp.cookies.items():
                self._cookies[name] = value
                if name == "msToken":
                    self._ms_token = value
        except Exception:
            pass

        if len(resp.content) == 0:
            if self._synthetic:
                # Không có dòng platform_accounts nào để bảo vệ và cũng chẳng có ích gì khi theo dõi chuỗi bị
                # chặn theo một id mà client này sẽ không bao giờ dùng lại - lần thử lại của chính chỗ gọi
                # (một TikTokClient mới, danh tính synthetic mới) vốn đã là cách sửa, xem start() của
                # hashtag_search/search.py.
                logger.warning(
                    "tiktok_empty_response",
                    status_code=resp.status_code,
                    body_len=0,
                    endpoint=endpoint,
                    logged_in=self._is_logged_in,
                    synthetic=True,
                )
            else:
                from social_crawler.db.accounts import disable_account

                key = f"tiktok_block_streak:{self._device_id}"
                streak = self._redis.incr(key)
                self._redis.expire(key, 1800)
                logger.warning(
                    "tiktok_empty_response",
                    status_code=resp.status_code,
                    body_len=0,
                    streak=streak,
                    endpoint=endpoint,
                    logged_in=self._is_logged_in,
                )

                if streak >= 3:
                    disabled = disable_account(
                        "tiktok", self._device_id, reason="repeated empty response (likely stale identity)"
                    )
                    logger.error(
                        "tiktok_account_disabled_repeated_block",
                        telegram=True,
                        device_id=self._device_id,
                        disabled=disabled,
                    )
            raise TikTokBlockedError(
                f"TikTok returned an empty response (status={resp.status_code}). The account's "
                "identity has likely gone stale - re-capture cookie/device_id/odin_id."
            )

        return resp.json()

    def _post_with_retry(self, url: str, headers: dict[str, str]):
        last_exc: Exception | None = None
        resp = None
        # True ngay khi bất kỳ lần thử nào trong lời gọi này gặp 429/5xx/lỗi mạng - cấp cho
        # _adjust_interval để một request chỉ thành công sau khi thử lại vẫn được tính là căng thẳng,
        # không phải response sạch.
        stressed = False
        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, headers=headers, cookies=self._cookies, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed",
                    device_id=self._device_id,
                    attempt=attempt,
                    max_retries=MAX_RETRIES,
                    error=str(exc),
                )
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    stressed = True
                    logger.warning(
                        "tiktok_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    self._record_proxy_outcome_once(success=not stressed)
                    return resp

            if attempt < MAX_RETRIES:
                # Jitter cộng thêm vào mức cơ sở tăng theo cấp số - cùng lý do như jitter thử lại của
                # facebook/auth/graphql_client.py.
                delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                    0, RETRY_BACKOFF_JITTER_SECONDS
                )
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

        if resp is not None and resp.status_code == 429:
            raise TikTokRateLimitedError(
                f"TikTok rate-limited this request (status=429) even after {MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp ở đây vẫn là None - mọi lần thử đều raise RequestsError (lỗi ở cấp kết nối), chưa hề
        # tới được server của TikTok, nên đây là vấn đề mạng/proxy, không phải danh tính đã cũ.
        raise TikTokNetworkError(f"Request failed after {MAX_RETRIES} attempts: {last_exc}") from last_exc


class TikTokHashtagClient(TikTokClient):
    def resolve_hashtag(self, name: str) -> str | None:
        """id TikTok dạng số của một hashtag, cho trước tên (không có '#' ở đầu, không khoảng trắng -
        ví dụ "holinhtrangsi"). None nếu TikTok không có hashtag đó. id này là thứ `challenge_id`
        của search_hashtag() cần - nó không đổi, nên chỗ gọi có thể tra một lần rồi dùng lại cho
        mọi lời gọi search_hashtag()/phân trang sau đó."""
        name = name.lstrip("#").strip()
        if not name.isascii() or " " in name:
            raise ValueError(
                f"{name!r} isn't a TikTok hashtag slug - pass the actual tag "
                "(no spaces/diacritics, e.g. 'holinhtrangsi'), not a movie "
                "title or display keyword. curl_cffi can't put non-ASCII "
                "text in a header, and TikTok's real hashtag ids look "
                "nothing like a Vietnamese title anyway."
            )
        data = self._request(HASHTAG_DETAIL_URL, {"challengeName": name}, referer=f"https://www.tiktok.com/tag/{name}")
        return (data.get("challengeInfo") or {}).get("challenge", {}).get("id")

    def search_hashtag(self, challenge_id: str, cursor: int = 0, count: int = 30, hashtag: str = "") -> dict[str, Any]:
        """Lấy một trang video của một hashtag (trang đầu khi cursor là 0). `challenge_id` là id
        hashtag dạng số của TikTok - không phải chính tên hashtag (xem resolve_hashtag). Referer
        phải là URL công khai `/tag/<slug>`, khớp với challenge/detail - `/tag/<id>` là thứ mà
        item_list đã đăng nhập từ chối bằng 200 rỗng."""
        slug = hashtag.lstrip("#").strip() or challenge_id
        return self._request(
            HASHTAG_ITEM_LIST_URL,
            {"challengeID": challenge_id, "count": str(count), "cursor": str(cursor)},
            referer=f"https://www.tiktok.com/tag/{slug}",
        )


class TikTokCommentClient(TikTokClient):
    """Client curl_cffi khách cho /api/comment/list/ — cùng đường tạo synthetic như
    TikTokHashtagClient, nhưng mọi request còn mang thêm X-Dynosaur tính ở local (xem
    _request(sign_dynosaur=True))."""

    def warm_session(self) -> None:
        """Chạy hashtag detail + item_list để jar nhận msToken trước comment/list. Chuyển đổi thực tế
        (2026-09-18): riêng Dynosaur chưa đủ với một khách nguội — lần A/B thành công luôn gọi
        item_list trước (thứ Set-Cookie msToken); riêng challenge/detail thì không. Lỗi ở đây được
        bỏ qua — list_comments vẫn chạy."""
        try:
            data = self._request(
                HASHTAG_DETAIL_URL,
                {"challengeName": "phimviet"},
                referer="https://www.tiktok.com/tag/phimviet",
            )
            challenge_id = (data.get("challengeInfo") or {}).get("challenge", {}).get("id")
            if not challenge_id:
                logger.info("tiktok_comment_warm_no_challenge", device_id=self._device_id)
                return
            self._request(
                HASHTAG_ITEM_LIST_URL,
                {"challengeID": str(challenge_id), "count": "4", "cursor": "0"},
                referer="https://www.tiktok.com/tag/phimviet",
            )
        except TikTokBlockedError:
            logger.info("tiktok_comment_warm_empty", device_id=self._device_id)

    def list_comments(
        self,
        aweme_id: str,
        *,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """Một trang comment cấp một của `aweme_id` (id video TikTok). `video_url` nên là permalink
        công khai dùng làm Referer; quay về đường dẫn synthetic /video/<id> khi chỗ gọi chỉ có id.
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{aweme_id}"
        return self._request(
            COMMENT_ITEM_LIST_URL,
            {
                "aweme_id": str(aweme_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "enter_from": "tiktok_web",
                "is_non_personalized": "false",
                "current_region": "VN",
            },
            referer=referer,
            sign_dynosaur=True,
        )

    def list_replies(
        self,
        *,
        comment_id: str,
        item_id: str,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """Một trang reply dưới một `comment_id` cấp một trên video `item_id`. Cùng cách ký Dynosaur
        như list_comments; đã xác nhận thực tế 2026-09-18 với /api/comment/list/reply/ (cursor bắt
        đầu từ 0).
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{item_id}"
        return self._request(
            COMMENT_REPLY_LIST_URL,
            {
                "comment_id": str(comment_id),
                "item_id": str(item_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "current_region": "VN",
            },
            referer=referer,
            sign_dynosaur=True,
        )
