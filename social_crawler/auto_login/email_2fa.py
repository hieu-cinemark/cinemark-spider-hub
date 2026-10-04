"""Lấy mã 2FA gửi qua email bằng IMAP.

Đường dự phòng khi tài khoản không có totp_secret (xem MissingTotpSecretError của
social_crawler.spiders.facebook.auth.triggers.submit_two_factor_code - cùng vấn đề, chỉ
là giải quyết theo cách khác). Form đăng nhập Facebook/threads đôi khi gửi mã xác minh
6 chữ số qua email thay vì hỏi mã TOTP; màn hình cài đặt 2FA của nền tảng cho mỗi người
dùng tự chọn kênh, nên cùng một tài khoản hôm nay có thể cần TOTP, hôm khác lại cần
email.

Đọc hộp thư của `account.email` (dùng `account.email_password`) qua IMAP. Chỉ đọc thư:
  1) tới (INTERNALDATE của server) vào hoặc sau `since_unix` - thời điểm chỗ gọi bắt
     đầu thử đăng nhập, hoặc mặc định là `_LOOKBACK_SECONDS` gần nhất. IMAP SEARCH
     SINCE chỉ chính xác tới ngày, nên điều kiện này được kiểm tra trên từng thư, mới
     nhất trước,
  2) do Facebook/threads gửi (khớp envelope-from / header From với `_KNOWN_SENDERS`),
  3) chưa đọc (cờ IMAP \\Seen là thứ gần nhất, dùng được ở mọi nơi, cho ý "ta chưa dùng
     thư này" - thư được lấy bằng BODY.PEEK nên việc quét không lật cờ \\Seen, còn thư
     có mã được trả về thì được đánh dấu \\Seen rõ ràng để lần thử sau không dùng lại).

Tách mã 6 chữ số bằng regex thay vì parse HTML - bố cục hiển thị của email "mã xác minh
đăng nhập" của Facebook và email "mã của bạn" của threads khác nhau rất nhiều, nhưng mọi
biến thể đã thử (và vài biến thể trong ảnh chụp trung tâm trợ giúp của họ) đều có chuỗi
số thô trong thân thư.

Các kiểu lỗi được báo ra riêng biệt:
  - Chưa cấu hình email: trả về None (chỗ gọi có thể chuyển sang
    mark_needs_manual_login với reason="no_email_for_2fa").
  - Chưa cấu hình email_password: như trên.
  - Lỗi kết nối/xác thực IMAP: raise Email2FAUnreachableError (server IMAP trục trặc
    tạm thời không nên làm tắt tài khoản).
  - Không có email khớp trong cửa sổ thời gian: trả về None (mã của một lần đăng nhập
    thật có thể chưa tới - chỗ gọi quyết định thử lại hay chuyển sang
    needs_manual_login).
  - Tìm thấy email nhưng thân thư không có mã 6 chữ số: trả về None kèm log cảnh báo;
    chỗ gọi tự quyết định có thử lại không.
"""

from __future__ import annotations

import imaplib
import re
import time
from dataclasses import dataclass
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from typing import Any

from social_crawler.logger import get_logger

logger = get_logger(__name__)


class Email2FAUnreachableError(RuntimeError):
    """Server IMAP không truy cập được / sai thông tin đăng nhập / xác thực thất bại - khác
    với "chưa có mã nào tới", vì mail server sập tạm thời không nên làm tắt tài khoản (cùng
    lý do như MissingTotpSecretError trong facebook/auth/triggers.py: đừng nhầm một lỗ
    hổng tự động hoá với một vấn đề thật của tài khoản)."""


class Email2FANotConfiguredError(RuntimeError):
    """Tài khoản chưa đặt cột email/email_password - lỗ hổng cấu hình, không phải lỗi tạm
    thời. Orchestrator auto-login bắt lỗi này và mark_needs_manual_login() dòng đó luôn,
    không thử lại."""


# Cửa sổ nhìn lại cho câu hỏi "đã có email xác minh mới chưa?". 5 phút là dư cho một
# email Facebook/threads bình thường (thường dưới 30s, đôi khi 1-2 phút khi chậm) và đủ
# ngắn để vòng thử lại không lấy nhầm mã của người khác trong cùng hộp thư nếu người dùng
# thật sự đã đăng nhập ở chỗ khác trước.
_LOOKBACK_SECONDS = 300
# Chờ bao lâu để có email khớp sau khi bắt đầu kiểm tra định kỳ. Đủ dài để chờ hết một
# lần gửi mail chậm của Facebook, đủ ngắn để một lượt đăng nhập lại không làm treo lượt
# lập lịch 10 phút vì một hộp thư hỏng hẳn.
_POLL_TIMEOUT_SECONDS = 90
_POLL_INTERVAL_SECONDS = 5
# Timeout cho từng thao tác socket - _POLL_TIMEOUT_SECONDS chỉ giới hạn vòng kiểm tra
# định kỳ, không giới hạn một lần kết nối/đọc tới mail host đã ngừng trả lời.
_IMAP_TIMEOUT_SECONDS = 30

# Khớp chuỗi con trên header From. Chuỗi con chữ thường, không dùng regex, vì đây là các
# chuỗi ổn định ai cũng biết (facebookmail.com, threads.net, instagram.com) - siết chặt
# hơn bằng regex không đáng công bảo trì.
_KNOWN_SENDERS = (
    "facebookmail.com",
    "threads.net",
    "instagram.com",
    "facebook.com",
)

# Mẫu mã 6 chữ số. Có thư bọc mã kiểu "Your code is 123 456" (có khoảng trắng); có thư
# đặt nó trên một dòng riêng; có thư dùng ký tự unicode "·" làm dấu phân cách. Lớp ký tự
# bên dưới khớp cả ba.
_CODE_PATTERN = re.compile(r"(?<!\d)(\d[\d  ·\u00b7]{4,11}\d)(?!\d)")


@dataclass
class _Config:
    host: str
    port: int
    # IMAP qua TLS ngầm định (993) là mặc định; STARTTLS (143) là dự phòng. ssl=False +
    # starttls=True phủ trường hợp hiếm nhà cung cấp không hỗ trợ TLS ngầm định. None nghĩa
    # là "không ghi đè - dùng mặc định của imaplib.IMAP4".
    ssl: bool


# Ánh xạ host/port IMAP mặc định theo nhà cung cấp mail - sau này người vận hành có thể
# ghi đè theo từng tài khoản qua env / dashboard nếu cần. Để ở đây thay vì env.py vì đây
# không phải cấu hình nền tảng, mà là cấu hình nhà cung cấp mail.
_PROVIDER_DEFAULTS: dict[str, _Config] = {
    "gmail.com": _Config("imap.gmail.com", 993, True),
    "googlemail.com": _Config("imap.gmail.com", 993, True),
    "outlook.com": _Config("imap.outlook.com", 993, True),
    "hotmail.com": _Config("imap.outlook.com", 993, True),
    "live.com": _Config("imap.outlook.com", 993, True),
    "yahoo.com": _Config("imap.mail.yahoo.com", 993, True),
    "yandex.com": _Config("imap.yandex.com", 993, True),
    "yandex.ru": _Config("imap.yandex.com", 993, True),
}


def _config_for(email: str) -> _Config:
    """Chọn đúng host/port IMAP cho domain của email này. Quay về đoán chung
    `_Config("imap." + domain, 993, True)` nếu domain không có trong danh sách quen thuộc -
    chạy được với hầu hết hệ thống mail tự host / doanh nghiệp theo quy ước
    imap.<domain>:993, kèm một dòng log rõ ràng cho số ít trường hợp không theo để người vận
    hành thêm một dòng `_PROVIDER_DEFAULTS`."""
    domain = email.rsplit("@", 1)[-1].lower()
    cfg = _PROVIDER_DEFAULTS.get(domain)
    if cfg:
        return cfg
    fallback = _Config(f"imap.{domain}", 993, True)
    logger.info("email_2fa_imap_host_guessed", domain=domain, host=fallback.host)
    return fallback


def _decode_subject(raw: str | None) -> str:
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw


def _normalize_digits(s: str) -> str:
    """Bỏ khoảng trắng và dấu chấm giữa unicode khỏi một cụm mã đã khớp, chỉ trả về các chữ
    số. "123 456" -> "123456"; "123·456" -> "123456". Có biến thể Facebook chèn mã kèm một
    khoảng trắng; có biến thể dùng thin-space hoặc dấu chấm giữa."""
    return re.sub(r"[\s \u00b7·]+", "", s)


def _extract_code(body: str) -> str | None:
    """Chuỗi 6 chữ số đầu tiên trong thân thư còn nguyên sau khi chuẩn hoá. Nếu có nhiều ứng
    viên (ví dụ một số điện thoại + một mã), chuỗi ngắn nhất >= 6 chữ số thắng - mã xác
    minh thật luôn đúng 6 chữ số, còn số điện thoại / mã khách hàng thường dài hơn."""
    candidates = [_normalize_digits(m.group(1)) for m in _CODE_PATTERN.finditer(body)]
    candidates = [c for c in candidates if c.isdigit() and len(c) == 6]
    if not candidates:
        return None
    # Chuỗi 6 chữ số đầu tiên thắng; kiểm tra tính duy nhất bên dưới bắt trường hợp hiếm và
    # mơ hồ khi cùng một email có hai mã khác nhau.
    return candidates[0]


def _parse_email(raw: bytes) -> tuple[str, str, str]:
    """(from, subject, text_body) từ một thư IMAP. Giải mã multipart đúng cách bằng bộ parse
    email của stdlib; quay về byte thô cho phần thân khi không có phần text/plain (một số
    email Facebook chỉ có HTML)."""
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    from_header = msg.get("From", "")
    subject = _decode_subject(msg.get("Subject"))
    # Duyệt các phần và ưu tiên text/plain; quay về text/html đã bỏ tag nếu không có phần
    # plain (email "login verification" của Facebook đôi khi chỉ có HTML).
    text_body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                text_body = part.get_content()
                break
        if not text_body:
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    text_body = re.sub(r"<[^>]+>", " ", part.get_content() or "")
                    break
    else:
        ctype = msg.get_content_type()
        content = msg.get_content() if msg else ""
        if ctype == "text/html":
            text_body = re.sub(r"<[^>]+>", " ", content)
        else:
            text_body = content
    return from_header, subject, text_body or ""


def _is_from_known_sender(from_header: str) -> bool:
    f = from_header.lower()
    return any(sender in f for sender in _KNOWN_SENDERS)


def _fetch_recent_messages(imap: imaplib.IMAP4, since_unix: float) -> list[tuple[bytes, str, str, str]]:
    """Trả về (uid, from, subject, body) cho các thư chưa đọc tới vào hoặc sau `since_unix`,
    mới nhất trước. INBOX được chọn ở chế độ đọc-ghi (để chỗ gọi đánh dấu \\Seen thư đã
    dùng) nhưng lấy bằng BODY.PEEK[], để nguyên cờ của mọi thư khác. Dùng UID
    SEARCH/FETCH để một IMAP client khác chạy cùng lúc (người dùng đọc mail ở tab khác)
    không làm đảo thứ tự những gì ta thấy giữa lúc search và fetch."""
    status, _ = imap.select("INBOX")
    if status != "OK":
        raise Email2FAUnreachableError("IMAP SELECT INBOX failed")
    since_date = time.strftime("%d-%b-%Y", time.gmtime(since_unix))
    status, data = imap.uid("SEARCH", None, f"(UNSEEN SINCE {since_date})")
    if status != "OK":
        raise Email2FAUnreachableError("IMAP UID SEARCH failed")
    if not data or not data[0]:
        return []
    out: list[tuple[bytes, str, str, str]] = []
    # UID tăng theo thứ tự thư tới, nên duyệt ngược sẽ gặp thư mới nhất trước và có thể dừng
    # ở thư đầu tiên cũ hơn since_unix - SINCE ở trên chỉ thu hẹp được tới mức ngày.
    for uid in reversed(data[0].split()):
        status, msg_data = imap.uid("FETCH", uid, "(INTERNALDATE BODY.PEEK[])")
        if status != "OK" or not msg_data:
            continue
        meta = b" ".join(part[0] if isinstance(part, tuple) else part for part in msg_data if part)
        raw = next((part[1] for part in msg_data if isinstance(part, tuple)), None)
        arrived = imaplib.Internaldate2tuple(meta)
        if arrived is None or not isinstance(raw, (bytes, bytearray)):
            continue
        if time.mktime(arrived) < since_unix:
            break
        out.append((uid, *_parse_email(bytes(raw))))
    return out


def fetch_email_2fa_code(
    account: dict[str, Any], *, since_unix: float | None = None, deadline_unix: float | None = None
) -> str | None:
    """Trả về mã 2FA 6 chữ số từ hộp thư của account.email, kiểm tra định kỳ tối đa
    _POLL_TIMEOUT_SECONDS chờ thư khớp tới. None nếu không tìm thấy mã trong khoảng đó.
    Raise Email2FAUnreachableError khi mail server trục trặc tạm thời;
    Email2FANotConfiguredError nếu tài khoản chưa cấu hình email / email_password.

    `since_unix` là thời điểm tới sớm nhất được chấp nhận - truyền thời điểm bắt đầu lần
    thử đăng nhập, để mã gửi cho một lần thử trước đó không bị lấy nhầm. Mặc định là
    _LOOKBACK_SECONDS gần nhất. `deadline_unix` mở ra cho test (ép hết giờ ngay lập tức).
    """
    email_addr = (account.get("email") or "").strip()
    email_password = account.get("email_password") or ""
    if not email_addr or not email_password:
        raise Email2FANotConfiguredError("account has no email or email_password configured")

    cfg = _config_for(email_addr)
    if since_unix is None:
        since_unix = time.time() - _LOOKBACK_SECONDS
    deadline = deadline_unix if deadline_unix is not None else time.time() + _POLL_TIMEOUT_SECONDS

    try:
        if cfg.ssl:
            imap = imaplib.IMAP4_SSL(cfg.host, cfg.port, timeout=_IMAP_TIMEOUT_SECONDS)
        else:
            imap = imaplib.IMAP4(cfg.host, cfg.port, timeout=_IMAP_TIMEOUT_SECONDS)
        with imap:
            imap.login(email_addr, email_password)
            while True:
                msgs = _fetch_recent_messages(imap, since_unix)
                # Lượt quét đầu: các thư đã có trong hộp thư lúc bắt đầu. Nếu không có gì khớp, ngủ rồi
                # quét lại tới khi hết hạn - email của một lần đăng nhập mới thường tới trong vài giây.
                for uid, from_header, subject, body in msgs:
                    if not _is_from_known_sender(from_header):
                        continue
                    code = _extract_code(body)
                    if code:
                        try:
                            imap.uid("STORE", uid, "+FLAGS", "(\\Seen)")
                        except imaplib.IMAP4.error as exc:
                            # Vẫn trả về mã - tệ nhất là một lần thử sau thấy lại nó, và since_unix của lần đó vốn
                            # đã loại nó ra theo tuổi.
                            logger.warning("email_2fa_mark_seen_failed", error=str(exc))
                        logger.info(
                            "email_2fa_code_found",
                            email_domain=email_addr.rsplit("@", 1)[-1],
                            subject=subject[:60],
                        )
                        return code
                if time.time() >= deadline:
                    return None
                time.sleep(_POLL_INTERVAL_SECONDS)
    except imaplib.IMAP4.error as exc:
        raise Email2FAUnreachableError(f"IMAP error: {exc}") from exc
    except OSError as exc:
        raise Email2FAUnreachableError(f"IMAP connection failed: {exc}") from exc
