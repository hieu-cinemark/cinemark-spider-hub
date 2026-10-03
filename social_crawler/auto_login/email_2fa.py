"""Email-based 2FA code retrieval via IMAP.

Fallback path when an account has no totp_secret configured (see
social_crawler.spiders.facebook.auth.triggers.submit_two_factor_code's
own MissingTotpSecretError - same problem, just resolved a different
way). The Facebook/threads login form occasionally emails a 6-digit
verification code instead of asking for a TOTP code; the platform's
own 2FA setup screen lets each user pick which channel they want, so
the same account may need TOTP one day and email another.

Reads the inbox of `account.email` (using `account.email_password`) over
IMAP. Only reads mail that:
  1) arrived (server INTERNALDATE) at or after `since_unix` - the caller's
     login-attempt start, or the last `_LOOKBACK_SECONDS` by default. IMAP
     SEARCH SINCE only has day granularity, so this is checked per message,
     newest first,
  2) was sent by Facebook/threads (envelope-from / From-header match
     against `_KNOWN_SENDERS`),
  3) is unread (an IMAP \\Seen flag is the closest portable proxy for
     "we haven't consumed this yet" - messages are fetched with BODY.PEEK so
     scanning doesn't flip \\Seen, and the one whose code we return is
     flagged \\Seen explicitly so a later attempt never reuses it).

Extracts the 6-digit code with a regex rather than HTML parsing - the
exact rendered layout differs wildly between Facebook's "login
verification code" email and threads' "your code" email, but every
variant tested (and the few variants in their help-center screenshots)
include the raw digits inside the message body.

Failure modes that surface distinctly:
  - No email configured: returns None (the caller can fall through to
    mark_needs_manual_login with reason="no_email_for_2fa").
  - No email_password configured: same as above.
  - IMAP connect/auth failure: raises Email2FAUnreachableError (a
    transient IMAP server problem shouldn't disable the account).
  - No matching email in lookback window: returns None (a real login
    attempt's code may not have arrived yet - the caller decides whether
    to retry vs. fall through to needs_manual_login).
  - Email found but no 6-digit code in the body: returns None with a
    warning log; the caller can decide whether to retry.
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
    """IMAP server is unreachable / credentials wrong / auth failed -
    distinct from "no code arrived", because a transient mail-server
    outage shouldn't disable the account (same reasoning as
    MissingTotpSecretError in facebook/auth/triggers.py: don't conflate
    an automation gap with a real account problem)."""


class Email2FANotConfiguredError(RuntimeError):
    """Account has no email/email_password column set - a config gap, not
    a transient failure. The auto-login orchestrator catches this and
    mark_needs_manual_login()s the row directly, no retry."""


# Lookback window for "is there a fresh verification email yet?". 5
# minutes is plenty for a normal Facebook/threads email (usually under
# 30s, occasionally 1-2 min on the slow path) and short enough that a
# retry loop won't pick up someone else's code from the same inbox if
# the user genuinely logged in elsewhere first.
_LOOKBACK_SECONDS = 300
# How long to wait for a matching email to show up after we start
# polling. Long enough to ride out a slow Facebook mailer, short
# enough that a single re-login pass doesn't hang the scheduler tick
# for 10 minutes on a permanently-broken inbox.
_POLL_TIMEOUT_SECONDS = 90
_POLL_INTERVAL_SECONDS = 5
# Per-socket-operation timeout - _POLL_TIMEOUT_SECONDS only bounds the
# polling loop, not a connect/read against a mail host that stopped
# answering.
_IMAP_TIMEOUT_SECONDS = 30

# Substring match on From-header. Lowercase substring, not regex, since
# these are well-known stable strings (facebookmail.com, threads.net,
# instagram.com) - tightening further with regex isn't worth the
# maintenance.
_KNOWN_SENDERS = (
    "facebookmail.com",
    "threads.net",
    "instagram.com",
    "facebook.com",
)

# The 6-digit code pattern. Some messages wrap it with "Your code is
# 123 456" (spaces); some put it in its own line; some use the unicode
# "·" as a separator. The character class below matches all three.
_CODE_PATTERN = re.compile(r"(?<!\d)(\d[\d  ·\u00b7]{4,11}\d)(?!\d)")


@dataclass
class _Config:
    host: str
    port: int
    # IMAP over implicit TLS (993) is the default; STARTTLS (143) is
    # the fallback. ssl=False + starttls=True covers the rare provider
    # that doesn't do implicit TLS. None means "don't override - use
    # imaplib.IMAP4 default".
    ssl: bool


# Default IMAP host/port mapping per mail provider - operator can
# override per-account later via env / dashboard if needed. Kept here
# rather than in env.py because it's not platform config, it's mail
# provider config.
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
    """Pick the right IMAP host/port for this email's domain. Falls back
    to a generic `_Config("imap." + domain, 993, True)` heuristic if
    the domain isn't in the well-known list - works for most
    self-hosted / corporate mail setups that follow the
    imap.<domain>:993 convention, with a clear log line for the few
    that don't so the operator can add a `_PROVIDER_DEFAULTS` row."""
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
    """Strip whitespace and unicode middle-dot separators from a matched
    code blob, returning just the digits. "123 456" -> "123456";
    "123·456" -> "123456". Some Facebook variants embed the code with a
    single space; others use a thin-space or middle-dot."""
    return re.sub(r"[\s \u00b7·]+", "", s)


def _extract_code(body: str) -> str | None:
    """First 6-digit run in the body that survives normalization. If
    multiple run candidates exist (e.g. a phone number + a code), the
    shortest one >= 6 digits wins - real verification codes are always
    exactly 6, while phone numbers / customer IDs are typically longer."""
    candidates = [_normalize_digits(m.group(1)) for m in _CODE_PATTERN.finditer(body)]
    candidates = [c for c in candidates if c.isdigit() and len(c) == 6]
    if not candidates:
        return None
    # First 6-digit one wins; uniqueness check below catches the rare
    # ambiguous case where two distinct codes appear in the same email.
    return candidates[0]


def _parse_email(raw: bytes) -> tuple[str, str, str]:
    """(from, subject, text_body) from one IMAP message. Decodes
    multipart correctly via the stdlib email parser; falls back to the
    raw bytes for the body when no text/plain part is present (some
    Facebook emails are HTML-only)."""
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    from_header = msg.get("From", "")
    subject = _decode_subject(msg.get("Subject"))
    # Walk parts and prefer text/plain; fall back to text/html stripped
    # of tags if no plain part exists (Facebook's "login verification"
    # email is sometimes HTML-only).
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
    """Returns (uid, from, subject, body) for unread messages that arrived
    at or after `since_unix`, newest first. INBOX is selected read-write
    (so the caller can flag the consumed message \\Seen) but fetched with
    BODY.PEEK[], which leaves every other message's flags alone. UID
    SEARCH/FETCH so a concurrent IMAP client (the user reading their mail
    in another tab) doesn't reorder our view between search and fetch."""
    status, _ = imap.select("INBOX")
    if status != "OK":
        raise Email2FAUnreachableError("IMAP SELECT INBOX failed")
    since_date = time.strftime("%d-%b-%Y", time.gmtime(since_unix))
    status, data = imap.uid("SEARCH", None, f'(UNSEEN SINCE {since_date})')
    if status != "OK":
        raise Email2FAUnreachableError("IMAP UID SEARCH failed")
    if not data or not data[0]:
        return []
    out: list[tuple[bytes, str, str, str]] = []
    # UIDs grow with arrival order, so walking them in reverse visits the
    # newest message first and can stop at the first one older than
    # since_unix - SINCE above only narrows it down to the day.
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
    """Returns the 6-digit 2FA code from account.email's inbox, polling
    for up to _POLL_TIMEOUT_SECONDS for a matching message to arrive.
    None if no code found in the window. Raises Email2FAUnreachableError
    for transient mail-server problems; Email2FANotConfiguredError if
    the account has no email / email_password configured.

    `since_unix` is the earliest arrival time accepted - pass when the
    login attempt started, so a code mailed for an earlier attempt can't
    be picked up. Defaults to the last _LOOKBACK_SECONDS.
    `deadline_unix` is exposed for tests (force timeout immediately).
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
                # First scan: messages already in the inbox when we start.
                # If nothing matches, sleep and re-scan until the
                # deadline - a fresh login attempt's email usually
                # arrives within a few seconds.
                for uid, from_header, subject, body in msgs:
                    if not _is_from_known_sender(from_header):
                        continue
                    code = _extract_code(body)
                    if code:
                        try:
                            imap.uid("STORE", uid, "+FLAGS", "(\\Seen)")
                        except imaplib.IMAP4.error as exc:
                            # Still return the code - the worst case is a
                            # later attempt seeing it again, and that one's
                            # since_unix already rules it out by age.
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
