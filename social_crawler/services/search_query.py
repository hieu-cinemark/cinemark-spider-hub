"""Search-query text helper shared by facebook/threads search spiders -
kept out of each spider so both stay in sync on this instead of
reimplementing it separately."""

from __future__ import annotations


def build_search_query(keyword: str) -> str:
    """Prepends "Phim" (Vietnamese for "movie") to a Vietnamese-language
    keyword before it's sent to Facebook/Threads' own search API - NOT
    before keyword_match/dedup, which must keep matching the bare keyword
    stored in D1 (see cinemark-api's posts.py contains_keyword) - callers
    keep using the original keyword for that and use this only for the
    actual outgoing search call. Several movie titles double as ordinary
    Vietnamese words/phrases with a completely unrelated everyday meaning
    (e.g. "Mẹ Mìn" - also a decades-old folk term for a child-abductor;
    "Loạn Thế" - also a generic phrase for "chaotic times") - a bare
    search for just that word/phrase pulls in mostly unrelated results.
    Prepending "Phim" nudges the platform's own search ranking toward the
    movie sense.

    English/ASCII keywords are left untouched (isascii() as the "is this
    Vietnamese" heuristic) - a search like "Iron Man" doesn't have this
    collision problem, and "Phim Iron Man" reads oddly. TikTok is not a
    caller here at all: its search is hashtag-based (see
    TikTokCommentClient/resolve_hashtag), which rejects spaces/diacritics
    outright - there's no query string to prepend a word onto."""
    stripped = keyword.strip()
    if not stripped or stripped.isascii() or stripped.lower().startswith("phim "):
        return stripped
    return f"Phim {stripped}"
