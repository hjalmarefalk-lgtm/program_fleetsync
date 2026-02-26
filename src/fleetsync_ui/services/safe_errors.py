"""Deterministic message sanitization (GDPR gate).

Rules (LOCK):
- Digit runs of length >= 8 are replaced with "[…]".
- Newlines/CR/tabs are collapsed to single spaces.
- Quotes are stripped.
- Output is capped to 200 characters.
- If the sanitized message still looks suspicious (or was truncated), return
  "Message hidden for privacy.".

This module is pure Python and must not import Qt.
"""

from __future__ import annotations

import re
from typing import Optional

from .dto import MessageItem


_DIGIT_RUN_RE = re.compile(r"\d{8,}")
_WHITESPACE_RE = re.compile(r"[\r\n\t]+")
_MULTI_SPACE_RE = re.compile(r"\s{2,}")

_LENGTH_CAP = 200


def sanitize_text(text: Optional[str]) -> str:
    """Sanitize untrusted text for UI display.

    Deterministic: no randomness, no time.
    """

    if not text:
        return ""

    raw = str(text)

    # Normalize whitespace first.
    s = _WHITESPACE_RE.sub(" ", raw)
    s = _MULTI_SPACE_RE.sub(" ", s)
    s = s.strip()

    # Strip quotes.
    if s:
        s = s.replace("\"", "")
        s = s.replace("'", "")

    # Scrub long digit runs.
    s = _DIGIT_RUN_RE.sub("[…]", s)

    truncated = False
    if len(s) > _LENGTH_CAP:
        s = s[:_LENGTH_CAP].rstrip()
        truncated = True

    # Heuristic privacy gate: if it still looks like it could contain raw data.
    # - truncation implies lots of content
    # - many separators implies structured/raw dumps
    separators = "/\\|;,:"
    sep_count = sum(1 for ch in s if ch in separators)
    sep_ratio = (sep_count / len(s)) if s else 0.0

    looks_suspicious = truncated or (len(s) >= 80 and (sep_count >= 30 or sep_ratio >= 0.25))

    if looks_suspicious:
        return "Message hidden for privacy."

    return s


def sanitize_message_item(item: MessageItem) -> MessageItem:
    """Return a sanitized copy of a MessageItem."""

    return MessageItem(level=item.level, code=item.code, message=sanitize_text(item.message))


def safe_user_error(code: str, message: str) -> MessageItem:
    """Create a sanitized MessageItem for user-visible errors.

    Level rule (LOCK): defaults to fatal; treat codes starting with "WARN" as warning.
    """

    level = "warning" if str(code).upper().startswith("WARN") else "fatal"
    return MessageItem(level=level, code=str(code), message=sanitize_text(message))
