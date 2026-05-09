"""GitHub Actions job summary helpers: capture full console output for STEP_SUMMARY."""

from __future__ import annotations

import re
from typing import TextIO

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
# GitHub rejects oversized summaries; stay under ~1 MiB with margin.
MAX_SUMMARY_CHARS = 950_000


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


class TeeStream:
    """Duplicate writes to the real stream (console) and a capture buffer."""

    __slots__ = ("_primary", "_capture")

    def __init__(self, primary: TextIO, capture: TextIO) -> None:
        self._primary = primary
        self._capture = capture

    def write(self, s: str) -> int:
        self._primary.write(s)
        self._capture.write(s)
        try:
            self._primary.flush()
        except Exception:
            pass
        return len(s)

    def flush(self) -> None:
        self._primary.flush()

    def isatty(self) -> bool:
        return self._primary.isatty()

    def fileno(self) -> int:
        return self._primary.fileno()

    def writable(self) -> bool:
        return True

    @property
    def encoding(self) -> str | None:
        return getattr(self._primary, "encoding", None)

    def reconfigure(self, **kwargs):
        if hasattr(self._primary, "reconfigure"):
            return self._primary.reconfigure(**kwargs)
        return None


def markdown_full_log(captured: str) -> str:
    """Return markdown section with fenced full log (ANSI stripped, size-capped)."""
    body = strip_ansi(captured)
    note = ""
    if len(body) > MAX_SUMMARY_CHARS:
        note = f"\n\n_[Truncated: log was {len(body)} characters; showing first {MAX_SUMMARY_CHARS}]_\n"
        body = body[:MAX_SUMMARY_CHARS]
    return f"\n### Full console output\n\n```text\n{body}{note}\n```\n"
