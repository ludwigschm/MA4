"""Logging helpers for the Neon tabletop stack.

This module centralises logging configuration so that all subsystems share a
consistent, structured output format.  The configuration is intentionally
minimal to avoid interfering with applications embedding the library – callers
can invoke :func:`configure_logging` once during start-up.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable

__all__ = ["configure_logging", "get_logger"]


class _StructuredFormatter(logging.Formatter):
    """Formatter that renders log records in a key=value style."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - wrapper
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        parts = [
            f"level={record.levelname}",
            f"logger={record.name}",
        ]
        if record.message:
            parts.append(f"msg={record.message}")
        if record.exc_info:
            parts.append(self.formatException(record.exc_info))
        return " ".join(parts)


def _resolve_level(default_level: int) -> int:
    verbose = os.getenv("LOG_VERBOSE")
    if verbose in {"1", "true", "TRUE", "yes", "on"}:
        return logging.DEBUG
    return default_level


def configure_logging(
    *,
    default_level: int = logging.INFO,
    structured: bool = True,
    extra_loggers: Iterable[str] | None = None,
) -> None:
    """Configure the root logging handler if none is installed.

    Parameters
    ----------
    default_level:
        Logging level used when ``LOG_VERBOSE`` is not enabled.
    structured:
        When :class:`True`, attach a key=value formatter for easy parsing.
    extra_loggers:
        Optional collection of logger names that should inherit the configured
        level.  This is useful for third-party modules that do not respect the
        root logger level by default.
    """

    level = _resolve_level(default_level)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
    else:
        handler = logging.StreamHandler()
        if structured:
            handler.setFormatter(_StructuredFormatter())
        else:  # pragma: no cover - not used in tests but kept for flexibility
            handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
        root.addHandler(handler)
        root.setLevel(level)

    for name in extra_loggers or ():
        logging.getLogger(name).setLevel(level)

    logging.captureWarnings(True)


def get_logger(name: str) -> logging.Logger:
    """Return a logger using the shared configuration."""

    return logging.getLogger(name)
