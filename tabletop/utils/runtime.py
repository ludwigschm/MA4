"""Environment helpers for latency tuning and performance diagnostics."""

from __future__ import annotations

import os

_LOW_LATENCY_ENVS = ("LOW_LATENCY_DISABLED", "LOW_LATENCY_OFF")
_PERF_ENVS = ("PERF_LOGGING", "TABLETOP_PERF")
_BATCH_WINDOW_ENV = "EVENT_BATCH_WINDOW_MS"
_BATCH_SIZE_ENV = "EVENT_BATCH_SIZE"


def is_low_latency_disabled() -> bool:
    """Return ``True`` when the low-latency pipeline is disabled."""

    for env in _LOW_LATENCY_ENVS:
        if os.environ.get(env, "").strip() == "1":
            return True
    return False


def is_perf_logging_enabled() -> bool:
    """Return whether verbose performance logging is requested."""

    if is_low_latency_disabled():
        return False
    for env in _PERF_ENVS:
        if os.environ.get(env, "").strip() == "1":
            return True
    return False


def event_batch_window_override(default_seconds: float) -> float:
    """Return the batch window in seconds.

    The value can be overridden by :envvar:`EVENT_BATCH_WINDOW_MS`.
    Invalid inputs fall back to ``default_seconds``.
    """

    raw = os.environ.get(_BATCH_WINDOW_ENV)
    if not raw:
        return default_seconds
    try:
        millis = float(raw)
    except ValueError:
        return default_seconds
    return max(0.0, millis / 1000.0)


def event_batch_size_override(default_size: int) -> int:
    """Return the batch size honouring :envvar:`EVENT_BATCH_SIZE`."""

    raw = os.environ.get(_BATCH_SIZE_ENV)
    if not raw:
        return default_size
    try:
        value = int(raw)
    except ValueError:
        return default_size
    return max(1, value)


__all__ = [
    "is_low_latency_disabled",
    "is_perf_logging_enabled",
    "event_batch_size_override",
    "event_batch_window_override",
]
