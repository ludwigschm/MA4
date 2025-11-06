"""Environment helpers for latency tuning and performance diagnostics."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass

_LOW_LATENCY_ENVS = ("LOW_LATENCY_DISABLED", "LOW_LATENCY_OFF")
_PERF_ENVS = ("PERF_LOGGING", "TABLETOP_PERF")
_EYE_START_ENV = "EYE_START_EXTERNALLY"
_BATCH_WINDOW_ENV = "EVENT_BATCH_WINDOW_MS"
_BATCH_SIZE_ENV = "EVENT_BATCH_SIZE"

_DEFAULT_BATCH_WINDOW_MS = 5.0
_DEFAULT_BATCH_SIZE = 4

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatencyConfig:
    """Effective latency related settings derived from the environment."""

    event_batch_window_ms: float
    event_batch_size: int
    low_latency_disabled: bool

    event_batch_window_overridden: bool = False
    event_batch_size_overridden: bool = False
    low_latency_overridden: bool = False

    @property
    def event_batch_window_seconds(self) -> float:
        """Return the batching window as seconds suitable for timers."""

        return max(0.0, self.event_batch_window_ms / 1000.0)


_LATENCY_LOGGED = False


def _coerce_float_env(value: str | None, default: float, *, minimum: float = 0.0) -> tuple[float, bool]:
    if value is None or value.strip() == "":
        return default, False
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default, False
    if not math.isfinite(parsed):
        return default, False
    return max(minimum, parsed), True


def _coerce_int_env(value: str | None, default: int, *, minimum: int = 1) -> tuple[int, bool]:
    if value is None or value.strip() == "":
        return default, False
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        return default, False
    return max(minimum, parsed), True


def _coerce_bool_env(value: str | None, default: bool) -> tuple[bool, bool]:
    if value is None:
        return default, False
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True, True
    if normalized in {"0", "false", "no", "off", ""}:
        return False, True
    return default, False


def _load_latency_config() -> LatencyConfig:
    batch_window_ms, window_overridden = _coerce_float_env(
        os.environ.get(_BATCH_WINDOW_ENV),
        _DEFAULT_BATCH_WINDOW_MS,
        minimum=0.0,
    )
    batch_size, size_overridden = _coerce_int_env(
        os.environ.get(_BATCH_SIZE_ENV),
        _DEFAULT_BATCH_SIZE,
        minimum=1,
    )

    low_latency_disabled = False
    low_latency_overridden = False
    for env in _LOW_LATENCY_ENVS:
        value, overridden = _coerce_bool_env(os.environ.get(env), False)
        if overridden:
            low_latency_disabled = value
            low_latency_overridden = True
            break
    else:
        low_latency_disabled = False

    return LatencyConfig(
        event_batch_window_ms=batch_window_ms,
        event_batch_size=batch_size,
        low_latency_disabled=low_latency_disabled,
        event_batch_window_overridden=window_overridden,
        event_batch_size_overridden=size_overridden,
        low_latency_overridden=low_latency_overridden,
    )


def get_latency_config() -> LatencyConfig:
    """Return the currently active latency configuration."""

    global _LATENCY_LOGGED
    config = _load_latency_config()
    if not _LATENCY_LOGGED:
        window_display = f"{config.event_batch_window_ms:.3f}".rstrip("0").rstrip(".")
        log.info(
            "Latency config: EVENT_BATCH_WINDOW_MS=%s EVENT_BATCH_SIZE=%d LOW_LATENCY_DISABLED=%d",
            window_display or "0",
            config.event_batch_size,
            1 if config.low_latency_disabled else 0,
        )
        _LATENCY_LOGGED = True
    return config


def is_low_latency_disabled() -> bool:
    """Return ``True`` when the low-latency pipeline is disabled."""

    return get_latency_config().low_latency_disabled


def is_perf_logging_enabled() -> bool:
    """Return whether verbose performance logging is requested."""

    if is_low_latency_disabled():
        return False
    for env in _PERF_ENVS:
        if os.environ.get(env, "").strip() == "1":
            return True
    return False


def are_eye_trackers_managed_externally() -> bool:
    """Return ``True`` if an external orchestrator is responsible for starting."""

    value = os.environ.get(_EYE_START_ENV)
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return False


def event_batch_window_override(default_seconds: float) -> float:
    """Return the batch window in seconds.

    The value can be overridden by :envvar:`EVENT_BATCH_WINDOW_MS`.
    """

    config = get_latency_config()
    if config.event_batch_window_overridden:
        return config.event_batch_window_seconds
    return max(0.0, default_seconds)


def event_batch_size_override(default_size: int) -> int:
    """Return the batch size honouring :envvar:`EVENT_BATCH_SIZE`."""

    config = get_latency_config()
    if config.event_batch_size_overridden:
        return config.event_batch_size
    return max(1, default_size)


__all__ = [
    "LatencyConfig",
    "get_latency_config",
    "is_low_latency_disabled",
    "is_perf_logging_enabled",
    "are_eye_trackers_managed_externally",
    "event_batch_size_override",
    "event_batch_window_override",
]
