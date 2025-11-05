"""Asynchronous device/host time synchronisation utilities."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from .logging import get_logger

__all__ = ["TimeSyncManager", "TimeSyncSampleError"]


class TimeSyncSampleError(RuntimeError):
    """Raised when a time synchronisation measurement fails."""


@dataclass(slots=True)
class _SyncState:
    offset_s: float = 0.0
    last_sync_ts: float = 0.0
    sample_count: int = 0


class TimeSyncManager:
    """Maintain a smoothed estimate of the device-to-host time offset.

    The manager calls the provided :func:`measure_fn` to obtain multiple offset
    samples (in seconds).  The returned offset ``offset_s`` is defined such that
    ``host_time = device_time - offset_s``.
    """

    def __init__(
        self,
        device_id: str,
        measure_fn: Callable[[int, float], Awaitable[list[float]]],
        *,
        max_samples: int = 20,
        sample_timeout: float = 0.25,
        resync_interval_s: int = 120,
        drift_threshold_s: float = 0.005,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.device_id = device_id
        self._measure_fn = measure_fn
        self.max_samples = max_samples
        self.sample_timeout = sample_timeout
        self.resync_interval_s = resync_interval_s
        self.drift_threshold_s = drift_threshold_s
        self._state = _SyncState()
        self._lock = asyncio.Lock()
        self._log = logger or get_logger(f"core.time_sync.{device_id}")

    def get_offset_s(self) -> float:
        """Return the most recent offset estimate."""

        return self._state.offset_s

    async def initial_sync(self) -> float:
        """Perform the initial synchronisation and return the offset."""

        async with self._lock:
            return await self._sync_locked(reason="initial")

    async def maybe_resync(self, observed_drift_s: float | None = None) -> float:
        """Re-synchronise when the interval or drift threshold is exceeded."""

        now = time.monotonic()
        state = self._state
        if state.last_sync_ts and (now - state.last_sync_ts) < self.resync_interval_s:
            if observed_drift_s is None or abs(observed_drift_s) <= self.drift_threshold_s:
                return state.offset_s

        async with self._lock:
            if state.last_sync_ts and (time.monotonic() - state.last_sync_ts) < self.resync_interval_s:
                if observed_drift_s is None or abs(observed_drift_s) <= self.drift_threshold_s:
                    return state.offset_s
            return await self._sync_locked(reason="resync")

    async def _sync_locked(self, *, reason: str) -> float:
        try:
            samples = await self._measure_fn(self.max_samples, self.sample_timeout)
        except asyncio.CancelledError:  # pragma: no cover - defensive
            raise
        except Exception as exc:  # pragma: no cover - network dependent
            self._log.warning("time_sync device=%s reason=%s status=failed error=%s", self.device_id, reason, exc)
            return self._state.offset_s

        filtered = [float(sample) for sample in samples if isinstance(sample, (int, float))]
        if not filtered:
            self._log.warning(
                "time_sync device=%s reason=%s status=empty", self.device_id, reason
            )
            return self._state.offset_s

        offset_s = statistics.median(filtered)
        if len(filtered) >= 3:
            try:
                variance = _biweight_midvariance(filtered)
            except Exception:  # pragma: no cover - numerical edge case
                variance = None
        else:
            variance = None

        self._state = _SyncState(
            offset_s=offset_s,
            last_sync_ts=time.monotonic(),
            sample_count=len(filtered),
        )
        if variance is None:
            self._log.info(
                "time_sync device=%s offset_s=%.6f samples=%d method=median",
                self.device_id,
                offset_s,
                len(filtered),
            )
        else:
            self._log.info(
                "time_sync device=%s offset_s=%.6f samples=%d method=median variance=%.9f",
                self.device_id,
                offset_s,
                len(filtered),
                variance,
            )
        return offset_s


def _biweight_midvariance(samples: list[float]) -> float:
    """Robust variance estimate based on Tukey's biweight."""

    median = statistics.median(samples)
    mad = statistics.median(abs(x - median) for x in samples)
    if mad == 0:
        return 0.0
    c = 9.0
    weights = []
    for value in samples:
        u = (value - median) / (c * mad)
        if abs(u) >= 1:
            continue
        weights.append((value - median) ** 2 * (1 - u**2) ** 4)
    if not weights:
        return 0.0
    return sum(weights) / (len(weights) * (mad**2))
