import asyncio
import statistics

import pytest

from core.time_sync import TimeSyncManager


def test_initial_sync_uses_median_and_limits_samples():
    calls: list[tuple[int, float]] = []

    async def measure(samples: int, timeout: float) -> list[float]:
        calls.append((samples, timeout))
        return [0.010, 0.015, 0.005, 0.020]

    manager = TimeSyncManager("dev1", measure, max_samples=4, sample_timeout=0.25)
    offset = asyncio.run(manager.initial_sync())
    assert offset == pytest.approx(statistics.median([0.010, 0.015, 0.005, 0.020]))
    assert calls == [(4, 0.25)]


def test_maybe_resync_interval_and_drift_threshold():
    measurements = [[0.1, 0.2, 0.15], [0.2, 0.25, 0.22]]

    async def measure(samples: int, timeout: float) -> list[float]:
        return measurements.pop(0)

    manager = TimeSyncManager(
        "dev2",
        measure,
        max_samples=3,
        sample_timeout=0.1,
        resync_interval_s=60,
        drift_threshold_s=0.005,
    )
    first = asyncio.run(manager.initial_sync())
    assert first == pytest.approx(0.15)

    # Within interval and drift threshold -> no new measurement
    second = asyncio.run(manager.maybe_resync(observed_drift_s=0.001))
    assert second == pytest.approx(first)
    # Force drift-based resync
    third = asyncio.run(manager.maybe_resync(observed_drift_s=0.02))
    assert third == pytest.approx(statistics.median([0.2, 0.25, 0.22]))


def test_sync_failure_keeps_last_offset_and_warns(caplog):
    caplog.set_level("WARNING")
    called = asyncio.Event()

    async def measure(samples: int, timeout: float) -> list[float]:
        if not called.is_set():
            called.set()
            return [0.0]
        raise RuntimeError("device offline")

    manager = TimeSyncManager("dev3", measure, max_samples=1, sample_timeout=0.1)
    asyncio.run(manager.initial_sync())
    result = asyncio.run(manager.maybe_resync(observed_drift_s=0.1))
    assert result == 0.0
    assert any("device=dev3" in record.message for record in caplog.records)
