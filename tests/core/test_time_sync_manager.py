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


def test_wait_ready_collects_enough_samples():
    batches = [
        [1.0e-6, 1.1e-6],
        [0.9e-6, 1.05e-6],
        [1.0e-6],
    ]
    call_count = 0

    async def measure(samples: int, timeout: float) -> list[float]:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0)
        if batches:
            return batches.pop(0)
        return []

    manager = TimeSyncManager("dev4", measure, max_samples=5, sample_timeout=0.05)
    ready = asyncio.run(
        manager.wait_ready(min_samples=5, max_rms_ns=200_000, timeout=1.0)
    )
    assert ready is True
    assert call_count == 3


def test_resync_observer_receives_metrics():
    measurements = [
        [0.010, 0.011, 0.009],
        [0.012, 0.013, 0.011],
        [0.012, 0.0125, 0.0115],
    ]

    async def measure(samples: int, timeout: float) -> list[float]:
        if measurements:
            return measurements.pop(0)
        return [0.012]

    class _Observer:
        def __init__(self) -> None:
            self.events: list[tuple] = []

        def on_resync_begin(self, device_id: str) -> None:
            self.events.append(("begin", device_id))

        def on_resync_complete(self, device_id: str, metrics) -> None:
            self.events.append(("complete", device_id, metrics))

        def on_resync_wait_ready(
            self, device_id: str, ready: bool, total_samples: int, rms_ns: float | None
        ) -> None:
            self.events.append(("wait_ready", device_id, ready, total_samples, rms_ns))

    observer = _Observer()
    manager = TimeSyncManager(
        "dev5",
        measure,
        max_samples=3,
        sample_timeout=0.05,
        resync_observer=observer,
        drift_threshold_s=0.0,
    )

    asyncio.run(manager.initial_sync())
    asyncio.run(manager.maybe_resync(observed_drift_s=0.01))
    assert observer.events[0][0] == "begin"
    assert observer.events[1][0] == "complete"
    metrics = observer.events[1][2]
    assert metrics.success is True
    assert metrics.sample_count == 3
    assert metrics.offset_delta_s != 0.0

    ready = asyncio.run(
        manager.wait_ready(min_samples=3, max_rms_ns=1_000_000_000, timeout=0.2)
    )
    assert ready is True
    assert observer.events[-1][0] == "wait_ready"
    assert observer.events[-1][2] is True
