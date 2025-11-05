import asyncio

import pytest

from core.recording import RecordingController, RecordingHttpError


class _IdempotentClient:
    def __init__(self) -> None:
        self.started = False
        self.start_attempts = 0

    async def is_recording(self) -> bool:
        return self.started

    async def recording_start(self, *, label: str | None = None) -> None:
        self.start_attempts += 1
        raise RecordingHttpError(400, "Already recording!")

    async def recording_begin(self) -> None:
        return None

    async def recording_stop(self) -> None:
        self.started = False


class _TransientClient:
    def __init__(self) -> None:
        self.started = False
        self.start_attempts = 0

    async def is_recording(self) -> bool:
        return self.started

    async def recording_start(self, *, label: str | None = None) -> None:
        self.start_attempts += 1
        if self.start_attempts < 2:
            raise RecordingHttpError(503, "temporary error", transient=True)
        self.started = True

    async def recording_begin(self) -> None:
        return None

    async def recording_stop(self) -> None:
        self.started = False


class _TimeoutClient(_TransientClient):
    async def recording_begin(self) -> None:
        raise asyncio.TimeoutError


def test_ensure_started_idempotent_on_400_already_recording():
    client = _IdempotentClient()
    controller = RecordingController(client)
    asyncio.run(controller.ensure_started(label="test"))
    assert client.start_attempts == 1
    assert asyncio.run(controller.is_recording()) is True


def test_ensure_started_transient_retry_then_success(monkeypatch: pytest.MonkeyPatch):
    client = _TransientClient()
    controller = RecordingController(client)

    async def fast_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    asyncio.run(controller.ensure_started(label="retry"))
    assert client.start_attempts == 2
    assert asyncio.run(controller.is_recording()) is True


def test_begin_segment_timeout_best_effort(caplog):
    client = _TimeoutClient()
    controller = RecordingController(client)
    client.started = True
    controller._active = True  # type: ignore[attr-defined]
    caplog.set_level("WARNING")
    asyncio.run(controller.begin_segment(deadline_ms=10))
    assert any("best-effort" in record.message for record in caplog.records)
