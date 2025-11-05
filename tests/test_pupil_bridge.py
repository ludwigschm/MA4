import math
import statistics
from pathlib import Path
from typing import Tuple

import pytest

from tabletop.pupil_bridge import NeonDeviceConfig, PupilBridge


class _FakeDevice:
    def __init__(self) -> None:
        self.events: list[Tuple[Tuple[object, ...], dict]] = []
        self.start_calls = 0
        self.offset_index = 0
        self.offset_samples = [0.01 + i * 0.0005 for i in range(20)]
        self._notifications: dict[str, object] = {"recording.begin": {"recording_id": "fake"}}

    def recording_start(self) -> None:
        self.start_calls += 1

    def recording_stop(self) -> None:  # pragma: no cover - defensive
        self.start_calls = max(0, self.start_calls - 1)

    def wait_for_notification(self, event: str, timeout: float = 0.5) -> object:
        return self._notifications.get(event)

    def estimate_time_offset(self) -> float:
        if self.offset_index < len(self.offset_samples):
            value = self.offset_samples[self.offset_index]
            self.offset_index += 1
            return value
        return self.offset_samples[-1]

    def send_event(self, *args, **kwargs) -> None:
        self.events.append((args, kwargs))


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch) -> Tuple[PupilBridge, _FakeDevice]:
    monkeypatch.setattr("tabletop.pupil_bridge.requests", None)
    monkeypatch.setenv("LOW_LATENCY_DISABLED", "1")
    config_path = Path("/tmp/test_neon_devices.txt")
    config_path.write_text("VP1_IP=127.0.0.1\nVP1_PORT=8080\n", encoding="utf-8")
    bridge = PupilBridge(device_mapping={}, config_path=config_path)
    device = _FakeDevice()
    cfg = NeonDeviceConfig(player="VP1", ip="127.0.0.1", port=8080)
    bridge._device_by_player["VP1"] = device  # type: ignore[attr-defined]
    bridge._on_device_connected("VP1", device, cfg, "dev-1")  # type: ignore[attr-defined]
    yield bridge, device
    bridge.close()
    config_path.unlink(missing_ok=True)


def test_event_router_single_target(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.send_event("ui.test", "VP1", {"value": 42})
    pupil_bridge._event_router.flush_all()  # type: ignore[attr-defined]
    assert device.events
    args, kwargs = device.events[0]
    assert args[0].startswith("ui.test")


def test_recording_start_idempotent(bridge):
    pupil_bridge, device = bridge
    pupil_bridge.start_recording(1, 1, "VP1")
    pupil_bridge.start_recording(1, 1, "VP1")
    assert device.start_calls == 1


def test_time_sync_manager_used_for_offsets(bridge):
    pupil_bridge, device = bridge
    offset = pupil_bridge.estimate_time_offset("VP1")
    expected = statistics.median(device.offset_samples)
    assert offset == pytest.approx(expected)
    # subsequent call should not error even if samples exhausted
    offset2 = pupil_bridge.estimate_time_offset("VP1")
    assert math.isclose(offset2, offset, rel_tol=1e-6)
