"""Device capability registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

__all__ = ["DeviceCapabilities", "CapabilityRegistry"]


@dataclass(slots=True)
class DeviceCapabilities:
    """Feature flags describing optional device behaviour."""

    frame_name_supported: bool = False


class CapabilityRegistry:
    """In-memory cache mapping device IDs to their capabilities."""

    def __init__(self) -> None:
        self._caps: Dict[str, DeviceCapabilities] = {}

    def get(self, device_id: str) -> DeviceCapabilities:
        return self._caps.setdefault(device_id, DeviceCapabilities())

    def set(self, device_id: str, caps: DeviceCapabilities) -> None:
        self._caps[device_id] = caps
