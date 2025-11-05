"""Track the mapping between device endpoints and stable identifiers."""

from __future__ import annotations

from typing import Dict

from .logging import get_logger

__all__ = ["DeviceRegistry"]


class DeviceRegistry:
    """Remember device identifiers observed during the session."""

    def __init__(self) -> None:
        self._endpoint_to_id: Dict[str, str] = {}
        self._warned: set[str] = set()
        self._log = get_logger("core.device_registry")

    def resolve(self, endpoint: str) -> str:
        """Return the cached identifier for *endpoint* if available."""

        return self._endpoint_to_id.get(endpoint, endpoint)

    def confirm(self, endpoint: str, device_id: str) -> None:
        """Record the identifier observed for *endpoint* and warn on mismatch."""

        if not endpoint or not device_id:
            return
        previous = self._endpoint_to_id.get(endpoint)
        if previous is None:
            self._endpoint_to_id[endpoint] = device_id
            return
        if previous == device_id:
            return
        self._endpoint_to_id[endpoint] = device_id
        if endpoint not in self._warned:
            self._log.warning(
                "device_id mismatch endpoint=%s cached=%s new=%s",
                endpoint,
                previous,
                device_id,
            )
            self._warned.add(endpoint)
