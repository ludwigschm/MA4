"""Integration helpers for communicating with Pupil Labs devices."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, Literal, Optional, Tuple, Union

from core.capabilities import CapabilityRegistry, DeviceCapabilities
from core.device_registry import DeviceRegistry
from core.event_router import EventRouter, UIEvent
from core.recording import DeviceClient, RecordingController, RecordingHttpError
from core.time_sync import (
    TimeSyncManager,
    TimeSyncResyncMetrics,
    TimeSyncResyncObserver,
)

try:  # pragma: no cover - optional dependency
    from pupil_labs.realtime_api.simple import discover_devices
except Exception:  # pragma: no cover - optional dependency
    discover_devices = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from pupil_labs.realtime_api.simple.device import Device
except Exception:  # pragma: no cover - optional dependency
    try:  # fallback for legacy API surface
        from pupil_labs.realtime_api.simple import Device  # type: ignore[assignment]
    except Exception:  # pragma: no cover - optional dependency
        Device = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    import requests
except Exception:  # pragma: no cover - optional dependency
    requests = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

from tabletop.utils.runtime import (
    are_eye_trackers_managed_externally,
    get_latency_config,
    is_perf_logging_enabled,
)

CONFIG_TEMPLATE = """# Neon Geräte-Konfiguration

VP1_ID=
VP1_IP=192.168.137.121
VP1_PORT=8080

VP2_ID=
VP2_IP=
VP2_PORT=8080
"""

CONFIG_PATH = Path(__file__).resolve().parent.parent / "neon_devices.txt"

_HEX_ID_PATTERN = re.compile(r"([0-9a-fA-F]{16,})")

_HIGH_PRIORITY_PREFIXES: tuple[str, ...] = ("button.", "action.", "sync.", "fix.")

try:
    _DRIFT_INTERVAL = float(os.environ.get("DRIFT_MARKER_INTERVAL_S", "8"))
except ValueError:
    _DRIFT_INTERVAL = 8.0
DRIFT_MARKER_INTERVAL_S = max(0.0, _DRIFT_INTERVAL)


def _ensure_config_file(path: Path) -> None:
    if path.exists():
        return
    try:
        path.write_text(CONFIG_TEMPLATE, encoding="utf-8")
    except Exception:  # pragma: no cover - defensive fallback
        log.exception("Konfigurationsdatei %s konnte nicht erstellt werden", path)


@dataclass
class NeonDeviceConfig:
    player: str
    device_id: str = ""
    ip: str = ""
    port: Optional[int] = None
    port_invalid: bool = False

    @classmethod
    def load(
        cls, role: str, *, path: Optional[Path] = None
    ) -> "NeonDeviceConfig":
        """Load the configuration for the given role from disk."""

        configs = _load_device_config(path or CONFIG_PATH)
        try:
            return configs[role]
        except KeyError as exc:  # pragma: no cover - defensive fallback
            raise ValueError(f"Unbekannte Rolle {role!r}") from exc

    @property
    def is_configured(self) -> bool:
        return bool(self.ip)

    @property
    def address(self) -> Optional[str]:
        if not self.ip:
            return None
        if self.port:
            return f"{self.ip}:{self.port}"
        return self.ip

    def summary(self) -> str:
        if not self.is_configured:
            return f"{self.player}(deaktiviert)"
        ip_display = self.ip or "-"
        if self.port_invalid:
            port_display = "?"
        else:
            port_display = str(self.port) if self.port is not None else "-"
        id_display = self.device_id or "-"
        return f"{self.player}(ip={ip_display}, port={port_display}, id={id_display})"


@dataclass
class _QueuedEvent:
    name: str
    player: str
    payload: Optional[Dict[str, Any]]
    priority: Literal["high", "normal"]
    t_ui_ns: int
    t_enqueue_ns: int


@dataclass
class _ResyncBufferState:
    """Track buffered events while a time resync is in progress."""

    active: bool = False
    start_monotonic: float = 0.0
    awaiting_ready: bool = False
    buffered_total: int = 0
    device_id: str = ""
    metrics: Optional[TimeSyncResyncMetrics] = None
    _buffer: Deque[Tuple[_QueuedEvent, float]] = field(default_factory=deque)

    def clear(self) -> None:
        self.active = False
        self.awaiting_ready = False
        self.buffered_total = 0
        self.device_id = ""
        self.metrics = None
        self._buffer.clear()


@dataclass
class RecordingStartResult:
    ok: bool
    is_recording_active: bool
    recording_id: Optional[str]
    video_guid: Optional[str]
    message: str

class _BridgeDeviceClient(DeviceClient):
    """Adapter exposing async recording operations for :class:`RecordingController`."""

    def __init__(
        self,
        bridge: "PupilBridge",
        player: str,
        device: Any,
        cfg: NeonDeviceConfig,
    ) -> None:
        self._bridge = bridge
        self._player = player
        self._device = device
        self._cfg = cfg

    async def recording_start(self, *, label: str | None = None) -> None:
        if self._bridge._active_recording.get(self._player):
            raise RecordingHttpError(400, "Already recording!")

        ready = await self._bridge._wait_for_time_sync_gate(
            self._player,
            reason="recording.start",
        )
        if not ready:
            raise RecordingHttpError(503, "time sync not ready", transient=True)

        def _start() -> None:
            if self._bridge._active_recording.get(self._player):
                raise RecordingHttpError(400, "Already recording!")
            success, _ = self._bridge._invoke_recording_start(
                self._player, self._device
            )
            if not success:
                raise RecordingHttpError(503, "recording start failed", transient=True)
            if label:
                self._bridge._apply_recording_label(
                    self._player,
                    self._device,
                    label,
                )
            self._bridge._active_recording[self._player] = True
            self._bridge._start_drift_keepalive(self._player)

        await asyncio.to_thread(_start)

    async def recording_begin(self) -> None:
        def _begin() -> None:
            info = self._bridge._wait_for_notification(
                self._device, "recording.begin", timeout=0.5
            )
            if info is None:
                raise asyncio.TimeoutError()

        await asyncio.to_thread(_begin)

    async def recording_stop(self) -> None:
        def _stop() -> None:
            stopped = False
            stop_fn = getattr(self._device, "recording_stop", None)
            if callable(stop_fn):
                try:
                    stop_fn()
                    stopped = True
                except Exception:
                    stopped = False
            if not stopped:
                self._bridge._post_device_api(
                    self._player,
                    "/api/recording",
                    {"action": "STOP"},
                    warn=False,
                )
            self._bridge._active_recording[self._player] = False
            self._bridge._stop_drift_keepalive(self._player)

        await asyncio.to_thread(_stop)

    async def is_recording(self) -> bool:
        return bool(self._bridge._active_recording.get(self._player))

def _load_device_config(path: Path) -> Dict[str, NeonDeviceConfig]:
    configs: Dict[str, NeonDeviceConfig] = {
        "VP1": NeonDeviceConfig("VP1"),
        "VP2": NeonDeviceConfig("VP2"),
    }
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return configs
    except Exception:  # pragma: no cover - defensive fallback
        log.exception("Konfiguration %s konnte nicht gelesen werden", path)
        return configs

    parsed: Dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        parsed[key.strip().upper()] = value.strip()

    vp1 = configs["VP1"]
    vp1.device_id = parsed.get("VP1_ID", vp1.device_id)
    vp1.ip = parsed.get("VP1_IP", vp1.ip).strip()
    vp1_port_raw = parsed.get("VP1_PORT", "").strip()
    if vp1_port_raw:
        try:
            vp1.port = int(vp1_port_raw)
        except ValueError:
            vp1.port_invalid = True
            vp1.port = None
    else:
        vp1.port = 8080

    vp2 = configs["VP2"]
    vp2.device_id = parsed.get("VP2_ID", vp2.device_id)
    vp2.ip = parsed.get("VP2_IP", vp2.ip).strip()
    vp2_port_raw = parsed.get("VP2_PORT", "").strip()
    if vp2_port_raw:
        try:
            vp2.port = int(vp2_port_raw)
        except ValueError:
            vp2.port_invalid = True
            vp2.port = None
    elif vp2.ip:
        vp2.port = 8080

    log.info("[Konfig geladen] %s, %s", vp1.summary(), vp2.summary())

    return configs


_ensure_config_file(CONFIG_PATH)


class PupilBridge:
    """Facade around the Pupil Labs realtime API with graceful fallbacks."""

    DEFAULT_MAPPING: Dict[str, str] = {}
    _PLAYER_INDICES: Dict[str, int] = {"VP1": 1, "VP2": 2}

    def __init__(
        self,
        device_mapping: Optional[Dict[str, str]] = None,
        connect_timeout: float = 10.0,
        *,
        config_path: Optional[Path] = None,
    ) -> None:
        config_file = config_path or CONFIG_PATH
        _ensure_config_file(config_file)
        self._device_config = _load_device_config(config_file)
        mapping_src = device_mapping if device_mapping is not None else self.DEFAULT_MAPPING
        self._device_id_to_player: Dict[str, str] = {
            str(device_id).lower(): player for device_id, player in mapping_src.items() if player
        }
        self._connect_timeout = float(connect_timeout)
        self._http_timeout = max(0.1, min(0.3, float(connect_timeout)))
        self._device_by_player: Dict[str, Any] = {"VP1": None, "VP2": None}
        self._active_recording: Dict[str, bool] = {"VP1": False, "VP2": False}
        self._recording_metadata: Dict[str, Dict[str, Any]] = {}
        self._auto_session: Optional[int] = None
        self._auto_block: Optional[int] = None
        self._auto_players: set[str] = set()
        latency_config = get_latency_config()
        self._low_latency_disabled = latency_config.low_latency_disabled
        self._perf_logging = is_perf_logging_enabled()
        self._event_queue_maxsize = 1000
        self._event_queue_drop = 0
        self._queue_sentinel: object = object()
        self._sender_stop = threading.Event()
        self._event_queue: Optional[queue.Queue[object]] = None
        self._sender_thread: Optional[threading.Thread] = None
        configured_batch_size = latency_config.event_batch_size
        configured_batch_window = latency_config.event_batch_window_seconds
        if self._low_latency_disabled:
            self._event_batch_size = 1
            self._event_batch_window = 0.0
        else:
            self._event_batch_size = configured_batch_size
            self._event_batch_window = configured_batch_window
        self._last_queue_log = 0.0
        self._last_send_log = 0.0
        self._offset_semantics_warned: set[str] = set()
        self._device_registry = DeviceRegistry()
        self._capabilities = CapabilityRegistry()
        self._time_sync: Dict[str, TimeSyncManager] = {}
        self._time_sync_tasks: Dict[str, asyncio.Future[None]] = {}
        self._recording_controllers: Dict[str, RecordingController] = {}
        self._resync_state: Dict[str, _ResyncBufferState] = {}
        self._active_router_player: Optional[str] = None
        self._player_device_id: Dict[str, str] = {}
        self._drift_marker_interval = float(DRIFT_MARKER_INTERVAL_S or 0.0)
        self._drift_keepalive_tasks: Dict[str, asyncio.Future[None]] = {}
        self._drift_keepalive_paused = False
        self._async_loop = asyncio.new_event_loop()
        self._async_thread = threading.Thread(
            target=self._async_loop.run_forever,
            name="PupilBridgeAsync",
            daemon=True,
        )
        self._async_thread.start()
        self._status_cache: Dict[str, Dict[str, Any]] = {}
        self._recording_start_inflight: Dict[str, bool] = {}
        self._timesync_warned: set[str] = set()
        self._event_router = EventRouter(
            self._on_routed_event,
            batch_interval_s=self._event_batch_window,
            max_batch=self._event_batch_size,
            multi_route=False,
        )
        self._event_executor = ThreadPoolExecutor(max_workers=2)
        self._external_start = are_eye_trackers_managed_externally()
        self._event_router.set_active_player("VP1")
        self._active_router_player = "VP1"
        if not self._low_latency_disabled:
            self._event_queue = queue.Queue(maxsize=self._event_queue_maxsize)
            self._sender_thread = threading.Thread(
                target=self._event_sender_loop,
                name="PupilBridgeSender",
                daemon=True,
            )
            self._sender_thread.start()

    # ---------------------------------------------------------------------
    # Lifecycle management
    def _log_error(self, message: str) -> None:
        log.error(message)

    def connect(self) -> bool:
        """Discover or configure devices and map them to configured players."""

        configured_players = {
            player for player, cfg in self._device_config.items() if cfg.is_configured
        }
        if configured_players:
            return self._connect_from_config(configured_players)
        return self._connect_via_discovery()

    def _validate_config(self) -> None:
        vp1 = self._device_config.get("VP1")
        if vp1 is None or not vp1.ip:
            log.error("VP1_IP ist nicht gesetzt – Verbindung wird abgebrochen.")
            raise RuntimeError("VP1_IP fehlt in neon_devices.txt")
        if vp1.port_invalid or vp1.port is None:
            log.error("VP1_PORT ist ungültig – Verbindung wird abgebrochen.")
            raise RuntimeError("VP1_PORT ungültig in neon_devices.txt")
        if vp1.port is None:
            vp1.port = 8080

        vp2 = self._device_config.get("VP2")
        if vp2 and vp2.is_configured and (vp2.port_invalid or vp2.port is None):
            log.error("VP2_PORT ist ungültig – Gerät wird übersprungen.")

    def _connect_from_config(self, configured_players: Iterable[str]) -> bool:
        if Device is None:
            raise RuntimeError(
                "Pupil Labs realtime API not available – direkte Verbindung nicht möglich."
            )

        self._validate_config()

        success = True
        for player in ("VP1", "VP2"):
            cfg = self._device_config.get(player)
            if cfg is None:
                continue
            if not cfg.is_configured:
                if player == "VP2":
                    log.info("VP2(deaktiviert) – keine Verbindung aufgebaut.")
                continue
            if cfg.port_invalid or cfg.port is None:
                message = f"Ungültiger Port für {player}: {cfg.port!r}"
                if player == "VP1":
                    raise RuntimeError(message)
                log.error(message)
                success = False
                continue
            try:
                device = self._connect_device_with_retries(player, cfg)
                actual_id = self._validate_device_identity(device, cfg)
            except Exception as exc:  # pragma: no cover - hardware dependent
                if player == "VP1":
                    raise RuntimeError(f"VP1 konnte nicht verbunden werden: {exc}") from exc
                log.error("Verbindung zu VP2 fehlgeschlagen: %s", exc)
                success = False
                continue

            self._device_by_player[player] = device
            log.info(
                "Verbunden mit %s (ip=%s, port=%s, device_id=%s)",
                player,
                cfg.ip,
                cfg.port,
                actual_id,
            )
            self._on_device_connected(player, device, cfg, actual_id)
            if not self._external_start:
                self._auto_start_recording(player, device)

        if "VP1" in configured_players and self._device_by_player.get("VP1") is None:
            raise RuntimeError("VP1 ist konfiguriert, konnte aber nicht verbunden werden.")
        vp2_cfg = self._device_config.get("VP2")
        if (
            vp2_cfg
            and vp2_cfg.is_configured
            and self._device_by_player.get("VP2") is None
        ):
            log.warning("VP2 nicht verbunden – später erneut versuchen.")
        return success and (self._device_by_player.get("VP1") is not None)

    def _connect_device_with_retries(self, player: str, cfg: NeonDeviceConfig) -> Any:
        attempt_timeouts = [2.0, 3.5, 5.0]
        attempts = len(attempt_timeouts)
        last_error: Optional[BaseException] = None
        for index, timeout in enumerate(attempt_timeouts, start=1):
            log.info(
                "Verbinde mit ip=%s, port=%s (Versuch %s/%s, Timeout %.1fs)",
                cfg.ip,
                cfg.port,
                index,
                attempts,
                timeout,
            )
            try:
                device = self._connect_device_once(cfg)
                return self._ensure_device_connection(device, timeout=timeout)
            except Exception as exc:
                last_error = exc
                log.error(
                    "Verbindungsversuch %s/%s für %s fehlgeschlagen: %s",
                    index,
                    attempts,
                    player,
                    exc,
                )
        raise last_error if last_error else RuntimeError("Unbekannter Verbindungsfehler")

    def _make_device(self, ip: str, port: int) -> Any:
        assert Device is not None  # guarded by caller
        try:
            return Device(address=ip, port=port)
        except TypeError:
            pass
        try:
            return Device(url=f"http://{ip}:{port}")
        except TypeError:
            pass
        return Device(ip, port)

    def _connect_device_once(self, cfg: NeonDeviceConfig) -> Any:
        assert Device is not None  # guarded by caller
        if not cfg.ip or cfg.port is None:
            raise RuntimeError("IP oder Port fehlen für den Verbindungsaufbau")

        ip = cfg.ip
        port = int(cfg.port)
        return self._make_device(ip, port)

    def _ensure_device_connection(self, device: Any, *, timeout: Optional[float] = None) -> Any:
        connect_fn = getattr(device, "connect", None)
        if callable(connect_fn):
            variants = []
            if timeout is not None:
                variants.extend(
                    [
                        lambda: connect_fn(timeout=timeout),
                        lambda: connect_fn(device, timeout=timeout),
                    ]
                )
            variants.extend([lambda: connect_fn(), lambda: connect_fn(device)])
            for variant in variants:
                try:
                    variant()
                    break
                except TypeError:
                    continue
        return device

    def _close_device(self, device: Any) -> None:
        for attr in ("disconnect", "close"):
            fn = getattr(device, attr, None)
            if callable(fn):
                try:
                    fn()
                except Exception:
                    log.debug("%s() schlug fehl beim Aufräumen", attr, exc_info=True)

    def _validate_device_identity(self, device: Any, cfg: NeonDeviceConfig) -> str:
        status = self._get_device_status(device)
        if status is None and requests is not None and cfg.ip and cfg.port is not None:
            url = f"http://{cfg.ip}:{cfg.port}/api/status"
            try:
                response = requests.get(url, timeout=self._connect_timeout)
                response.raise_for_status()
                status = response.json()
            except Exception as exc:
                log.error("HTTP-Statusabfrage %s fehlgeschlagen: %s", url, exc)

        if status is None:
            raise RuntimeError("/api/status konnte nicht abgerufen werden")

        device_id, module_serial = self._extract_identity_fields(status)
        expected_raw = (cfg.device_id or "").strip()
        expected_hex = self._extract_hex_device_id(expected_raw)

        if not expected_raw:
            log.warning(
                "Keine device_id für %s in der Konfiguration gesetzt – Validierung nur über Statusdaten.",
                cfg.player,
            )
        elif not expected_hex:
            log.warning(
                "Konfigurierte device_id %s enthält keine gültige Hex-ID.", expected_raw
            )

        cfg_display = expected_hex or (expected_raw or "-")

        if device_id:
            log.info("device_id=%s bestätigt (cfg=%s)", device_id, cfg_display)
            if expected_hex and device_id.lower() != expected_hex.lower():
                self._close_device(device)
                raise RuntimeError(
                    f"Gefundenes device_id={device_id} passt nicht zu Konfig {cfg_display}"
                )
            if not cfg.device_id:
                cfg.device_id = device_id
            return device_id

        if module_serial:
            log.info("Kein device_id im Status, nutze module_serial=%s (cfg=%s)", module_serial, cfg_display)
        else:
            log.warning(
                "device_id not present in status; proceeding based on IP/port only (cfg=%s)",
                cfg_display,
            )

        if expected_hex and not device_id:
            log.warning(
                "Konfigurierte device_id %s konnte nicht bestätigt werden.", expected_hex
            )

        return module_serial or ""

    def _schedule_recording_start(
        self, player: str, controller: RecordingController, label: str
    ) -> "asyncio.Future[Any]":
        async def orchestrate() -> None:
            await controller.ensure_started(label=label)
            await controller.begin_segment()

        return asyncio.run_coroutine_threadsafe(orchestrate(), self._async_loop)

    def _finalise_recording_start(
        self,
        player: str,
        metadata: Dict[str, Any],
        *,
        send_session_event: bool = False,
    ) -> Dict[str, Any]:
        payload = dict(metadata)
        payload.setdefault("player", player)
        payload.setdefault("recording_id", None)
        payload.setdefault("confirmed_monotonic", time.monotonic())
        self._active_recording[player] = True
        self._recording_metadata[player] = payload
        self._start_drift_keepalive(player)
        if send_session_event:
            try:
                self.send_event("session.recording_started", player, payload)
            except Exception:  # pragma: no cover - defensive fallback
                log.debug(
                    "Emitting session.recording_started failed for %s", player, exc_info=True
                )
        return payload

    def _auto_start_recording(self, player: str, device: Any) -> None:
        if self._active_recording.get(player):
            log.info("recording.start übersprungen (%s bereits aktiv)", player)
            return
        label = f"auto.{player.lower()}.{int(time.time())}"
        controller = self._recording_controllers.get(player)
        if controller is None:
            cfg = self._device_config.get(player)
            if cfg is None:
                return
            controller = self._build_recording_controller(player, device, cfg)
            self._recording_controllers[player] = controller

        future = self._schedule_recording_start(player, controller, label)
        try:
            future.result(timeout=max(1.0, self._connect_timeout))
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Auto recording start failed for %s: %s", player, exc)
            return

        self._finalise_recording_start(
            player,
            {
                "player": player,
                "recording_label": label,
                "event": "auto_start",
            },
        )

    def _on_device_connected(
        self,
        player: str,
        device: Any,
        cfg: NeonDeviceConfig,
        device_id: str,
    ) -> None:
        endpoint = cfg.address or ""
        if endpoint:
            self._device_registry.confirm(endpoint, device_id)
        self._player_device_id[player] = device_id
        self._event_router.register_player(player)
        if self._active_router_player is None:
            self._event_router.set_active_player(player)
            self._active_router_player = player
        self._setup_time_sync(player, device_id, device)
        self._recording_controllers[player] = self._build_recording_controller(
            player, device, cfg
        )
        self._probe_capabilities(player, cfg, device_id)

    # ------------------------------------------------------------------
    # Recording start helpers
    def _player_log_prefix(self, player: str) -> str:
        index = self._PLAYER_INDICES.get(player.upper())
        if index is not None:
            return f"[p{index}]"
        return f"[{player}]"

    def _update_recording_status_cache(self, player: str) -> Dict[str, Any]:
        snapshot: Dict[str, Any] = {
            "is_recording_active": None,
            "recording_id": None,
            "video_guid": None,
            "time_sync_state": None,
            "raw": None,
            "updated_monotonic": time.monotonic(),
        }
        device = self._device_by_player.get(player)
        if device is not None:
            raw_status = self._get_device_status(device)
            if raw_status is not None:
                extracted = self._extract_recording_status_fields(raw_status)
                snapshot.update(extracted)
                snapshot["raw"] = raw_status
        self._status_cache[player] = snapshot
        return snapshot

    def _extract_recording_status_fields(self, status: Any) -> Dict[str, Any]:
        state: Dict[str, Any] = {
            "is_recording_active": None,
            "recording_id": None,
            "video_guid": None,
            "time_sync_state": None,
        }

        def normalise_identifier(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                stripped = value.strip()
                return stripped or None
            return str(value)

        def inspect(obj: Any) -> None:
            if isinstance(obj, dict):
                if state["is_recording_active"] is None:
                    coerced = self._coerce_bool_value(obj.get("is_recording_active"))
                    if coerced is None:
                        coerced = self._coerce_bool_value(obj.get("recording_active"))
                    if coerced is not None:
                        state["is_recording_active"] = coerced
                if state["recording_id"] is None:
                    for key in ("recording_id", "recordingId", "recording_uuid", "recordingUuid"):
                        candidate = normalise_identifier(obj.get(key))
                        if candidate:
                            state["recording_id"] = candidate
                            break
                if state["video_guid"] is None:
                    for key in ("video_guid", "videoGuid", "video_id", "videoId"):
                        candidate = normalise_identifier(obj.get(key))
                        if candidate:
                            state["video_guid"] = candidate
                            break
                if state["time_sync_state"] is None:
                    time_sync = obj.get("time_sync") or obj.get("timeSync")
                    if isinstance(time_sync, dict):
                        candidate = time_sync.get("status") or time_sync.get("state")
                        if candidate:
                            state["time_sync_state"] = str(candidate)
                    elif isinstance(time_sync, str) and time_sync:
                        state["time_sync_state"] = time_sync
                    else:
                        for key in (
                            "time_sync_status",
                            "timeSyncStatus",
                            "time_sync_state",
                            "timeSyncState",
                        ):
                            candidate = obj.get(key)
                            if candidate:
                                state["time_sync_state"] = str(candidate)
                                break
                nested = obj.get("data")
                if nested is not None:
                    inspect(nested)
                nested_recording = obj.get("recording")
                if nested_recording is not None:
                    inspect(nested_recording)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    inspect(item)

        inspect(status)
        return state

    def _get_cached_recording_status(self, player: str) -> Dict[str, Any]:
        cached = self._status_cache.get(player)
        if cached is None:
            return self._update_recording_status_cache(player)
        return cached

    def _send_recording_broadcast(self, player: str, label: str) -> bool:
        payload = {"action": "start", "label": label}
        try:
            self.send_event("broadcast.start", player, payload, priority="high")
        except Exception as exc:  # pragma: no cover - defensive fallback
            log.warning(
                "%s broadcast start send failed: %s",
                self._player_log_prefix(player),
                exc,
            )
            return False
        return True

    def start_recording(
        self, player: str, label: str, timeout: float = 10.0
    ) -> RecordingStartResult:
        prefix = self._player_log_prefix(player)
        device = self._device_by_player.get(player)
        if device is None:
            message = "device not connected"
            log.error("%s %s", prefix, message)
            return RecordingStartResult(False, False, None, None, message)

        snapshot = self._get_cached_recording_status(player)
        is_active = self._coerce_bool_value(snapshot.get("is_recording_active"))
        recording_id = snapshot.get("recording_id")
        video_guid = snapshot.get("video_guid")

        if is_active:
            log.info("%s already recording; skipping", prefix)
            self._active_recording[player] = True
            self._start_drift_keepalive(player)
            return RecordingStartResult(True, True, recording_id, video_guid, "already active")

        if self._recording_start_inflight.get(player):
            log.info("%s start already in-flight; skipping", prefix)
            return RecordingStartResult(True, bool(is_active), recording_id, video_guid, "in-flight")

        self._recording_start_inflight[player] = True
        warn_key = f"{player}:empty"
        try:
            broadcast_ok = self._send_recording_broadcast(player, label)
            snapshot = self._update_recording_status_cache(player)
            is_active_now = self._coerce_bool_value(snapshot.get("is_recording_active"))
            recording_id = snapshot.get("recording_id")
            video_guid = snapshot.get("video_guid")
            log.info(
                "%s broadcast start -> ok=%s | is_recording_active(now)=%s | label=%s | rec_id=%s | video_guid=%s",
                prefix,
                broadcast_ok,
                bool(is_active_now),
                label,
                recording_id,
                video_guid,
            )
            if not broadcast_ok:
                message = "broadcast start failed"
                return RecordingStartResult(False, bool(is_active_now), recording_id, video_guid, message)

            poll_interval = min(0.25, max(0.1, timeout / 20.0 if timeout > 0 else 0.1))
            deadline = time.monotonic() + max(0.0, timeout)

            while time.monotonic() < deadline:
                snapshot = self._get_cached_recording_status(player)
                time_sync_state = snapshot.get("time_sync_state")
                if (
                    isinstance(time_sync_state, str)
                    and time_sync_state.strip().lower() == "empty"
                    and warn_key not in self._timesync_warned
                ):
                    log.warning("%s time sync status empty during start", prefix)
                    self._timesync_warned.add(warn_key)
                is_active_loop = self._coerce_bool_value(
                    snapshot.get("is_recording_active")
                )
                if is_active_loop:
                    recording_id = snapshot.get("recording_id")
                    video_guid = snapshot.get("video_guid")
                    log.info("%s is_recording_active=True", prefix)
                    metadata = {
                        "player": player,
                        "recording_label": label,
                        "recording_id": recording_id,
                    }
                    if video_guid:
                        metadata["video_guid"] = video_guid
                    self._finalise_recording_start(player, metadata)
                    log.info("%s Recording CONFIRM=True", prefix)
                    return RecordingStartResult(True, True, recording_id, video_guid, "recording active")
                time.sleep(poll_interval)
                self._update_recording_status_cache(player)

            snapshot = self._get_cached_recording_status(player)
            recording_id = snapshot.get("recording_id")
            video_guid = snapshot.get("video_guid")
            message = "timeout waiting for recording confirmation"
            log.error("%s %s", prefix, message)
            return RecordingStartResult(False, False, recording_id, video_guid, message)
        finally:
            self._recording_start_inflight.pop(player, None)
            self._timesync_warned.discard(warn_key)

    # Streaming helpers
    def start_streaming(self, player: str, *, timeout: Optional[float] = None) -> None:
        """Start the preview/stream for the given player if possible."""

        device = self._device_by_player.get(player)
        if device is None:
            raise RuntimeError(f"{player} nicht verbunden")

        if self.is_streaming(player):
            return

        if not self._invoke_stream_start(player, device):
            raise RuntimeError(f"Streamstart fehlgeschlagen für {player}")

        effective_timeout = timeout if timeout is not None else self._connect_timeout
        if not self.wait_for_streaming(player, timeout=effective_timeout):
            raise TimeoutError(f"Streamstart Timeout für {player}")

    def wait_for_streaming(
        self, player: str, *, timeout: Optional[float] = None, interval: float = 0.2
    ) -> bool:
        """Poll streaming state until active or timeout."""

        device = self._device_by_player.get(player)
        if device is None:
            return False

        if self.is_streaming(player):
            return True

        effective_timeout = timeout if timeout is not None else self._connect_timeout
        deadline = time.monotonic() + max(0.0, effective_timeout)
        poll_delay = max(0.05, interval)
        while time.monotonic() < deadline:
            if self.is_streaming(player):
                return True
            time.sleep(poll_delay)

        return self.is_streaming(player)

    def _invoke_stream_start(self, player: str, device: Any) -> bool:
        methods = (
            "start_stream",
            "start_streaming",
            "start_preview",
            "preview_start",
            "start_video",
            "ensure_streaming",
        )
        for name in methods:
            fn = getattr(device, name, None)
            if not callable(fn):
                continue
            try:
                fn()
                return True
            except TypeError:
                try:
                    fn(player)
                    return True
                except Exception:
                    continue
            except Exception:
                log.debug("Streamstart via %s für %s fehlgeschlagen", name, player, exc_info=True)
        unsupported = 0
        for path in ("/api/stream", "/api/streaming", "/api/preview"):
            response = self._post_device_api(
                player,
                path,
                {"action": "START"},
                warn=False,
            )
            status_code = getattr(response, "status_code", None)
            if status_code in {200, 202, 204}:
                return True
            if status_code == 404:
                unsupported += 1

        if unsupported == 3:
            log.info(
                "No explicit stream endpoints on %s – assuming auto-stream.",
                player,
            )
            return True

        return False

    def _status_indicates_video(self, player: str) -> bool:
        """Nutze den letzten Gerätestatus als Streaming-Fallback."""

        status = self._get_device_status(self._device_by_player.get(player))
        items = status if isinstance(status, list) else (status or {}).get("items", [])

        ok = False
        for entry in items:
            if not isinstance(entry, dict):
                continue
            if entry.get("model") != "Sensor":
                continue
            data = entry.get("data", {})
            if not isinstance(data, dict):
                continue
            sensor = str(data.get("sensor", "")).lower()
            if sensor in {"world", "eyes", "gaze"}:
                if data.get("connected") and not data.get("stream_error"):
                    ok = True
        return ok

    def _coerce_bool_value(self, value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if value == 0:
                return False
            if value == 1:
                return True
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
        return None

    def is_streaming(self, player: str) -> bool:
        """Return ``True`` if the player reports an active preview stream."""

        device = self._device_by_player.get(player)
        if device is None:
            return False

        attr_candidates = (
            "is_streaming",
            "is_streaming_active",
            "is_streaming_running",
            "preview_running",
            "preview_active",
            "is_preview_running",
            "preview_is_running",
            "streaming_active",
        )
        for attr in attr_candidates:
            candidate = getattr(device, attr, None)
            result: Any
            if callable(candidate):
                try:
                    result = candidate()
                except TypeError:
                    try:
                        result = candidate(player)
                    except Exception:
                        continue
                except Exception:
                    continue
            else:
                result = candidate
            coerced = self._coerce_bool_value(result)
            if coerced is not None:
                return coerced

        status = self._get_device_status(device)
        if isinstance(status, dict):
            keys = (
                "preview_active",
                "preview_running",
                "streaming",
                "streaming_active",
                "streaming_running",
            )
            for key in keys:
                coerced = self._coerce_bool_value(status.get(key))
                if coerced is not None:
                    return coerced
            data = status.get("data")
            if isinstance(data, dict):
                for key in keys:
                    coerced = self._coerce_bool_value(data.get(key))
                    if coerced is not None:
                        return coerced

        return self._status_indicates_video(player)

    def _setup_time_sync(self, player: str, device_id: str, device: Any) -> None:
        async def measure(samples: int, timeout: float) -> list[float]:
            estimator = getattr(device, "estimate_time_offset", None)
            if not callable(estimator):
                return []
            offsets: list[float] = []
            for _ in range(samples):
                try:
                    value = await asyncio.wait_for(
                        asyncio.to_thread(estimator), timeout
                    )
                except asyncio.TimeoutError:
                    break
                except Exception:
                    break
                else:
                    try:
                        offsets.append(float(value))
                    except Exception:
                        continue
            return offsets

        manager = TimeSyncManager(
            device_id=device_id or player,
            measure_fn=measure,
            max_samples=20,
            sample_timeout=0.25,
            resync_observer=self._build_resync_observer(player),
        )
        try:
            future = asyncio.run_coroutine_threadsafe(
                manager.initial_sync(), self._async_loop
            )
            future.result(timeout=self._connect_timeout)
        except Exception as exc:
            log.warning("Initial time sync failed for %s: %s", player, exc)
        self._time_sync[player] = manager
        self._schedule_periodic_resync(player)

    def _build_resync_observer(self, player: str) -> TimeSyncResyncObserver:
        bridge = self

        class _Observer(TimeSyncResyncObserver):
            def on_resync_begin(self, device_id: str) -> None:
                bridge._handle_resync_begin(player, device_id)

            def on_resync_complete(
                self, device_id: str, metrics: TimeSyncResyncMetrics
            ) -> None:
                bridge._handle_resync_complete(player, device_id, metrics)

            def on_resync_wait_ready(
                self,
                device_id: str,
                ready: bool,
                total_samples: int,
                rms_ns: float | None,
            ) -> None:
                bridge._handle_resync_wait_ready(
                    player, device_id, ready, total_samples, rms_ns
                )

        return _Observer()

    def _get_resync_state(self, player: str) -> _ResyncBufferState:
        state = self._resync_state.get(player)
        if state is None:
            state = _ResyncBufferState()
            self._resync_state[player] = state
        return state

    def _handle_resync_begin(self, player: str, device_id: str) -> None:
        state = self._get_resync_state(player)
        state.clear()
        state.active = True
        state.start_monotonic = time.monotonic()
        state.device_id = device_id
        marker_event_id = str(uuid.uuid4())
        payload = {
            "event_id": marker_event_id,
            "reason": "time_resync",
            "device_id": device_id,
            "origin_player": player,
        }
        log.info(
            "time_sync resync begin player=%s device=%s", player, device_id or player
        )
        try:
            self.send_event(
                "sync.resync_marker",
                player,
                payload,
                priority="high",
            )
            self.send_host_mirror(
                player,
                marker_event_id,
                time.perf_counter_ns(),
                extra={"reason": "time_resync", "device_id": device_id},
            )
        except Exception:  # pragma: no cover - defensive
            log.debug("Failed to emit resync marker for %s", player, exc_info=True)

    def _handle_resync_complete(
        self, player: str, device_id: str, metrics: TimeSyncResyncMetrics
    ) -> None:
        state = self._get_resync_state(player)
        if not state.active:
            state.active = True
            state.start_monotonic = time.monotonic()
        state.metrics = metrics
        state.awaiting_ready = metrics.success
        state.device_id = device_id or state.device_id
        if not metrics.success:
            self._finalise_resync(player, device_id, ready=False, rms_ns=None, samples=0)

    def _handle_resync_wait_ready(
        self,
        player: str,
        device_id: str,
        ready: bool,
        total_samples: int,
        rms_ns: float | None,
    ) -> None:
        self._finalise_resync(
            player,
            device_id,
            ready=ready,
            rms_ns=rms_ns,
            samples=total_samples,
        )

    def _buffer_resync_event(self, player: str, event: _QueuedEvent) -> bool:
        state = self._resync_state.get(player)
        if state is None or not state.active:
            return False
        now = time.monotonic()
        max_age = 0.1
        while state._buffer and now - state._buffer[0][1] >= max_age:
            stale, _ = state._buffer.popleft()
            self._dispatch_with_metrics(stale)
        if not state.active:
            return False
        state._buffer.append((event, now))
        state.buffered_total += 1
        return True

    def _finalise_resync(
        self,
        player: str,
        device_id: str,
        *,
        ready: bool,
        rms_ns: float | None,
        samples: int,
    ) -> None:
        state = self._get_resync_state(player)
        if not state.active and not state._buffer:
            return
        duration_total_ms = (
            (time.monotonic() - state.start_monotonic) * 1000.0
            if state.start_monotonic
            else 0.0
        )
        metrics = state.metrics
        measure_ms = metrics.duration_s * 1000.0 if metrics else 0.0
        if metrics is not None:
            offset_value = metrics.offset_s
            drift_s = metrics.offset_delta_s
            variance = metrics.variance
        else:
            manager = self._time_sync.get(player)
            offset_value = manager.get_offset_s() if manager else 0.0
            drift_s = 0.0
            variance = None
        flushed = 0
        while state._buffer:
            buffered, _ = state._buffer.popleft()
            self._dispatch_with_metrics(buffered)
            flushed += 1
        log.info(
            "time_sync resync summary player=%s device=%s ready=%s buffered=%d flushed=%d measure_ms=%.1f total_ms=%.1f offset_s=%.6f drift_s=%.6f rms_ns=%s variance=%s samples=%d",
            player,
            device_id or state.device_id or player,
            "yes" if ready else "no",
            state.buffered_total,
            flushed,
            measure_ms,
            duration_total_ms,
            offset_value,
            drift_s,
            "n/a" if rms_ns is None else f"{rms_ns:.0f}",
            "n/a" if variance is None else f"{variance:.9f}",
            samples,
        )
        state.clear()

    async def _wait_for_time_sync_gate(
        self,
        player: str,
        *,
        reason: str,
        min_samples: int = 60,
        max_rms_ns: int = 2_000_000,
        timeout: float = 5.0,
        attempts: int = 2,
        retry_delay: float = 2.0,
    ) -> bool:
        manager = self._time_sync.get(player)
        if manager is None:
            log.warning(
                "time_sync gate failed (no manager) player=%s reason=%s",
                player,
                reason,
            )
            return False

        device = manager.device_id
        log.info(
            "time_sync gate enter player=%s device=%s reason=%s attempts=%d",
            player,
            device,
            reason,
            attempts,
        )

        attempts = max(1, int(attempts))
        retry_delay = max(0.0, float(retry_delay))
        for attempt in range(1, attempts + 1):
            ready = await manager.wait_ready(
                min_samples=min_samples,
                max_rms_ns=max_rms_ns,
                timeout=timeout,
            )
            if ready:
                log.info(
                    "time_sync gate ready player=%s device=%s reason=%s attempt=%d",
                    player,
                    device,
                    reason,
                    attempt,
                )
                return True
            if attempt < attempts:
                log.info(
                    "time_sync gate retry player=%s device=%s reason=%s attempt=%d",
                    player,
                    device,
                    reason,
                    attempt,
                )
                await asyncio.sleep(retry_delay)

        log.warning(
            "time_sync gate failed player=%s device=%s reason=%s attempts=%d",
            player,
            device,
            reason,
            attempts,
        )
        return False

    def wait_for_time_sync_gate(
        self,
        player: str,
        *,
        reason: str,
        min_samples: int = 60,
        max_rms_ns: int = 2_000_000,
        timeout: float = 5.0,
        attempts: int = 2,
        retry_delay: float = 2.0,
    ) -> bool:
        loop = self._async_loop
        if loop.is_closed():
            log.warning(
                "time_sync gate skipped (loop closed) player=%s reason=%s",
                player,
                reason,
            )
            return False

        attempts = max(1, int(attempts))
        timeout = float(timeout)
        retry_delay = max(0.0, float(retry_delay))
        total_timeout = attempts * timeout + max(0, attempts - 1) * retry_delay + 1.0
        future = asyncio.run_coroutine_threadsafe(
            self._wait_for_time_sync_gate(
                player,
                reason=reason,
                min_samples=min_samples,
                max_rms_ns=max_rms_ns,
                timeout=timeout,
                attempts=attempts,
                retry_delay=retry_delay,
            ),
            loop,
        )
        try:
            return future.result(timeout=total_timeout)
        except Exception as exc:
            log.warning(
                "time_sync gate error player=%s reason=%s error=%s",
                player,
                reason,
                exc,
            )
            return False

    def _schedule_periodic_resync(self, player: str) -> None:
        existing = self._time_sync_tasks.get(player)
        if existing is not None:
            existing.cancel()

        manager = self._time_sync.get(player)
        if manager is None:
            return

        async def periodic() -> None:
            while True:
                await asyncio.sleep(manager.resync_interval_s)
                try:
                    await manager.maybe_resync()
                except Exception as exc:  # pragma: no cover - defensive
                    log.debug("time_sync resync failed for %s: %s", player, exc)

        task = asyncio.run_coroutine_threadsafe(periodic(), self._async_loop)
        self._time_sync_tasks[player] = task

    def _build_recording_controller(
        self, player: str, device: Any, cfg: NeonDeviceConfig
    ) -> RecordingController:
        client = _BridgeDeviceClient(self, player, device, cfg)
        logger = logging.getLogger(f"{__name__}.recording.{player.lower()}")
        return RecordingController(client, logger)

    def _probe_capabilities(
        self, player: str, cfg: NeonDeviceConfig, device_id: str
    ) -> None:
        identifier = device_id or player
        supported = False
        if requests is not None and cfg.ip and cfg.port is not None:
            url = f"http://{cfg.ip}:{cfg.port}/api/frame_name"
            try:
                response = requests.options(url, timeout=self._http_timeout)
            except Exception:
                supported = False
            else:
                supported = response.status_code in {200, 204}
        caps = DeviceCapabilities(frame_name_supported=supported)
        self._capabilities.set(identifier, caps)
        if not supported:
            log.info("frame_name skipped (unsupported) device=%s", player)

    def _get_device_status(self, device: Any) -> Optional[Any]:
        for attr in ("api_status", "status", "get_status"):
            status_fn = getattr(device, attr, None)
            if not callable(status_fn):
                continue
            try:
                result = status_fn()
            except Exception:
                log.debug("Statusabfrage über %s fehlgeschlagen", attr, exc_info=True)
                continue
            if result is None:
                continue
            if isinstance(result, dict):
                return result
            if isinstance(result, (list, tuple)):
                return list(result)
            if isinstance(result, str):
                try:
                    parsed = json.loads(result)
                except json.JSONDecodeError:
                    continue
                else:
                    if isinstance(parsed, (dict, list)):
                        return parsed
            to_dict = getattr(result, "to_dict", None)
            if callable(to_dict):
                try:
                    converted = to_dict()
                except Exception:
                    continue
                if isinstance(converted, (dict, list)):
                    return converted
            as_dict = getattr(result, "_asdict", None)
            if callable(as_dict):
                try:
                    converted = as_dict()
                except Exception:
                    continue
                if isinstance(converted, (dict, list)):
                    return converted
        return None

    def _extract_device_id_from_status(self, status: Any) -> Optional[str]:
        device_id, _ = self._extract_identity_fields(status)
        return device_id

    def _extract_identity_fields(self, status: Any) -> tuple[Optional[str], Optional[str]]:
        device_id: Optional[str] = None
        module_serial: Optional[str] = None

        def set_device(candidate: Any) -> None:
            nonlocal device_id
            if device_id:
                return
            coerced = self._coerce_identity_value(candidate)
            if coerced:
                device_id = coerced

        def set_module(candidate: Any) -> None:
            nonlocal module_serial
            if module_serial:
                return
            coerced = self._coerce_identity_value(candidate)
            if coerced:
                module_serial = coerced

        try:
            if isinstance(status, dict):
                set_device(status.get("device_id"))
                data = status.get("data")
                if isinstance(data, dict):
                    set_device(data.get("device_id"))
                    set_module(data.get("module_serial"))
                set_module(status.get("module_serial"))
            elif isinstance(status, (list, tuple)):
                records = [record for record in status if isinstance(record, dict)]
                for record in records:
                    if record.get("model") == "Phone":
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_device(data.get("device_id"))
                        if device_id:
                            break
                if not device_id:
                    for record in records:
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_device(data.get("device_id"))
                        if device_id:
                            break

                for record in records:
                    if record.get("model") == "Hardware":
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_module(data.get("module_serial"))
                        if module_serial:
                            break
                if not module_serial:
                    for record in records:
                        data = record.get("data")
                        if isinstance(data, dict):
                            set_module(data.get("module_serial"))
                        if module_serial:
                            break
        except Exception:
            log.debug("Konnte Statusinformationen nicht vollständig auswerten", exc_info=True)

        return device_id, module_serial

    def _coerce_identity_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None
        if isinstance(value, str):
            candidate = value.strip()
        else:
            candidate = str(value).strip()
        return candidate or None

    def _extract_hex_device_id(self, value: str) -> Optional[str]:
        if not value:
            return None
        match = _HEX_ID_PATTERN.search(value)
        if not match:
            return None
        return match.group(1).lower()

    def _perform_discovery(self, *, log_errors: bool = True) -> list[Any]:
        if discover_devices is None:
            return []
        try:
            try:
                devices = discover_devices(timeout_seconds=self._connect_timeout)
            except TypeError:
                try:
                    devices = discover_devices(timeout=self._connect_timeout)
                except TypeError:
                    devices = discover_devices(self._connect_timeout)
        except Exception as exc:  # pragma: no cover - network/hardware dependent
            if log_errors:
                log.exception("Failed to discover Pupil devices: %s", exc)
            else:
                log.debug("Discovery fehlgeschlagen: %s", exc, exc_info=True)
            return []
        return list(devices) if devices else []

    def _match_discovered_device(
        self, device_id: str, devices: Optional[Iterable[Any]]
    ) -> Optional[Dict[str, Any]]:
        if not device_id or not devices:
            return None
        wanted = device_id.lower()
        for device in devices:
            info = self._inspect_discovered_device(device)
            candidate = info.get("device_id")
            if candidate and candidate.lower() == wanted:
                return info
        return None

    def _inspect_discovered_device(self, device: Any) -> Dict[str, Any]:
        info: Dict[str, Any] = {"device": device}
        direct_id = self._extract_device_id_attribute(device)
        status: Optional[Dict[str, Any]] = None
        if direct_id:
            info["device_id"] = direct_id
            status = self._get_device_status(device)
        else:
            status = self._get_device_status(device)
            if status is not None:
                status_id = self._extract_device_id_from_status(status)
                if status_id:
                    info["device_id"] = status_id
        if status is None:
            status = {}
        ip, port = self._extract_ip_port(device, status)
        if ip:
            info["ip"] = ip
        if port is not None:
            info["port"] = port
        return info

    def _extract_device_id_attribute(self, device: Any) -> Optional[str]:
        for attr in ("device_id", "id"):
            value = getattr(device, attr, None)
            if value is None:
                continue
            candidate = str(value).strip()
            if candidate:
                return candidate
        return None

    def _extract_ip_port(
        self, device: Any, status: Optional[Any] = None
    ) -> tuple[Optional[str], Optional[int]]:
        for attr in ("address", "ip", "ip_address", "host"):
            value = getattr(device, attr, None)
            ip, port = self._parse_network_value(value)
            if ip:
                return ip, port
        if status:
            dict_sources: list[Dict[str, Any]] = []
            if isinstance(status, dict):
                dict_sources.append(status)
            elif isinstance(status, (list, tuple)):
                for record in status:
                    if isinstance(record, dict):
                        dict_sources.append(record)
                        data = record.get("data")
                        if isinstance(data, dict):
                            dict_sources.append(data)
            for source in dict_sources:
                for path in (
                    ("address",),
                    ("ip",),
                    ("network", "ip"),
                    ("network", "address"),
                    ("system", "ip"),
                    ("system", "address"),
                ):
                    value = self._dig(source, path)
                    ip, port = self._parse_network_value(value)
                    if ip:
                        return ip, port
        return None, None

    def _parse_network_value(self, value: Any) -> tuple[Optional[str], Optional[int]]:
        if value is None:
            return None, None
        if isinstance(value, (list, tuple)):
            if not value:
                return None, None
            host = value[0]
            port = value[1] if len(value) > 1 else None
            return self._coerce_host(host), self._coerce_port(port)
        if isinstance(value, dict):
            host = value.get("host") or value.get("ip") or value.get("address")
            port = value.get("port")
            return self._coerce_host(host), self._coerce_port(port)
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None, None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None, None
            if "//" in text:
                text = text.split("//", 1)[-1]
            if ":" in text:
                host_part, _, port_part = text.rpartition(":")
                host = host_part.strip() or None
                port = self._coerce_port(port_part)
                return host, port
            return text, None
        return self._coerce_host(value), None

    def _coerce_host(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except Exception:
                return None
        host = str(value).strip()
        return host or None

    def _coerce_port(self, value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _dig(self, data: Dict[str, Any], path: Iterable[str]) -> Any:
        current: Any = data
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    def _connect_via_discovery(self) -> bool:
        if discover_devices is None:
            log.warning(
                "Pupil Labs realtime API not available. Running without device integration."
            )
            return False

        found_devices = self._perform_discovery(log_errors=True)
        if not found_devices:
            log.warning("No Pupil devices discovered within %.1fs", self._connect_timeout)
            return False

        for device in found_devices:
            info = self._inspect_discovered_device(device)
            device_id = info.get("device_id")
            if not device_id:
                log.debug("Skipping device ohne device_id: %r", device)
                continue
            player = self._device_id_to_player.get(device_id.lower())
            if not player:
                log.info("Ignoring unmapped device with device_id %s", device_id)
                continue
            cfg = NeonDeviceConfig(player=player, device_id=device_id)
            cfg.ip = (info.get("ip") or "").strip()
            port_info = info.get("port")
            if isinstance(port_info, str):
                try:
                    cfg.port = int(port_info)
                except ValueError:
                    cfg.port = None
            else:
                cfg.port = port_info
            if cfg.ip and cfg.port is None:
                cfg.port = 8080
            try:
                prepared = self._ensure_device_connection(device)
                actual_id = self._validate_device_identity(prepared, cfg)
                self._device_by_player[player] = prepared
                log.info(
                    "Verbunden mit %s (ip=%s, port=%s, device_id=%s)",
                    player,
                    cfg.ip or "-",
                    cfg.port,
                    actual_id,
                )
                self._on_device_connected(player, prepared, cfg, actual_id)
                if not self._external_start:
                    self._auto_start_recording(player, prepared)
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning("Gerät %s konnte nicht verbunden werden: %s", device_id, exc)

        missing_players = [player for player, device in self._device_by_player.items() if device is None]
        if missing_players:
            log.warning(
                "No device found for players: %s", ", ".join(sorted(missing_players))
            )
        return self._device_by_player.get("VP1") is not None

    def close(self) -> None:
        """Close all connected devices if necessary."""

        self._event_router.flush_all()
        try:
            self._event_executor.shutdown(wait=False)
        except Exception:  # pragma: no cover - defensive
            log.debug("Failed to shutdown event executor", exc_info=True)
        for future in list(self._time_sync_tasks.values()):
            future.cancel()
            try:
                future.result()
            except Exception:
                pass
        self._time_sync_tasks.clear()
        self._time_sync.clear()
        self._stop_all_drift_keepalive()
        if self._async_loop.is_running():
            async def _cancel_all() -> None:
                for task in asyncio.all_tasks():
                    if task is not asyncio.current_task():
                        task.cancel()

            stopper = asyncio.run_coroutine_threadsafe(_cancel_all(), self._async_loop)
            try:
                stopper.result()
            except Exception:
                pass
        if self._async_loop.is_running():
            self._async_loop.call_soon_threadsafe(self._async_loop.stop)
        if self._async_thread.is_alive():
            self._async_thread.join(timeout=1.0)
        try:
            self._async_loop.close()
        except RuntimeError:
            pass
        if self._event_queue is not None:
            self._sender_stop.set()
            try:
                self._event_queue.put_nowait(self._queue_sentinel)
            except queue.Full:
                self._event_queue.put(self._queue_sentinel)
            if self._sender_thread is not None:
                self._sender_thread.join(timeout=1.0)
            self._event_queue = None
            self._sender_thread = None
        for player, device in list(self._device_by_player.items()):
            if device is None:
                continue
            try:
                close_fn = getattr(device, "close", None)
                if callable(close_fn):
                    close_fn()
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.exception("Failed to close device for %s: %s", player, exc)
            finally:
                self._device_by_player[player] = None
        for player in list(self._active_recording):
            self._active_recording[player] = False
            self._stop_drift_keepalive(player)
        self._recording_metadata.clear()

    # ------------------------------------------------------------------
    # Recording helpers
    def ensure_recordings(
        self,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
        players: Optional[Iterable[str]] = None,
    ) -> set[str]:
        if session is not None:
            self._auto_session = session
        if block is not None:
            self._auto_block = block
        if players is not None:
            self._auto_players = {p for p in players if p}

        if self._auto_players:
            target_players = self._auto_players
        else:
            target_players = {p for p, dev in self._device_by_player.items() if dev is not None}

        if self._auto_session is None or self._auto_block is None:
            return set()

        started: set[str] = set()
        for player in target_players:
            self.start_recording(self._auto_session, self._auto_block, player)
            if self._active_recording.get(player):
                started.add(player)
        return started

    def start_recording(self, session: int, block: int, player: str) -> None:
        """Start a recording for the given player using the agreed label schema."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.start übersprungen (%s nicht verbunden)", player)
            return

        if self._active_recording.get(player):
            log.debug("Recording already active for %s", player)
            return

        vp_index = self._PLAYER_INDICES.get(player, 0)
        recording_label = f"{session}.{block}.{vp_index}"

        controller = self._recording_controllers.get(player)
        if controller is None:
            cfg = self._device_config.get(player)
            if cfg is None:
                log.info("recording.start übersprungen (%s ohne Konfig)", player)
                return
            controller = self._build_recording_controller(player, device, cfg)
            self._recording_controllers[player] = controller

        log.info(
            "recording start requested player=%s label=%s session=%s block=%s",
            player,
            recording_label,
            session,
            block,
        )

        future = self._schedule_recording_start(player, controller, recording_label)
        try:
            future.result(timeout=max(1.0, self._connect_timeout))
        except RecordingHttpError as exc:
            log.warning(
                "recording start failed player=%s status=%s msg=%s",
                player,
                exc.status,
                exc.message,
            )
            return
        except asyncio.TimeoutError:
            log.warning("recording start timeout player=%s", player)
            return
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("recording start error player=%s error=%s", player, exc)
            return

        metadata = {
            "session": session,
            "block": block,
            "player": player,
            "recording_label": recording_label,
            "recording_id": None,
        }
        self._finalise_recording_start(
            player,
            metadata,
            send_session_event=True,
        )

    def start_external_recording(
        self, player: str, label: Optional[str] = None
    ) -> bool:
        """Start a recording outside the automatic session/block flow."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.start übersprungen (%s nicht verbunden)", player)
            return False

        if self._active_recording.get(player):
            log.debug("Recording already active for %s", player)
            return True

        controller = self._recording_controllers.get(player)
        if controller is None:
            cfg = self._device_config.get(player)
            if cfg is None:
                log.info("recording.start übersprungen (%s ohne Konfig)", player)
                return False
            controller = self._build_recording_controller(player, device, cfg)
            self._recording_controllers[player] = controller

        actual_label = label or f"external.{player.lower()}.{int(time.time())}"
        future = self._schedule_recording_start(player, controller, actual_label)
        try:
            future.result(timeout=max(1.0, self._connect_timeout))
        except RecordingHttpError as exc:
            log.warning(
                "external recording start failed player=%s status=%s msg=%s",
                player,
                exc.status,
                exc.message,
            )
            return False
        except asyncio.TimeoutError:
            log.warning("external recording start timeout player=%s", player)
            return False
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("external recording start error player=%s error=%s", player, exc)
            return False

        self._finalise_recording_start(
            player,
            {
                "player": player,
                "recording_label": actual_label,
                "event": "external_start",
            },
        )
        return True

    def is_recording_active(self, player: str) -> bool:
        """Return ``True`` if the local state marks a recording as active."""

        return bool(self._active_recording.get(player))

    def recording_was_confirmed(
        self, player: str, *, within_s: float = 3.0
    ) -> bool:
        """Return ``True`` if a recent confirmation marker exists."""

        metadata = self._recording_metadata.get(player)
        if not isinstance(metadata, dict):
            return False
        timestamp = metadata.get("confirmed_monotonic")
        if timestamp is None:
            return False
        try:
            elapsed = time.monotonic() - float(timestamp)
        except (TypeError, ValueError):
            return False
        return elapsed <= max(0.0, within_s)

    def ensure_recording(
        self,
        player: str,
        *,
        label: Optional[str] = None,
        tries: int = 1,
        wait_s: float = 0.5,
    ) -> None:
        """Ensure a recording is active, retrying REST fallbacks if required."""

        attempts = max(1, int(tries))
        delay = max(0.0, float(wait_s))
        last_error: Optional[BaseException] = None

        for attempt in range(attempts):
            if self.is_recording_active(player) or self.is_recording(player):
                return

            if self.start_external_recording(player, label=label):
                return

            last_error = RuntimeError(
                f"Recording start failed for {player} (attempt {attempt + 1}/{attempts})"
            )
            if attempt < attempts - 1 and delay:
                time.sleep(delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Recording start failed for {player}")

    def is_recording(self, player: str) -> bool:
        """Return ``True`` when the player has an active recording."""

        if self._active_recording.get(player):
            return True

        controller = self._recording_controllers.get(player)
        if controller is None:
            return False

        try:
            future = asyncio.run_coroutine_threadsafe(
                controller.is_recording(), self._async_loop
            )
            return bool(future.result(timeout=max(0.5, self._http_timeout)))
        except Exception:  # pragma: no cover - defensive fallback
            return False

    def _send_recording_start(
        self,
        player: str,
        device: Any,
        label: str,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
    ) -> Optional[Any]:
        if not self.wait_for_time_sync_gate(player, reason="recording.start"):
            log.warning(
                "recording start skipped (time sync) player=%s", player
            )
            return None

        success, _ = self._invoke_recording_start(player, device)
        if not success:
            return None

        self._apply_recording_label(player, device, label, session=session, block=block)

        begin_info = self._wait_for_notification(device, "recording.begin")
        if begin_info is None:
            return None
        return begin_info

    def _invoke_recording_start(
        self,
        player: str,
        device: Any,
        *,
        allow_busy_recovery: bool = True,
    ) -> tuple[bool, Optional[Any]]:
        start_methods = ("recording_start", "start_recording")
        for method_name in start_methods:
            start_fn = getattr(device, method_name, None)
            if not callable(start_fn):
                continue
            try:
                return True, start_fn()
            except TypeError:
                log.debug(
                    "recording start via %s requires unsupported arguments (%s)",
                    method_name,
                    player,
                    exc_info=True,
                )
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.exception(
                    "Failed to start recording for %s via %s: %s",
                    player,
                    method_name,
                    exc,
                )
                return False, None

        rest_status, rest_payload = self._start_recording_via_rest(player)
        if rest_status == "busy" and allow_busy_recovery:
            if self._handle_busy_state(player, device):
                return self._invoke_recording_start(player, device, allow_busy_recovery=False)
            return False, None
        if rest_status is True:
            return True, rest_payload
        log.error("No recording start method succeeded for %s", player)
        return False, None

    def _start_recording_via_rest(self, player: str) -> tuple[Optional[Union[str, bool]], Optional[Any]]:
        if requests is None:
            log.debug("requests not available – cannot start recording via REST (%s)", player)
            return False, None
        cfg = self._device_config.get(player)
        if cfg is None or not cfg.ip or cfg.port is None:
            log.debug("REST recording start skipped (%s: no IP/port)", player)
            return False, None

        url = f"http://{cfg.ip}:{cfg.port}/api/recording"
        try:
            response = requests.post(
                url,
                json={"action": "START"},
                timeout=self._http_timeout,
            )
        except Exception as exc:  # pragma: no cover - network dependent
            log.error("REST recording start failed for %s: %s", player, exc)
            return False, None

        if response.status_code == 200:
            try:
                return True, response.json()
            except ValueError:
                return True, None

        message: Optional[str] = None
        try:
            data = response.json()
            if isinstance(data, dict):
                message = str(data.get("message") or data.get("error") or "")
        except ValueError:
            message = response.text

        if (
            response.status_code == 400
            and message
            and "previous recording not completed" in message.lower()
        ):
            log.warning("Recording start busy for %s: %s", player, message)
            return "busy", None

        log.error(
            "REST recording start for %s failed (%s): %s",
            player,
            response.status_code,
            message or response.text,
        )
        return False, None

    def _handle_busy_state(self, player: str, device: Any) -> bool:
        log.info("Attempting to clear busy recording state for %s", player)
        stopped = False
        stop_fn = getattr(device, "recording_stop_and_save", None)
        if callable(stop_fn):
            try:
                stop_fn()
                stopped = True
            except Exception as exc:  # pragma: no cover - hardware dependent
                log.warning(
                    "recording_stop_and_save failed for %s: %s",
                    player,
                    exc,
                )

        if not stopped:
            response = self._post_device_api(
                player,
                "/api/recording",
                {"action": "STOP"},
                warn=False,
            )
            if response is not None and response.status_code == 200:
                stopped = True
            elif response is not None:
                log.warning(
                    "REST recording STOP for %s failed (%s)",
                    player,
                    response.status_code,
                )

        if not stopped:
            return False

        end_info = self._wait_for_notification(device, "recording.end")
        if end_info is None:
            log.warning("Timeout while waiting for recording.end (%s)", player)
        return True

    def _post_device_api(
        self,
        player: str,
        path: str,
        payload: Dict[str, Any],
        *,
        timeout: Optional[float] = None,
        warn: bool = True,
    ) -> Optional[Any]:
        if requests is None:
            if warn:
                log.warning(
                    "requests not available – cannot contact %s for %s",
                    path,
                    player,
                )
            return None

        cfg = self._device_config.get(player)
        if cfg is None or not cfg.ip or cfg.port is None:
            if warn:
                log.warning(
                    "REST endpoint %s not configured for %s",
                    path,
                    player,
                )
            return None

        url = f"http://{cfg.ip}:{cfg.port}{path}"
        effective_timeout = timeout or self._http_timeout
        delay = 0.05
        attempts = 3
        for attempt in range(attempts):
            try:
                return requests.post(
                    url,
                    json=payload,
                    timeout=effective_timeout,
                )
            except Exception as exc:  # pragma: no cover - network dependent
                if attempt == attempts - 1:
                    if warn:
                        log.warning("HTTP POST %s failed for %s: %s", url, player, exc)
                    else:
                        log.debug(
                            "HTTP POST %s failed for %s: %s",
                            url,
                            player,
                            exc,
                            exc_info=True,
                        )
                else:
                    time.sleep(delay)
                    delay *= 2
        return None

    def _apply_recording_label(
        self,
        player: str,
        device: Any,
        label: str,
        *,
        session: Optional[int] = None,
        block: Optional[int] = None,
    ) -> None:
        if not label:
            return

        identifier = self._player_device_id.get(player, "") or player
        caps = self._capabilities.get(identifier)
        if caps.frame_name_supported:
            response = self._post_device_api(
                player,
                "/api/frame_name",
                {"frame_name": label},
                warn=False,
            )
            if response is None:
                log.warning("Setting frame_name failed for %s (no response)", player)
            elif response.status_code == 404:
                log.info("Setting frame_name unsupported for %s", player)
            elif response.status_code != 200:
                log.warning(
                    "Setting frame_name failed for %s (%s)",
                    player,
                    response.status_code,
                )
        else:
            log.debug("frame_name skipped (unsupported) device=%s", player)

        payload: Dict[str, Any] = {"label": label}
        if session is not None:
            payload["session"] = session
        if block is not None:
            payload["block"] = block

        event_fn = getattr(device, "send_event", None)
        if callable(event_fn):
            try:
                event_fn(name="recording.label", payload=payload)
                return
            except TypeError:
                try:
                    event_fn("recording.label", payload)
                    return
                except Exception:
                    pass
            except Exception:
                pass

        try:
            self.send_event("recording.label", player, payload)
        except Exception:
            log.debug("recording.label event fallback failed for %s", player, exc_info=True)

    def _wait_for_notification(
        self, device: Any, event: str, timeout: float = 5.0
    ) -> Optional[Any]:
        waiters = ["wait_for_notification", "wait_for_event", "await_notification"]
        for attr in waiters:
            wait_fn = getattr(device, attr, None)
            if callable(wait_fn):
                try:
                    return wait_fn(event, timeout=timeout)
                except TypeError:
                    return wait_fn(event, timeout)
                except TimeoutError:
                    return None
                except Exception:
                    log.debug("Warten auf %s via %s fehlgeschlagen", event, attr, exc_info=True)
        return None

    def _extract_recording_id(self, info: Any) -> Optional[str]:
        if isinstance(info, dict):
            for key in ("recording_id", "id", "uuid"):
                value = info.get(key)
                if value:
                    return str(value)
        return None

    def stop_recording(self, player: str) -> None:
        """Stop the active recording for the player if possible."""

        device = self._device_by_player.get(player)
        if device is None:
            log.info("recording.stop übersprungen (%s: nicht konfiguriert/verbunden)", player)
            return

        if not self._active_recording.get(player):
            log.debug("No active recording to stop for %s", player)
            return

        log.info("recording.stop (%s)", player)

        stop_payload = dict(self._recording_metadata.get(player, {"player": player}))
        stop_payload["player"] = player
        stop_payload["event"] = "stop"
        self.send_event(
            "session.recording_stopped",
            player,
            stop_payload,
        )
        try:
            stop_fn = getattr(device, "recording_stop_and_save", None)
            if callable(stop_fn):
                stop_fn()
            else:
                log.warning("Device for %s lacks recording_stop_and_save", player)
        except Exception as exc:  # pragma: no cover - hardware dependent
            log.exception("Failed to stop recording for %s: %s", player, exc)
            return

        end_info = self._wait_for_notification(device, "recording.end")
        if end_info is not None:
            recording_id = self._extract_recording_id(end_info)
            log.info("recording.end empfangen (%s, id=%s)", player, recording_id or "?")
        else:
            log.info("recording.end nicht bestätigt (%s)", player)

        if player in self._active_recording:
            self._active_recording[player] = False
        self._stop_drift_keepalive(player)
        self._recording_metadata.pop(player, None)

    def connected_players(self) -> list[str]:
        """Return the players that currently have a connected device."""

        return [
            player
            for player, device in self._device_by_player.items()
            if device is not None
        ]

    # ------------------------------------------------------------------
    # Event helpers
    def _event_sender_loop(self) -> None:
        """Background worker that batches UI events before dispatching."""
        if self._event_queue is None:
            return
        while True:
            try:
                item = self._event_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            if item is self._queue_sentinel or self._sender_stop.is_set():
                self._event_queue.task_done()
                break
            if not isinstance(item, _QueuedEvent):
                self._event_queue.task_done()
                continue
            batch: list[_QueuedEvent] = [item]
            deadline = time.perf_counter() + self._event_batch_window
            while len(batch) < self._event_batch_size:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    break
                try:
                    next_item = self._event_queue.get(timeout=max(remaining, 0.0))
                except queue.Empty:
                    break
                if next_item is self._queue_sentinel:
                    self._event_queue.task_done()
                    self._sender_stop.set()
                    break
                if self._sender_stop.is_set():
                    self._event_queue.task_done()
                    break
                if isinstance(next_item, _QueuedEvent):
                    batch.append(next_item)
            self._flush_event_batch(batch)
            for _ in batch:
                self._event_queue.task_done()
            if self._sender_stop.is_set():
                break
        # Drain any remaining events to avoid dropping on shutdown
        while self._event_queue is not None and not self._event_queue.empty():
            try:
                item = self._event_queue.get_nowait()
            except queue.Empty:
                break
            if item is not self._queue_sentinel and isinstance(item, _QueuedEvent):
                self._flush_event_batch([item])
            self._event_queue.task_done()

    def _flush_event_batch(self, batch: list[_QueuedEvent]) -> None:
        """Send a batch of queued events sequentially."""
        if not batch:
            return
        start = time.perf_counter()
        for event in batch:
            self._dispatch_with_metrics(event)
        if self._perf_logging:
            duration = (time.perf_counter() - start) * 1000.0
            if time.monotonic() - self._last_send_log >= 1.0:
                log.debug(
                    "Pupil event batch sent %d events in %.2f ms", len(batch), duration
                )
                self._last_send_log = time.monotonic()

    def _prepare_event_for_device(
        self,
        name: str,
        player: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Prepare the event payload and dispatch asynchronously."""

        device = self._device_by_player.get(player)
        if device is None:
            return

        event_label = name
        fallback_args: tuple[Any, ...] | None = (name, payload)
        if payload:
            try:
                payload_json = json.dumps(payload, separators=(",", ":"), default=str)
            except TypeError:
                safe_payload = self._stringify_payload(payload)
                payload_json = json.dumps(safe_payload, separators=(",", ":"))
            event_label = f"{name}|{payload_json}"

        self._dispatch_event(device, event_label, fallback=fallback_args)

    def _dispatch_event(
        self,
        device: Any,
        event_label: str,
        *,
        fallback: tuple[Any, ...] | None = None,
    ) -> None:
        def _work() -> None:
            try:
                device.send_event(event_label)
            except TypeError as exc:
                if fallback is not None:
                    try:
                        device.send_event(*fallback)
                        return
                    except Exception as fallback_exc:  # pragma: no cover - hardware dependent
                        self._log_error(
                            f"[Failed to send event {event_label}] {fallback_exc}"
                        )
                        return
                self._log_error(f"[Failed to send event {event_label}] {exc}")
            except Exception as exc:  # pragma: no cover - hardware dependent
                self._log_error(f"[Failed to send event {event_label}] {exc}")

        self._event_executor.submit(_work)

    def event_queue_load(self) -> tuple[int, int]:
        if self._event_queue is None:
            return (0, 0)
        return (self._event_queue.qsize(), self._event_queue_maxsize)

    def _dispatch_with_metrics(self, event: _QueuedEvent) -> None:
        try:
            self._prepare_event_for_device(event.name, event.player, event.payload)
        finally:
            t_dispatch_ns = time.perf_counter_ns()
            self._log_dispatch_latency(event, t_dispatch_ns)

    def _on_routed_event(self, player: str, event: UIEvent) -> None:
        payload_dict = dict(event.payload or {})
        prepared_payload = self._normalise_event_payload(payload_dict)
        try:
            t_ui_ns = int(prepared_payload.get("t_local_ns", 0))
        except Exception:
            t_ui_ns = time.perf_counter_ns()
            prepared_payload["t_local_ns"] = t_ui_ns
        event_priority: Literal["high", "normal"]
        if event.priority == "high" or event.name.startswith(_HIGH_PRIORITY_PREFIXES):
            event_priority = "high"
        else:
            event_priority = "normal"
        enqueue_ns = time.perf_counter_ns()
        queued = _QueuedEvent(
            name=event.name,
            player=player,
            payload=prepared_payload,
            priority=event_priority,
            t_ui_ns=int(t_ui_ns),
            t_enqueue_ns=enqueue_ns,
        )
        if event_priority == "normal" and self._buffer_resync_event(player, queued):
            return
        if self._low_latency_disabled or self._event_queue is None or event_priority == "high":
            self._dispatch_with_metrics(queued)
            return
        try:
            self._event_queue.put_nowait(queued)
        except queue.Full:
            self._event_queue_drop += 1
            log.warning(
                "Dropping Pupil event %s for %s – queue full (%d drops)",
                event.name,
                player,
                self._event_queue_drop,
            )
            self._dispatch_with_metrics(queued)
        else:
            if self._perf_logging and self._event_queue.maxsize:
                load = self._event_queue.qsize() / self._event_queue.maxsize
                if load >= 0.8 and time.monotonic() - self._last_queue_log >= 1.0:
                    log.warning(
                        "Pupil event queue at %.0f%% capacity",
                        load * 100.0,
                    )
                    self._last_queue_log = time.monotonic()

    def _log_dispatch_latency(
        self, event: _QueuedEvent, t_dispatch_ns: int
    ) -> None:
        if not self._perf_logging:
            return
        log.debug(
            "bridge latency %s/%s priority=%s t_ui=%d t_enqueue=%d t_dispatch=%d",
            event.player,
            event.name,
            event.priority,
            event.t_ui_ns,
            event.t_enqueue_ns,
            t_dispatch_ns,
        )

    def send_event(
        self,
        name: str,
        player: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        priority: Literal["high", "normal"] = "normal",
    ) -> None:
        """Send an event to the player's device, encoding payload as JSON suffix."""

        event_payload = dict(payload or {})
        enforced_priority = "high" if name.startswith(_HIGH_PRIORITY_PREFIXES) else priority
        event_priority: Literal["high", "normal"] = (
            "high" if enforced_priority == "high" else "normal"
        )
        ui_event = UIEvent(
            name=name,
            payload=event_payload,
            target=player,
            priority=event_priority,
        )
        self._event_router.register_player(player)
        self._event_router.route(ui_event)

    def send_host_mirror(
        self,
        player: str,
        event_id: str,
        t_host_ns: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit a host-side timestamp mirror event for sync diagnostics."""

        payload: Dict[str, Any] = {
            "event_id": event_id,
            "t_host_ns": int(t_host_ns),
            "t_local_ns": int(t_host_ns),
            "origin_player": player,
            "origin_device": "host_mirror",
        }
        if extra:
            payload.update(extra)
        self.send_event(
            "sync.host_ns",
            player,
            payload,
            priority="high",
        )

    # ------------------------------------------------------------------
    def set_drift_keepalive_paused(self, paused: bool) -> None:
        """Toggle whether drift keepalive markers should be paused."""

        paused_flag = bool(paused)
        if self._drift_keepalive_paused == paused_flag:
            return
        self._drift_keepalive_paused = paused_flag
        log.info("drift keepalive %s", "paused" if paused_flag else "resumed")

    def _is_resync_active(self, player: str) -> bool:
        state = self._resync_state.get(player)
        if state is None:
            return False
        return state.active or state.awaiting_ready

    def _emit_drift_keepalive_marker(self, player: str) -> None:
        event_id = str(uuid.uuid4())
        t_host_ns = time.perf_counter_ns()
        payload: Dict[str, Any] = {
            "event_id": event_id,
            "t_local_ns": t_host_ns,
            "origin_player": player,
            "origin_device": "host_drift_keepalive",
            "provisional": False,
            "reason": "drift_keepalive",
        }
        metadata = self._recording_metadata.get(player)
        if isinstance(metadata, dict):
            for key in ("session", "block", "recording_label"):
                if key in metadata and key not in payload:
                    payload[key] = metadata[key]
        self.send_event("sync.flash_beep", player, payload, priority="high")
        self.send_host_mirror(
            player,
            event_id,
            t_host_ns,
            extra={"reason": "drift_keepalive"},
        )

    def _start_drift_keepalive(self, player: str) -> None:
        if self._drift_marker_interval <= 0.0:
            return
        if self._async_loop.is_closed():
            return

        existing = self._drift_keepalive_tasks.get(player)
        if existing is not None:
            existing.cancel()

        interval = max(1.0, self._drift_marker_interval)
        step = min(1.0, max(0.1, interval / 8.0))

        async def drift_keepalive_task() -> None:
            try:
                while True:
                    if not self._active_recording.get(player):
                        return
                    remaining = interval
                    while remaining > 0:
                        if not self._active_recording.get(player):
                            return
                        if self._drift_keepalive_paused or self._is_resync_active(player):
                            await asyncio.sleep(step)
                            continue
                        await asyncio.sleep(step)
                        remaining = max(0.0, remaining - step)
                    if not self._active_recording.get(player):
                        return
                    if self._drift_keepalive_paused or self._is_resync_active(player):
                        continue
                    try:
                        self._emit_drift_keepalive_marker(player)
                    except Exception:  # pragma: no cover - defensive
                        log.debug(
                            "drift keepalive marker failed for %s", player, exc_info=True
                        )
            except asyncio.CancelledError:
                return
            finally:
                self._drift_keepalive_tasks.pop(player, None)

        future = asyncio.run_coroutine_threadsafe(drift_keepalive_task(), self._async_loop)
        self._drift_keepalive_tasks[player] = future

    def _stop_drift_keepalive(self, player: str) -> None:
        future = self._drift_keepalive_tasks.pop(player, None)
        if future is None:
            return
        future.cancel()
        try:
            future.result(timeout=0.1)
        except Exception:
            pass

    def _stop_all_drift_keepalive(self) -> None:
        for player in list(self._drift_keepalive_tasks):
            self._stop_drift_keepalive(player)

    def refine_event(
        self,
        player: str,
        event_id: str,
        t_ref_ns: int,
        *,
        confidence: float,
        mapping_version: int,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Submit a refinement update for an existing event annotation."""

        payload: Dict[str, Any] = {
            "event_id": event_id,
            "t_ref_ns": int(t_ref_ns),
            "confidence": float(confidence),
            "mapping_version": int(mapping_version),
            "refined": True,
            "provisional": False,
            "origin_device": "host_ui",
        }
        if extra:
            payload.update(extra)

        response = None
        try:
            response = self._post_device_api(
                player,
                "/api/annotations/refine",
                payload,
                warn=False,
            )
        except Exception:  # pragma: no cover - network dependent
            log.debug("Refinement REST call failed for %s", player, exc_info=True)

        if response is not None and getattr(response, "status_code", None) in {200, 204}:
            log.debug(
                "Refinement acknowledged via REST for %s (event %s, v%s)",
                player,
                event_id,
                mapping_version,
            )
            return

        log.debug(
            "Falling back to realtime refine event for %s (event %s, v%s)",
            player,
            event_id,
            mapping_version,
        )
        self.send_event("event.refined", player, payload)

    def _normalise_event_payload(
        self, payload: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Ensure mandatory metadata is attached to outgoing event payloads."""

        data: Dict[str, Any] = {}
        if isinstance(payload, dict):
            data.update(payload)

        event_id = data.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            event_id = str(uuid.uuid4())
            data["event_id"] = event_id

        raw_local = data.get("t_local_ns")
        try:
            local_ns = int(raw_local)
        except Exception:
            local_ns = time.perf_counter_ns()
        else:
            if local_ns < 0:
                local_ns = time.perf_counter_ns()
        data["t_local_ns"] = local_ns

        provisional = data.get("provisional", True)
        if isinstance(provisional, str):
            data["provisional"] = provisional.strip().lower() not in {"false", "0", "no"}
        else:
            data["provisional"] = bool(provisional)

        try:
            mapping_version = int(data.get("mapping_version", 0))
        except Exception:
            mapping_version = 0
        data["mapping_version"] = mapping_version

        origin = data.get("origin_device")
        if not isinstance(origin, str) or not origin:
            data["origin_device"] = "host_ui"

        return data

    # ------------------------------------------------------------------
    def estimate_time_offset(self, player: str) -> Optional[float]:
        """Return device_time - host_time in seconds if available.

        The reconciler consumes the value as an offset in nanoseconds
        (device_time minus host_time, i.e. positive if the device clock
        runs ahead of the host).  The realtime API does not document the
        polarity, so we emit a warning once per player and let the
        reconciler lock the sign if required.
        """

        manager = self._time_sync.get(player)
        if manager is None:
            device = self._device_by_player.get(player)
            if device is None:
                return None
            device_id = self._player_device_id.get(player, "") or player
            self._setup_time_sync(player, device_id, device)
            manager = self._time_sync.get(player)
            if manager is None:
                return None
        try:
            asyncio.run_coroutine_threadsafe(
                manager.maybe_resync(), self._async_loop
            )
        except Exception:
            pass
        return manager.get_offset_s()

    def is_connected(self, player: str) -> bool:
        """Return whether the given player has an associated device."""

        return self._device_by_player.get(player) is not None

    # ------------------------------------------------------------------
    @staticmethod
    def _stringify_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Convert non-serialisable payload entries to strings."""

        result: Dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                result[key] = value
            elif isinstance(value, dict):
                result[key] = PupilBridge._stringify_payload(value)  # type: ignore[arg-type]
            elif isinstance(value, (list, tuple)):
                result[key] = [PupilBridge._coerce_item(item) for item in value]
            else:
                result[key] = str(value)
        return result

    @staticmethod
    def _coerce_item(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return PupilBridge._stringify_payload(value)  # type: ignore[arg-type]
        if isinstance(value, (list, tuple)):
            return [PupilBridge._coerce_item(item) for item in value]
        return str(value)


__all__ = ["PupilBridge", "NeonDeviceConfig", "RecordingStartResult"]
