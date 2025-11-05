from __future__ import annotations

import csv
import itertools
import logging
import os
import random
import time
import uuid
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

import numpy as np
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.properties import DictProperty, NumericProperty, ObjectProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.uix.spinner import Spinner
from kivy.uix.switch import Switch
from kivy.uix.textinput import TextInput

from tabletop.data.blocks import load_blocks, load_csv_rounds, value_to_card_path
from tabletop.data.config import ARUCO_OVERLAY_PATH, ROOT
from tabletop.logging import async_bridge
from tabletop.logging.events import Events
from tabletop.logging.round_csv import (
    close_round_log,
    init_round_log,
    round_log_action_label,
    write_round_log,
)
from tabletop.overlay.fixation import (
    generate_fixation_tone,
    play_fixation_tone as overlay_play_fixation_tone,
    run_fixation_sequence as overlay_run_fixation_sequence,
)
from tabletop.overlay.process import start_overlay_process, stop_overlay_process
from tabletop.state.controller import TabletopController, TabletopState
from tabletop.state.phases import UXPhase, to_engine_phase
from tabletop.ui import widgets as ui_widgets
from tabletop.engine import POINTS_PER_WIN, EventLogger
from tabletop.sync.reconciler import TimeReconciler
from tabletop.utils.async_tasks import AsyncCallQueue
from tabletop.utils.input_timing import Debouncer
from tabletop.utils.runtime import (
    is_low_latency_disabled,
    is_perf_logging_enabled,
)
from tabletop.ui.assets import (
    ASSETS,
    FIX_LIVE_IMAGE,
    FIX_STOP_IMAGE,
    resolve_background_texture,
)
from tabletop.ui.widgets import CardWidget, IconButton, RotatableLabel

Window.multitouch_on_demand = True

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from tabletop.pupil_bridge import PupilBridge


ui_widgets.ASSETS = ASSETS

STATE_FIELD_NAMES = set(TabletopState.__dataclass_fields__)


class _AsyncMarkerBridge:
    def __init__(self, owner: "TabletopRoot") -> None:
        self._owner = owner

    def enqueue(self, name: str, payload: Dict[str, Any]) -> None:
        payload_copy = dict(payload) if payload is not None else {}

        def _dispatch() -> None:
            self._owner.send_bridge_event(name, payload_copy)

        async_bridge.enqueue(_dispatch)


class TabletopRoot(FloatLayout):
    _STATE_FIELDS = STATE_FIELD_NAMES

    SCALE_FACTOR = NumericProperty(0.7)

    bg_texture = ObjectProperty(None, rebind=True)
    base_width = NumericProperty(3840.0)
    base_height = NumericProperty(2160.0)
    button_scale = NumericProperty(0.8)
    scale = NumericProperty(1.0)
    horizontal_offset = NumericProperty(0.08)
    # Responsive Seitenränder (Prozent + physische Untergrenze)
    side_margin_frac = NumericProperty(0.14)
    side_margin_min_px = NumericProperty(280.0)
    side_margin_max_frac = NumericProperty(0.22)
    side_margin_target_cm = NumericProperty(3.2)

    # Ergebniswert (in Pixeln)
    horizontal_margin_px = NumericProperty(0.0)

    btn_start_p1 = ObjectProperty(None)
    btn_start_p2 = ObjectProperty(None)
    p1_outer = ObjectProperty(None)
    p1_inner = ObjectProperty(None)
    p2_outer = ObjectProperty(None)
    p2_inner = ObjectProperty(None)
    intro_overlay = ObjectProperty(None)
    pause_cover = ObjectProperty(None)
    fixation_overlay = ObjectProperty(None)
    fixation_image = ObjectProperty(None)
    round_badge = ObjectProperty(None)

    signal_buttons = DictProperty({})
    decision_buttons = DictProperty({})
    center_cards = DictProperty({})
    user_displays = DictProperty({})
    intro_labels = DictProperty({})
    pause_labels = DictProperty({})

    def wid(self, name: str):
        # Liefert das Widget-Objekt oder None, ohne Truthiness auf WeakProxy auszulösen
        return self.ids.get(name, None)

    def wid_safe(self, name: str):
        # Wie wid(), aber tolerant gegen bereits freigegebene WeakProxy-Objekte
        w = self.ids.get(name, None)
        if w is None:
            return None
        try:
            # sanfter Deref-Test, löst ReferenceError aus, falls freigegeben
            _ = w.opacity
        except ReferenceError:
            return None
        return w

    @property
    def _current_ab_version(self) -> int:
        reconciler = self._time_reconciler
        if reconciler is None:
            return 0
        return reconciler.current_mapping_version

    def __init__(
        self,
        *,
        controller: Optional[TabletopController] = None,
        state: Optional[TabletopState] = None,
        events_factory: Callable[[str, str], Events] = Events,
        start_overlay: Callable[..., Optional[Any]] = start_overlay_process,
        stop_overlay: Callable[[Optional[Any]], Optional[Any]] = stop_overlay_process,
        fixation_runner: Callable[..., Any] = overlay_run_fixation_sequence,
        fixation_player: Callable[[Any], None] = overlay_play_fixation_tone,
        fixation_tone_factory: Callable[[int], Any] = generate_fixation_tone,
        bridge: Optional["PupilBridge"] = None,
        bridge_player: str = "VP1",
        bridge_session: Optional[int] = None,
        bridge_block: Optional[int] = None,
        single_block_mode: bool = False,
        perf_logging: bool = False,
        **kw: Any,
    ):
        super().__init__(**kw)
        self.events_factory = events_factory
        self.start_overlay = start_overlay
        self.stop_overlay = stop_overlay
        self.fixation_runner = fixation_runner
        self.fixation_player = fixation_player
        self.fixation_tone_factory = fixation_tone_factory
        self.bg_texture = resolve_background_texture()
        Window.bind(on_resize=self._on_window_resize)
        self.bind(size=self._update_scale)

        if state is None:
            state = TabletopState(blocks=load_blocks())
        elif not state.blocks:
            state.blocks = load_blocks()
        self.controller = controller or TabletopController(state)
        self._blocks = state.blocks if state.blocks else load_blocks()
        self.aruco_enabled = False
        self._aruco_proc = None
        self.start_block = 1
        # Versuchsperson 1 sitzt immer unten (Spieler 1), Versuchsperson 2 oben (Spieler 2)
        self._fixed_role_mapping = {1: 1, 2: 2}
        self.role_by_physical = self._fixed_role_mapping.copy()
        self.physical_by_role = {role: player for player, role in self.role_by_physical.items()}
        self.update_turn_order()
        self.phase = UXPhase.WAIT_BOTH_START
        self.session_number = None
        self.session_id = None
        self.session_storage_id = None
        self.logger = None
        self.log_dir = Path(ROOT) / 'logs'
        self.session_popup = None
        self.session_configured = False
        self.round_log_path = None
        self.round_log_fp = None
        self.round_log_writer = None
        self.round_log_buffer = []
        self.overlay_display_index = 0

        self._low_latency_disabled = is_low_latency_disabled()
        self.perf_logging = (
            bool(perf_logging) or is_perf_logging_enabled()
        ) and not self._low_latency_disabled
        self._input_debouncer = Debouncer()
        self._handler_log_gate: Dict[str, float] = {}
        self._bridge_dispatcher = AsyncCallQueue(
            "BridgeDispatch",
            maxsize=1000,
            perf_logging=self.perf_logging,
        )
        self.marker_bridge: Optional[_AsyncMarkerBridge] = _AsyncMarkerBridge(self)
        if self.perf_logging:
            Clock.schedule_interval(self._log_async_metrics, 1.0)
        self._bridge: Optional["PupilBridge"] = None
        self._bridge_player: Optional[str] = None
        self._bridge_players: set[str] = set()
        self._bridge_session: Optional[int] = None
        self._bridge_block: Optional[int] = None
        self._bridge_recordings_active: set[str] = set()
        self._bridge_recording_block: Optional[int] = None
        self._single_block_mode = single_block_mode
        self._bridge_state_dirty = True
        self._next_bridge_check = 0.0
        self._bridge_check_interval = 0.3
        self._time_reconciler: Optional[TimeReconciler] = None
        self._heartbeat_event: Optional[Any] = None
        self._heartbeat_interval = 30.0
        self._heartbeat_jitter = 5.0
        self._heartbeat_label = "sync.heartbeat"
        self._heartbeat_counter = 0
        self._origin_device_id = "host_ui"
        self.update_bridge_context(
            bridge=bridge,
            player=bridge_player,
            players={bridge_player} if bridge_player else None,
            session=bridge_session,
            block=bridge_block,
        )
        # kick recordings once Kivy has a chance to finish layout & session may be set
        Clock.schedule_once(
            lambda *_: self._ensure_bridge_recordings(force=True), 0.2
        )

        # --- UI Elemente initialisieren
        self._configure_widgets()
        self.setup_round()
        self.apply_phase()
        if self._single_block_mode and self._bridge_session is not None and self._bridge_block is not None:
            Clock.schedule_once(self._configure_session_from_cli, 0.1)
        else:
            Clock.schedule_once(lambda *_: self.prompt_session_number(), 0.1)

    def __setattr__(self, key, value):
        if key in self._STATE_FIELDS and 'controller' in self.__dict__:
            setattr(self.controller.state, key, value)
            return
        if key == 'overlay_process':
            super().__setattr__(key, value)
            object.__setattr__(self, '_aruco_proc', value)
            return
        super().__setattr__(key, value)

    def __getattr__(self, item):
        if item in self._STATE_FIELDS and 'controller' in self.__dict__:
            return getattr(self.controller.state, item)
        raise AttributeError(item)

    # ------------------------------------------------------------------
    # Bridge helpers
    def update_bridge_context(
        self,
        *,
        bridge: Optional["PupilBridge"],
        player: Optional[str] = None,
        players: Optional[Iterable[str]] = None,
        session: Optional[int],
        block: Optional[int],
    ) -> None:
        self._bridge = bridge
        if players is not None:
            self._bridge_players = {p for p in players if p}
        elif player:
            self._bridge_players = {player}
        elif bridge is not None:
            with suppress(AttributeError):
                detected = bridge.connected_players()
                if detected:
                    self._bridge_players = {p for p in detected if p}
        if self._bridge_players:
            self._bridge_player = next(iter(self._bridge_players))
        elif player:
            self._bridge_player = player
        if session is not None:
            self._bridge_session = session
        if block is not None:
            self._bridge_block = block
        players_snapshot = set(self._bridge_players)
        if players_snapshot and "VP2" not in players_snapshot:
            log.info("Nur VP1 aktiv – VP2 deaktiviert")

        if bridge is not None:
            def _kick_autostart(_dt: float) -> None:
                bridge_ref = self._bridge
                if bridge_ref is None:
                    return
                selected = players_snapshot or (
                    {self._bridge_player} if self._bridge_player else None
                )
                try:
                    bridge_ref.ensure_recordings(
                        session=self._bridge_session,
                        block=self._bridge_block,
                        players=selected,
                    )
                except AttributeError:
                    pass

            Clock.schedule_once(_kick_autostart, 0.2)

        self._mark_bridge_dirty()
        self._ensure_bridge_recordings()
        self._ensure_time_reconciler()
        if self.session_configured:
            self._schedule_sync_heartbeat(immediate=False)

    def _bridge_payload_base(self, *, player: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self._bridge_session is not None:
            payload["session"] = self._bridge_session
        if self._bridge_block is not None:
            payload["block"] = self._bridge_block
        if player is not None:
            payload["player"] = player
        elif self._bridge_player is not None:
            payload["player"] = self._bridge_player
        return payload

    def _bridge_ready_players(self) -> List[str]:
        if not self._bridge:
            return []

        players = set(self._bridge_players)
        if not players:
            with suppress(AttributeError):
                detected = self._bridge.connected_players()
                if detected:
                    players = {p for p in detected if p}
                    self._bridge_players = players

        return [player for player in players if self._bridge.is_connected(player)]

    def _current_bridge_block_index(self) -> Optional[int]:
        block_info = self.current_block_info
        if isinstance(block_info, dict):
            idx = block_info.get("index")
            try:
                return int(idx) if idx is not None else None
            except (TypeError, ValueError):
                return None
        return None

    def _mark_bridge_dirty(self) -> None:
        self._bridge_state_dirty = True

    def _ensure_bridge_recordings(self, *_: Any, force: bool = False) -> None:
        if not self._bridge or not self.session_configured:
            return

        if force:
            self._bridge_state_dirty = True

        now = time.monotonic()
        if not self._bridge_state_dirty and now < self._next_bridge_check:
            return

        if self._bridge_session is None and self.session_number is not None:
            self._bridge_session = self.session_number

        current_block = self._current_bridge_block_index()
        if current_block is not None:
            self._bridge_block = current_block

        session_value = self._bridge_session
        block_value = self._bridge_block
        if session_value is None or block_value is None:
            self._bridge_state_dirty = True
            self._next_bridge_check = now + self._bridge_check_interval
            return

        if (
            self._bridge_recording_block is not None
            and block_value != self._bridge_recording_block
            and self._bridge_recordings_active
        ):
            self.stop_bridge_recordings()

        players = self._bridge_ready_players()
        if not players:
            self._bridge_state_dirty = True
            self._next_bridge_check = now + self._bridge_check_interval
            return

        for player in players:
            if player in self._bridge_recordings_active:
                continue
            self._bridge.start_recording(session_value, block_value, player)
            self._bridge_recordings_active.add(player)

        if self._bridge_recordings_active:
            self._bridge_recording_block = block_value
            self._bridge_state_dirty = False
        else:
            self._bridge_state_dirty = True
        self._next_bridge_check = now + self._bridge_check_interval

    def _log_async_metrics(self, _dt: float) -> None:
        if not self.perf_logging:
            return
        size, capacity = self._bridge_dispatcher.load()
        if not capacity or size == 0:
            return
        log.debug("Bridge dispatch queue load: %d/%d", size, capacity)

    def _record_handler_duration(self, name: str, started: float) -> None:
        if not self.perf_logging:
            return
        duration_ms = (time.perf_counter() - started) * 1000.0
        now = time.monotonic()
        last = self._handler_log_gate.get(name, 0.0)
        if duration_ms >= 1.0 or now - last >= 1.0:
            log.debug("%s completed in %.3f ms", name, duration_ms)
            self._handler_log_gate[name] = now

    def stop_bridge_recordings(self) -> None:
        if not self._bridge_recordings_active:
            self._bridge_recording_block = None
            return

        if not self._bridge:
            self._bridge_recordings_active.clear()
            self._bridge_recording_block = None
            return

        for player in list(self._bridge_recordings_active):
            try:
                self._bridge.stop_recording(player)
            finally:
                self._bridge_recordings_active.discard(player)

        self._bridge_recording_block = None
        self._mark_bridge_dirty()

    def _resolve_event_logger(self) -> Optional[EventLogger]:
        logger_obj = getattr(self, "logger", None)
        if isinstance(logger_obj, EventLogger):
            return logger_obj
        inner = getattr(logger_obj, "_logger", None)
        if isinstance(inner, EventLogger):
            return inner
        return None

    def _ensure_time_reconciler(self) -> None:
        if self._time_reconciler is not None:
            return
        if not self._bridge:
            return
        event_logger = self._resolve_event_logger()
        if event_logger is None:
            return
        try:
            reconciler = TimeReconciler(self._bridge, event_logger)
        except Exception:
            log.exception("Zeitabgleich konnte nicht initialisiert werden")
            return
        reconciler.start()
        self._time_reconciler = reconciler

    def _cancel_sync_heartbeat(self) -> None:
        event = self._heartbeat_event
        if event is None:
            return
        cancel = getattr(event, "cancel", None)
        if callable(cancel):
            try:
                cancel()
            except Exception:
                log.debug("Abbruch des Sync-Heartbeats fehlgeschlagen", exc_info=True)
        self._heartbeat_event = None

    def _schedule_sync_heartbeat(self, *, immediate: bool = False) -> None:
        self._ensure_time_reconciler()
        if not self._bridge:
            return
        delay = 0.5 if immediate else self._heartbeat_interval
        if not immediate:
            jitter = min(self._heartbeat_jitter, self._heartbeat_interval * 0.25)
            if jitter > 0:
                delay = max(1.0, self._heartbeat_interval + random.uniform(-jitter, jitter))
        self._cancel_sync_heartbeat()
        self._heartbeat_event = Clock.schedule_once(self._emit_sync_heartbeat, delay)

    def _emit_sync_heartbeat(self, _dt: float) -> None:
        payload = {"marker": "hb", "heartbeat_index": self._heartbeat_counter}
        self._heartbeat_counter += 1
        try:
            if self.marker_bridge:
                # non-blocking: moved bridge send to async enqueue
                self.marker_bridge.enqueue(self._heartbeat_label, payload)
            else:
                self.send_bridge_event(self._heartbeat_label, payload)
        finally:
            self._schedule_sync_heartbeat(immediate=False)

    def _notify_event_pipeline(self, name: str, event_id: str, t_local_ns: int) -> None:
        reconciler = self._time_reconciler
        if reconciler is None:
            return
        try:
            reconciler.on_event(event_id, t_local_ns)
            if name.startswith("sync."):
                reconciler.submit_marker(name, t_local_ns)
        except Exception:
            log.debug("Weiterleitung an TimeReconciler fehlgeschlagen", exc_info=True)

    def shutdown_sync_services(self) -> None:
        self._cancel_sync_heartbeat()
        reconciler = self._time_reconciler
        if reconciler is not None:
            try:
                reconciler.stop()
            except Exception:
                log.debug("Stoppen des TimeReconciler fehlgeschlagen", exc_info=True)
            self._time_reconciler = None

    def send_bridge_event(
        self, name: str, payload: Optional[Dict[str, Any]] = None
    ) -> None:
        if not self._bridge:
            return
        self._ensure_time_reconciler()
        self._ensure_bridge_recordings()
        players = self._bridge_ready_players()
        if not players:
            return
        event_id = str(uuid.uuid4())
        t_local_ns = time.perf_counter_ns()
        priority = "high" if name.startswith(("sync.", "fix.")) else "normal"
        mapping_version = self._current_ab_version
        payload_copy: Dict[str, Any] = {}
        if payload:
            payload_copy.update(payload)
        session_label = self.session_id
        bridge_session = self._bridge_session
        bridge_block = self._bridge_block
        session_number = self.session_number

        def _dispatch() -> None:
            bridge_ref = self._bridge
            if not bridge_ref:
                return
            for player in players:
                event_payload = self._bridge_payload_base(player=player)
                event_payload.update(payload_copy)
                event_payload["event_id"] = event_id
                event_payload["t_local_ns"] = t_local_ns
                event_payload["provisional"] = True
                event_payload["mapping_version"] = mapping_version
                event_payload["origin_device"] = self._origin_device_id
                event_payload.setdefault("origin_player", player)
                if session_label:
                    event_payload.setdefault("session_label", session_label)
                if session_number is not None:
                    event_payload.setdefault("session_number", session_number)
                if bridge_session is not None:
                    event_payload.setdefault("bridge_session", bridge_session)
                if bridge_block is not None:
                    event_payload.setdefault("bridge_block", bridge_block)
                try:
                    bridge_ref.send_event(
                        name,
                        player,
                        event_payload,
                        priority=priority,
                    )
                    if priority == "high":
                        mirror_extra: Dict[str, Any] = {"mapping_version": mapping_version}
                        if bridge_session is not None:
                            mirror_extra["session"] = bridge_session
                        if bridge_block is not None:
                            mirror_extra["block"] = bridge_block
                        if session_label:
                            mirror_extra["session_label"] = session_label
                        if session_number is not None:
                            mirror_extra["session_number"] = session_number
                        bridge_ref.send_host_mirror(
                            player,
                            event_id,
                            t_local_ns,
                            extra=mirror_extra,
                        )
                except Exception:
                    log.exception("Bridge event dispatch failed: %s", name)
            self._notify_event_pipeline(name, event_id, t_local_ns)

        self._bridge_dispatcher.submit(_dispatch)  # non-blocking: moved to worker

    def _emit_button_bridge_event(
        self,
        button: str,
        *,
        player: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {"button": button}
        phase = getattr(self.phase, "name", None)
        if phase:
            payload["phase"] = phase
        try:
            round_idx = max(0, getattr(self, "round", 1) - 1)
        except Exception:
            round_idx = 0
        payload["round_index"] = round_idx
        if player in (1, 2):
            payload["game_player"] = player
            role = self.player_roles.get(player)
            if role is not None:
                payload["player_role"] = role
        if extra:
            payload.update(extra)

        actor = "SYS"
        if player in (1, 2):
            try:
                actor = self._actor_label(player)
            except Exception:
                role = None
                try:
                    role = self.player_roles.get(player)
                except Exception:
                    role = None
                if role == 1:
                    actor = "P1"
                elif role == 2:
                    actor = "P2"
                else:
                    actor = "P1" if player == 1 else "P2"
        payload["actor"] = actor

        if self.marker_bridge:
            self.marker_bridge.enqueue(f"button.{button}", payload)  # enriched payload (non-blocking)
        else:
            self.send_bridge_event(f"button.{button}", payload)

    # ------------------------------------------------------------------
    # Session helpers
    def _available_block_count(self) -> int:
        blocks = self._blocks or []
        return len(blocks)

    def _clamp_start_block_choice(self, choice: int) -> int:
        total = self._available_block_count()
        if total <= 0:
            return 1
        return max(1, min(choice, total))

    def _start_block_from_cli(self, block_index: Optional[int]) -> int:
        total = self._available_block_count()
        if total <= 0:
            return 1
        try:
            index = int(block_index) if block_index is not None else 0
        except (TypeError, ValueError):
            index = 0
        index = max(0, min(index, total - 1))
        return index + 1

    def _finalize_session_setup(
        self,
        session_label: str,
        *,
        start_block_value: Optional[int] = None,
        aruco_enabled: Optional[bool] = None,
    ) -> None:
        if not session_label:
            return

        self.session_id = session_label
        digits = ''.join(ch for ch in session_label if ch.isdigit())
        self.session_number = int(digits) if digits else None

        start_choice = start_block_value if start_block_value is not None else self.start_block
        self.start_block = self._clamp_start_block_choice(start_choice)

        if aruco_enabled is not None:
            self.aruco_enabled = bool(aruco_enabled)

        safe_session_id = ''.join(
            ch if ch.isalnum() or ch in ('-', '_') else '_'
            for ch in self.session_id
        ) or 'session'

        self.session_configured = True
        self.log_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.log_dir / f'events_{safe_session_id}.sqlite3'
        self.session_storage_id = safe_session_id
        self.logger = self.events_factory(self.session_id, str(db_path))
        self._ensure_time_reconciler()
        self._schedule_sync_heartbeat(immediate=True)
        init_round_log(self)
        self.update_role_assignments()

        self.log_event(
            None,
            'session_start',
            {
                'session_number': self.session_number,
                'session_id': self.session_id,
                'aruco_enabled': self.aruco_enabled,
                'start_block': self.start_block,
            },
        )
        self._mark_bridge_dirty()
        self._ensure_bridge_recordings()
        self._apply_session_options_and_start()

    def _configure_session_from_cli(self, *_args: Any) -> None:
        if self.session_configured:
            return
        if self._bridge_session is None:
            self.prompt_session_number()
            return
        start_block_value = self._start_block_from_cli(self._bridge_block)
        self._finalize_session_setup(
            str(self._bridge_session),
            start_block_value=start_block_value,
            aruco_enabled=self.aruco_enabled,
        )

    # --- Layout & Elemente
    def _configure_widgets(self):
        btn_start_p1 = self.wid_safe('btn_start_p1')
        if btn_start_p1 is not None:
            btn_start_p1.bind(on_release=lambda *_: self.start_pressed(1))
            btn_start_p1.set_rotation(0)
        btn_start_p2 = self.wid_safe('btn_start_p2')
        if btn_start_p2 is not None:
            btn_start_p2.bind(on_release=lambda *_: self.start_pressed(2))
            btn_start_p2.set_rotation(180)

        pause_btn_p1 = self.wid_safe('pause_btn_p1')
        if pause_btn_p1 is not None:
            pause_btn_p1.bind(on_release=lambda *_: self.start_pressed(1))
            pause_btn_p1.set_rotation(0)
            pause_btn_p1.set_live(False)
            pause_btn_p1.disabled = True
            pause_btn_p1.opacity = 0
        pause_btn_p2 = self.wid_safe('pause_btn_p2')
        if pause_btn_p2 is not None:
            pause_btn_p2.bind(on_release=lambda *_: self.start_pressed(2))
            pause_btn_p2.set_rotation(180)
            pause_btn_p2.set_live(False)
            pause_btn_p2.disabled = True
            pause_btn_p2.opacity = 0

        p1_outer = self.wid_safe('p1_outer')
        if p1_outer is not None:
            p1_outer.bind(on_release=lambda *_: self.tap_card(1, 'outer'))
        p1_inner = self.wid_safe('p1_inner')
        if p1_inner is not None:
            p1_inner.bind(on_release=lambda *_: self.tap_card(1, 'inner'))
        p2_outer = self.wid_safe('p2_outer')
        if p2_outer is not None:
            p2_outer.bind(on_release=lambda *_: self.tap_card(2, 'outer'))
        p2_inner = self.wid_safe('p2_inner')
        if p2_inner is not None:
            p2_inner.bind(on_release=lambda *_: self.tap_card(2, 'inner'))

        self.signal_buttons = {
            1: {
                'low': 'signal_p1_low',
                'mid': 'signal_p1_mid',
                'high': 'signal_p1_high',
            },
            2: {
                'low': 'signal_p2_low',
                'mid': 'signal_p2_mid',
                'high': 'signal_p2_high',
            },
        }
        for level, btn_id in self.signal_buttons.get(1, {}).items():
            btn = self.wid_safe(btn_id)
            if btn is not None:
                btn.bind(on_release=lambda _, lvl=level: self.pick_signal(1, lvl))
                btn.set_rotation(0)
        for level, btn_id in self.signal_buttons.get(2, {}).items():
            btn = self.wid_safe(btn_id)
            if btn is not None:
                btn.bind(on_release=lambda _, lvl=level: self.pick_signal(2, lvl))
                btn.set_rotation(180)

        self.decision_buttons = {
            1: {
                'bluff': 'decision_p1_bluff',
                'wahr': 'decision_p1_wahr',
            },
            2: {
                'bluff': 'decision_p2_bluff',
                'wahr': 'decision_p2_wahr',
            },
        }
        for choice, btn_id in self.decision_buttons.get(1, {}).items():
            btn = self.wid_safe(btn_id)
            if btn is not None:
                btn.bind(on_release=lambda _, ch=choice: self.pick_decision(1, ch))
                btn.set_rotation(0)
        for choice, btn_id in self.decision_buttons.get(2, {}).items():
            btn = self.wid_safe(btn_id)
            if btn is not None:
                btn.bind(on_release=lambda _, ch=choice: self.pick_decision(2, ch))
                btn.set_rotation(180)

        self.center_cards = {
            1: ['center_p1_card_right', 'center_p1_card_left'],
            2: ['center_p2_card_left', 'center_p2_card_right'],
        }

        self.user_displays = {
            1: 'user_display_p1',
            2: 'user_display_p2',
        }
        for player, display_id in self.user_displays.items():
            display = self.wid_safe(display_id)
            if display is not None:
                display.set_rotation(0 if player == 1 else 180)
                display.text = ''
                display.opacity = 1

        self.intro_labels = {
            1: 'intro_label_p1',
            2: 'intro_label_p2',
        }
        for player, label_id in self.intro_labels.items():
            label = self.wid_safe(label_id)
            if label is not None:
                label.set_rotation(0 if player == 1 else 180)

        self.pause_labels = {
            1: 'pause_label_p1',
            2: 'pause_label_p2',
        }
        for player, label_id in self.pause_labels.items():
            label = self.wid_safe(label_id)
            if label is not None:
                label.set_rotation(0 if player == 1 else 180)
                label.bind(texture_size=lambda *_: None)

        self.pause_start_buttons = {
            1: 'pause_btn_p1',
            2: 'pause_btn_p2',
        }

        fixation_overlay = self.wid_safe('fixation_overlay')
        if fixation_overlay is not None:
            fixation_overlay.opacity = 0
            fixation_overlay.disabled = True
        fixation_image = self.wid_safe('fixation_image')
        if fixation_image is not None:
            fixation_image.opacity = 1

        self.bring_start_buttons_to_front()

        self._update_scale()

        # interne States
        self.p1_pressed = False
        self.p2_pressed = False
        self.player_signals = {1: None, 2: None}
        self.player_decisions = {1: None, 2: None}
        self.status_lines = {1: [], 2: []}
        self.status_labels = {1: None, 2: None}
        self.last_outcome = {
            'winner': None,
            'truthful': None,
            'actual_level': None,
            'signal_choice': None,
            'judge_choice': None
        }
        self.card_cycle = itertools.cycle(['7.png', '8.png', '9.png', '10.png', '11.png'])

        self.total_rounds_planned = sum(len(block['rounds']) for block in self.blocks)
        self.overlay_process = self._aruco_proc
        self.fixation_running = False
        self.fixation_required = False
        self.pending_fixation_callback = None
        self.intro_active = True
        self.next_block_preview = None
        self.fixation_tone_fs = 44100
        self.fixation_tone = self.fixation_tone_factory(self.fixation_tone_fs)
        self._update_scale()
        self.update_user_displays()
        self.update_intro_overlay()

    def bring_start_buttons_to_front(self):
        btn_start_p1 = self.wid_safe('btn_start_p1')
        btn_start_p2 = self.wid_safe('btn_start_p2')
        if btn_start_p1 is not None and btn_start_p1.parent is self:
            self.remove_widget(btn_start_p1)
            self.add_widget(btn_start_p1)
        if btn_start_p2 is not None and btn_start_p2.parent is self:
            self.remove_widget(btn_start_p2)
            self.add_widget(btn_start_p2)

    def update_intro_overlay(self):
        intro_overlay = self.wid_safe('intro_overlay')
        if intro_overlay is None:
            return
        active = bool(self.intro_active)
        if active:
            if intro_overlay.parent is None:
                self.add_widget(intro_overlay)
            intro_overlay.opacity = 1
            intro_overlay.disabled = False
            self.bring_start_buttons_to_front()
        else:
            intro_overlay.opacity = 0
            intro_overlay.disabled = True
            if intro_overlay.parent is not None:
                self.remove_widget(intro_overlay)
                self.bring_start_buttons_to_front()

    def _on_window_resize(self, *_):
        self.size = Window.size
        self._update_scale()

    def _update_scale(self, *_):
        base_w = self.base_width or 3840.0
        base_h = self.base_height or 2160.0
        width = self.width or Window.width
        height = self.height or Window.height

        base_scale = min(width / base_w, height / base_h)
        self.scale = self.SCALE_FACTOR * base_scale
        self.horizontal_offset = 0.05 if width < 2500 else 0.08

        # --- Responsive Margin berechnen ---
        frac_px = float(width) * float(self.side_margin_frac)

        # physikalischer Zielwert → px
        dpi = getattr(Window, "dpi", 96.0) or 96.0
        px_per_cm = dpi / 2.54
        cm_px = float(self.side_margin_target_cm) * px_per_cm

        # harte Klammern: mindestens min_px bzw. cm, maximal Anteil der Breite
        min_px = max(float(self.side_margin_min_px), cm_px)
        max_px = float(width) * float(self.side_margin_max_frac)

        # finale Margin
        self.horizontal_margin_px = max(min(frac_px, max_px), min_px)

    @staticmethod
    def _parse_value(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(',', '.')
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    def _cards_for_role(self, role: int):
        if role not in (1, 2):
            return None
        plan_info = self.get_current_plan()
        if plan_info:
            _, plan = plan_info
            cards = plan.get(f'vp{role}') if plan else None
            if cards and len(cards) == 2 and not any(card is None for card in cards):
                return tuple(cards)
        # Fallback über die sichtbaren Karten
        player = self.physical_by_role.get(role)
        if player == 1:
            inner_widget = self.wid_safe('p1_inner')
            outer_widget = self.wid_safe('p1_outer')
        elif player == 2:
            inner_widget = self.wid_safe('p2_inner')
            outer_widget = self.wid_safe('p2_outer')
        else:
            return None
        if inner_widget is None or outer_widget is None:
            return None
        inner_val = self.card_value_from_path(inner_widget.front_image)
        outer_val = self.card_value_from_path(outer_widget.front_image)
        if inner_val is None or outer_val is None:
            return None
        return (inner_val, outer_val)

    def get_hand_total_for_role(self, role: int):
        cards = self._cards_for_role(role)
        if not cards:
            return None
        return sum(cards)

    def get_hand_value_for_role(self, role: int):
        total = self.get_hand_total_for_role(role)
        if total is None:
            return None
        return 0 if total in (20, 21, 22) else total

    def get_hand_value_for_player(self, player: int):
        role = self.role_by_physical.get(player)
        value = self.get_hand_value_for_role(role)
        if value is not None:
            return value
        total = self.get_hand_total_for_player(player)
        if total is None:
            return None
        return 0 if total in (20, 21, 22) else total

    def get_hand_total_for_player(self, player: int):
        role = self.role_by_physical.get(player)
        return self.get_hand_total_for_role(role) if role in (1, 2) else None

    def signal_level_from_value(self, value):
        parsed = self._parse_value(value)
        if parsed is None:
            return None
        if parsed <= 0:
            return None
        if parsed in (20, 21, 22):
            return None
        if parsed == 19:
            return 'high'
        if parsed in (16, 17, 18):
            return 'mid'
        if parsed in (14, 15):
            return 'low'
        if parsed > 22:
            return None
        if parsed >= 16:
            return 'mid'
        return 'low'

    def set_cards_from_plan(self, plan):
        if plan:
            vp1_cards = plan['vp1']
            vp2_cards = plan['vp2']
            first_vp1, second_vp1 = vp1_cards[0], vp1_cards[1]
            first_vp2, second_vp2 = vp2_cards[0], vp2_cards[1]
            p1_inner = self.wid_safe('p1_inner')
            if p1_inner is not None:
                p1_inner.set_front(value_to_card_path(first_vp1))
            p1_outer = self.wid_safe('p1_outer')
            if p1_outer is not None:
                p1_outer.set_front(value_to_card_path(second_vp1))
            p2_inner = self.wid_safe('p2_inner')
            if p2_inner is not None:
                p2_inner.set_front(value_to_card_path(first_vp2))
            p2_outer = self.wid_safe('p2_outer')
            if p2_outer is not None:
                p2_outer.set_front(value_to_card_path(second_vp2))
        else:
            default = ASSETS['cards']['back']
            for card_id in ('p1_inner', 'p1_outer', 'p2_inner', 'p2_outer'):
                widget = self.wid_safe(card_id)
                if widget is not None:
                    widget.set_front(default)

    def compute_global_round(self):
        return self.controller.compute_global_round()

    def score_line_text(self):
        if self.score_state:
            return f'Spielstand – VP1: {self.score_state[1]} | VP2: {self.score_state[2]}'
        return 'Spielstand – VP1: - | VP2: -'

    def get_current_plan(self):
        return self.controller.get_current_plan()

    def peek_next_round_info(self):
        """Ermittelt Metadaten zur nächsten Runde ohne den Status zu verändern."""
        return self.controller.peek_next_round_info()

    def advance_round_pointer(self):
        self.controller.advance_round_pointer()

    # --- Logik
    def apply_phase(self):
        phase_state = self.controller.apply_phase()
        for card_id in ('p1_outer', 'p1_inner', 'p2_outer', 'p2_inner'):
            widget = self.wid_safe(card_id)
            if widget is not None:
                widget.set_live(False)
        for buttons in self.signal_buttons.values():
            for btn_id in buttons.values():
                btn = self.wid_safe(btn_id)
                if btn is not None:
                    btn.set_live(False)
                    btn.disabled = True
        for buttons in self.decision_buttons.values():
            for btn_id in buttons.values():
                btn = self.wid_safe(btn_id)
                if btn is not None:
                    btn.set_live(False)
                    btn.disabled = True

        if not phase_state.show_showdown:
            self.refresh_center_cards(reveal=False)

        start_active = phase_state.start_active
        if self.fixation_running:
            start_active = False
        ready = phase_state.ready
        btn_start_p1 = self.wid_safe('btn_start_p1')
        btn_start_p2 = self.wid_safe('btn_start_p2')
        if btn_start_p1 is not None:
            btn_start_p1.set_live(start_active and ready)
        if btn_start_p2 is not None:
            btn_start_p2.set_live(start_active and ready)

        if ready:
            for player, cards in phase_state.active_cards.items():
                for which in cards:
                    widget = self.card_widget_for_player(player, which)
                    if widget is not None:
                        widget.set_live(True)
            for player, levels in phase_state.active_signal_buttons.items():
                for level in levels:
                    btn_id = self.signal_buttons.get(player, {}).get(level)
                    btn = self.wid_safe(btn_id) if btn_id else None
                    if btn is not None:
                        btn.set_live(True)
                        btn.disabled = False
            for player, decisions in phase_state.active_decision_buttons.items():
                for decision in decisions:
                    btn_id = self.decision_buttons.get(player, {}).get(decision)
                    btn = self.wid_safe(btn_id) if btn_id else None
                    if btn is not None:
                        btn.set_live(True)
                        btn.disabled = False

        if phase_state.show_showdown:
            if btn_start_p1 is not None:
                btn_start_p1.set_live(True)
            if btn_start_p2 is not None:
                btn_start_p2.set_live(True)
            self.update_showdown()

        round_badge = self.wid_safe('round_badge')
        if round_badge is not None:
            round_badge.text = ''
        self.update_user_displays()
        self.update_pause_overlay()

    def continue_after_start_press(self):
        result = self.controller.continue_after_start_press()
        if result.blocked:
            return
        if result.intro_deactivated:
            self.update_user_displays()
            self.update_intro_overlay()

        def proceed():
            if result.await_second_start:
                self.apply_phase()
                return
            self.log_round_start_if_pending()
            self.apply_phase()

        if result.requires_fixation and not self.fixation_running:
            self.run_fixation_sequence(proceed)
        else:
            proceed()

    def start_pressed(self, who:int):
        started = time.perf_counter()
        try:
            if not self._input_debouncer.allow(f"start:{who}", interval_override_ms=10.0):
                return
            if self.session_finished and not self.in_block_pause:
                return
            allowed_phase = self.phase in (UXPhase.WAIT_BOTH_START, UXPhase.SHOWDOWN)
            if not allowed_phase and not self.in_block_pause:
                return
            if who == 1:
                self.p1_pressed = True
            else:
                self.p2_pressed = True
            self.record_action(who, 'Play gedrückt')
            if self.session_configured:
                action = 'start_click' if self.phase == UXPhase.WAIT_BOTH_START else 'next_round_click'
                self.log_event(who, action)
            if self.p1_pressed and self.p2_pressed:
                # in nächste Phase
                self.p1_pressed = False
                self.p2_pressed = False
                if self.in_round_pause:
                    self.in_round_pause = False
                    self.pause_message = ''
                    self.update_pause_overlay()
                    self.log_round_start_if_pending()
                    self.prepare_next_round(start_immediately=True)
                    return
                if self.in_block_pause:
                    self.in_block_pause = False
                    self.pause_message = ''
                    self.update_pause_overlay()
                    self.setup_round()
                    if self.session_finished:
                        self.apply_phase()
                        return
                    self.phase = UXPhase.WAIT_BOTH_START
                    self.apply_phase()
                    self.log_round_start_if_pending()
                    self.continue_after_start_press()
                    return
                elif self.phase == UXPhase.SHOWDOWN:
                    self.log_round_start_if_pending()
                    self.prepare_next_round(start_immediately=True)
                    return
                else:
                    self.log_round_start_if_pending()
                    self.continue_after_start_press()
        finally:
            self._record_handler_duration('start_pressed', started)

    def run_fixation_sequence(self, on_complete=None):
        self.fixation_runner(
            self,
            schedule_once=Clock.schedule_once,
            stop_image=FIX_STOP_IMAGE,
            live_image=FIX_LIVE_IMAGE,
            on_complete=on_complete,
            bridge=self._bridge,
            players=sorted(self._bridge_players) if self._bridge_players else None,
            player=self._bridge_player,
            session=self._bridge_session,
            block=self._bridge_block,
        )

    def play_fixation_tone(self):
        self.fixation_player(self)

    def tap_card(self, who:int, which:str):
        started = time.perf_counter()
        try:
            if not self._input_debouncer.allow(f"tap:{who}:{which}"):
                return
            result = self.controller.tap_card(who, which)
            button_name = f'card_{which}'
            self._emit_button_bridge_event(
                button_name,
                player=who,
                extra={
                    'allowed': bool(result.allowed),
                    'card_slot': which,
                },
            )
            if not result.allowed:
                return
            widget = self.card_widget_for_player(who, which)
            if widget is None:
                return
            widget.flip()
            if result.record_text:
                self.record_action(who, result.record_text)
            if result.log_action:
                self.log_event(who, result.log_action, result.log_payload or {})
            if result.next_phase:
                Clock.schedule_once(lambda *_: self.goto(result.next_phase), 0.2)
        finally:
            self._record_handler_duration('tap_card', started)

    def pick_signal(self, player:int, level:str):
        started = time.perf_counter()
        try:
            if not self._input_debouncer.allow(f"signal:{player}:{level}"):
                return
            result = self.controller.pick_signal(player, level)
            self._emit_button_bridge_event(
                f'signal_{level}',
                player=player,
                extra={
                    'accepted': bool(result.accepted),
                    'signal_level': level,
                },
            )
            if not result.accepted:
                return
            for lvl, btn_id in self.signal_buttons.get(player, {}).items():
                btn = self.wid_safe(btn_id)
                if btn is None:
                    continue
                if lvl == level:
                    btn.set_pressed_state()
                else:
                    btn.set_live(False)
                    btn.disabled = True
            self.record_action(player, f'Signal gewählt: {self.describe_level(level)}')
            if result.log_payload:
                self.log_event(player, 'signal_choice', result.log_payload)
            self.update_user_displays()
            if result.next_phase:
                Clock.schedule_once(lambda *_: self.goto(result.next_phase), 0.2)
                self.update_user_displays()
        finally:
            self._record_handler_duration('pick_signal', started)

    def pick_decision(self, player:int, decision:str):
        started = time.perf_counter()
        try:
            if not self._input_debouncer.allow(f"decision:{player}:{decision}"):
                return
            result = self.controller.pick_decision(player, decision)
            self._emit_button_bridge_event(
                f'decision_{decision}',
                player=player,
                extra={
                    'accepted': bool(result.accepted),
                    'decision': decision,
                },
            )
            if not result.accepted:
                return
            for choice, btn_id in self.decision_buttons.get(player, {}).items():
                btn = self.wid_safe(btn_id)
                if btn is None:
                    continue
                if choice == decision:
                    btn.set_pressed_state()
                else:
                    btn.set_live(False)
                    btn.disabled = True
            self.record_action(player, f'Entscheidung: {decision.upper()}')
            if result.log_payload:
                self.log_event(player, 'call_choice', result.log_payload)
            self.update_user_displays()
            if result.next_phase:
                Clock.schedule_once(lambda *_: self.goto(result.next_phase), 0.2)
                self.update_user_displays()
        finally:
            self._record_handler_duration('pick_decision', started)

    def goto(self, phase):
        self.phase = phase
        self.apply_phase()

    def prepare_next_round(self, start_immediately: bool = False):
        result = self.controller.prepare_next_round(start_immediately=start_immediately)
        self.update_role_assignments()
        self._apply_round_setup(result.setup)
        self.apply_phase()
        if result.session_finished:
            message = self.controller.state.pause_message or (
                'Vielen Dank die Teilnahme! Das Experiment ist nun beendet!'
            )
            self.pause_message = message
            self.update_pause_overlay()
            self.update_user_displays()
            return
        if result.in_block_pause:
            self.in_block_pause = True
            self.pause_message = self.controller.state.pause_message or (
                "Dieser Block ist vorbei. Nehmen Sie sich einen Moment zum Durchatmen.\n"
                "Wenn Sie bereit sind, klicken Sie auf Weiter."
            )
            self.update_pause_overlay()
            self.update_user_displays()
            return

        def start_round():
            if result.start_phase:
                self.phase = result.start_phase
            self.log_round_start_if_pending()
            self.apply_phase()

        def after_fixation():
            if result.await_second_start:
                self.apply_phase()
            else:
                start_round()

        if result.requires_fixation and not self.fixation_running:
            self.run_fixation_sequence(after_fixation)
        elif start_immediately:
            start_round()

    def setup_round(self):
        result = self.controller.setup_round()
        self._apply_round_setup(result)

    def _apply_round_setup(self, result):
        plan = result.plan if result else None
        self.set_cards_from_plan(plan)
        for card_id in ('p1_inner', 'p1_outer', 'p2_inner', 'p2_outer'):
            widget = self.wid_safe(card_id)
            if widget is not None:
                widget.reset()
        for buttons in self.signal_buttons.values():
            for btn_id in buttons.values():
                btn = self.wid_safe(btn_id)
                if btn is not None:
                    btn.reset()
        for buttons in self.decision_buttons.values():
            for btn_id in buttons.values():
                btn = self.wid_safe(btn_id)
                if btn is not None:
                    btn.reset()
        self.status_lines = {1: [], 2: []}
        self.update_status_label(1)
        self.update_status_label(2)
        self.refresh_center_cards(reveal=False)
        self.update_user_displays()
        self._mark_bridge_dirty()
        self._ensure_bridge_recordings()

    def refresh_center_cards(self, reveal: bool):
        if reveal:
            p1_inner = self.wid_safe('p1_inner')
            p1_outer = self.wid_safe('p1_outer')
            p2_inner = self.wid_safe('p2_inner')
            p2_outer = self.wid_safe('p2_outer')
            sources = {
                1: [
                    p1_inner.front_image if p1_inner is not None else None,
                    p1_outer.front_image if p1_outer is not None else None,
                ],
                2: [
                    p2_inner.front_image if p2_inner is not None else None,
                    p2_outer.front_image if p2_outer is not None else None,
                ],
            }
        else:
            back = ASSETS['cards']['back']
            sources = {1: [back, back], 2: [back, back]}

        for player, imgs in self.center_cards.items():
            for idx, img_id in enumerate(imgs):
                img_widget = self.wid_safe(img_id)
                if img_widget is not None:
                    img_widget.source = sources[player][idx]
                    img_widget.opacity = 1

    def update_showdown(self):
        # Karten in der Mitte anzeigen
        self.refresh_center_cards(reveal=True)
        outcome = self.compute_outcome()
        if (
            self.current_round_has_stake
            and self.score_state
            and not self.outcome_score_applied
        ):
            winner = outcome.get('winner') if outcome else None
            if winner in (1, 2):
                winner_role = self.role_by_physical.get(winner)
                if winner_role in (1, 2):
                    self.score_state[winner_role] += POINTS_PER_WIN
                    self.outcome_score_applied = True
        if self.session_configured:
            self.log_event(None, 'showdown', outcome or {})
        self.update_user_displays()

    def card_value_from_path(self, path: str):
        if not path:
            return None
        name = os.path.basename(path)
        digits = ''.join(ch for ch in name if ch.isdigit())
        if not digits:
            return None
        try:
            return int(digits)
        except ValueError:
            return None

    def determine_signal_level(self, player: int):
        value = self.get_hand_value_for_player(player)
        return self.signal_level_from_value(value)

    def compute_outcome(self):
        outcome = self.controller.compute_outcome(
            signaler_total=self.get_hand_total_for_player(self.signaler),
            judge_total=self.get_hand_total_for_player(self.judge),
            signaler_value=self.get_hand_value_for_player(self.signaler),
            judge_value=self.get_hand_value_for_player(self.judge),
            level_from_value=self.signal_level_from_value,
        )
        return outcome

    def player_descriptor(self, player: int) -> str:
        role = self.role_by_physical.get(player)
        if role in (1, 2):
            return f'VP {role} – Spieler {player}'
        return f'Spieler {player}'

    def format_signal_choice(self, level: str):
        mapping = {
            'low': 'Tief',
            'mid': 'Mittel',
            'high': 'Hoch',
        }
        return mapping.get(level)

    def format_decision_choice(self, decision: str):
        mapping = {
            'wahr': 'Wahrheit',
            'bluff': 'Bluff',
        }
        return mapping.get(decision)
    def _result_signal_text(self, truthful: bool | None) -> str:
        if truthful is None:
            return 'Signal: -'
        return 'Signal: Wahr' if truthful else 'Signal: Bluff'

    def _result_judge_text(self, judge_ok: bool | None) -> str:
        if judge_ok is None:
            return 'Urteil: -'
        return 'Urteil: korrekt' if judge_ok else 'Urteil: inkorrekt'

    def _outcome_statement(self, truthful: bool | None, judge_choice: str | None) -> str:
        if truthful is None or judge_choice not in ('wahr', 'bluff'):
            return ''
        key = ('wahr' if truthful else 'bluff', judge_choice)
        mapping = {
            ('bluff', 'wahr'): 'Sp2 wurde getäuscht:',
            ('bluff', 'bluff'): 'Sp2 erkennt den Bluff:',
            ('wahr', 'wahr'): 'Showdown:',
            ('wahr', 'bluff'): 'SP1 ist ehrlich:',
        }
        return mapping.get(key, '')

    def _signal_label_german(self, level: str | None):
        return self.format_signal_choice(level) or '-'

    def _urteil_label_german(self, decision: str | None):
        return self.format_decision_choice(decision) or '-'

    def _judge_correct(self, truthful: bool | None, judge_choice: str | None):
        if truthful is None or judge_choice is None:
            return None
        expected = 'wahr' if truthful else 'bluff'
        return (judge_choice == expected)

    def _vp_for_player(self, player:int):
        vp = self.role_by_physical.get(player)
        return vp if vp in (1,2) else None

    def _result_for_vp(self, vp:int):
        """Gewonnen/Verloren/Unentschieden relativ zu VP1/VP2."""
        if not isinstance(self.last_outcome, dict):
            return ''
        winner_player = self.last_outcome.get('winner') if self.last_outcome else None
        if winner_player not in (1,2):
            if self.last_outcome.get('draw'):
                return 'Unentschieden'
            return ''
        winner_vp = self.role_by_physical.get(winner_player)
        if winner_vp == vp:
            return 'Gewonnen'
        return 'Verloren'

    def _result_with_score_for_vp(self, vp:int):
        base = self._result_for_vp(vp)
        if not base:
            return ''
        if base == 'Unentschieden':
            return 'Unentschieden 0'
        if base == 'Gewonnen':
            return f'Gewonnen +{POINTS_PER_WIN}'
        return 'Verloren 0'

    def _points_for_vp(self, vp:int):
        if not self.score_state:
            return None
        return self.score_state.get(vp)

    def format_user_display_text(self, vp:int):
        """Erzeugt den Text fürs Display gemäß Block (1/3 vs. 2/4)."""
        if self.intro_active:
            return ''
        # Runde im Block / total (Blockgröße variabel, Übung ohne Logging)
        total_rounds = max(1, self.current_block_total_rounds or 16)
        rnd_in_block = self.round_in_block or 1
        rnd_display = min(max(1, rnd_in_block), total_rounds)
        block_suffix = ' (Übung)' if self.is_practice_block_active() else ''
        header_round = f'Runde {rnd_display}/{total_rounds}{block_suffix}'

        # Zuordnung VP -> Spieler
        player = self.physical_by_role.get(vp)
        role_number = self.player_roles.get(player) if player in (1, 2) else None
        if role_number in (1, 2):
            header_role = f'VP {vp}: Spieler {role_number}'
        elif player in (1, 2):
            header_role = f'VP {vp}: Spieler {player}'
        else:
            header_role = f'VP {vp}'

        # Block-Logik
        block_idx = self.current_block_info['index'] if self.current_block_info else None
        with_points = bool(self.current_round_has_stake) and block_idx in (2,4)

        # Signal & Urteil (global – beziehen sich auf aktuelle Runde)
        signal_choice = self.last_outcome.get('signal_choice') if self.last_outcome else self.player_signals.get(self.signaler)
        judge_choice = self.last_outcome.get('judge_choice') if self.last_outcome else self.player_decisions.get(self.judge)

        truthful = self.last_outcome.get('truthful') if self.last_outcome else None
        judge_ok = self._judge_correct(truthful, judge_choice)

        signal_line = f"Signal: {self._signal_label_german(signal_choice)}"
        urteil_line = f"Urteil: {self._urteil_label_german(judge_choice)}"
        ergebnis_signal = self._result_signal_text(truthful)
        ergebnis_urteil = self._result_judge_text(judge_ok)
        outcome_statement = self._outcome_statement(truthful, judge_choice)

        if with_points:
            points = self._points_for_vp(vp)
            punkte = f' | Punkte: {points}' if points is not None else ''
            header = f'{header_round} | {header_role}{punkte}'
            result_line = self._result_with_score_for_vp(vp)
        else:
            header = f'{header_round} | {header_role}'
            result_line = self._result_for_vp(vp)

        column_width = 34

        def pad_column(text: str) -> str:
            padded = f"{text:<{column_width}}"
            return padded.replace(' ', '\u00A0')

        header_row = f"[b]{pad_column('Züge')}[/b][b]Ergebnis[/b]"
        move_rows = [
            f"{pad_column(signal_line)}{ergebnis_signal}",
            f"{pad_column(urteil_line)}{ergebnis_urteil}",
        ]

        lines = [
            f"[b]{header}[/b]",
            '',
            header_row,
            *move_rows,
        ]
        if outcome_statement:
            lines.extend(['', outcome_statement])
        if result_line and result_line.strip():
            lines.append(f"[b]{result_line}[/b]")

        # Mehrzeilig – leichte Abstände über \n
        return "\n".join(lines)

    def update_user_displays(self):
        """Setzt die Texte in den beiden Displays (unten=VP1, oben=VP2)."""
        for vp, display_id in self.user_displays.items():
            display = self.wid_safe(display_id)
            if display is not None:
                display.text = self.format_user_display_text(vp)

    def update_pause_overlay(self):
        pause_cover = self.wid_safe('pause_cover')
        if pause_cover is None:
            return
        active = self.in_block_pause or self.session_finished
        buttons_active = self.in_block_pause
        if active:
            # Ensure pause_cover is the topmost regular widget
            if pause_cover.parent is None:
                self.add_widget(pause_cover)
            else:
                parent = pause_cover.parent
                try:
                    parent.remove_widget(pause_cover)
                except Exception:
                    pass
                parent.add_widget(pause_cover)

            # Start buttons should be above the overlay
            self.bring_start_buttons_to_front()
            pause_cover.opacity = 1
            pause_cover.disabled = False
            for label_id in self.pause_labels.values():
                lbl = self.wid_safe(label_id)
                if lbl is not None:
                    lbl.text = self.pause_message or ''
            for player, btn_id in getattr(self, 'pause_start_buttons', {}).items():
                btn = self.wid_safe(btn_id)
                if btn is None:
                    continue
                btn.opacity = 1 if buttons_active else 0
                btn.disabled = not buttons_active
                btn.set_live(buttons_active)
        else:
            pause_cover.opacity = 0
            pause_cover.disabled = True
            for label_id in self.pause_labels.values():
                lbl = self.wid_safe(label_id)
                if lbl is not None:
                    lbl.text = ''
            for btn_id in getattr(self, 'pause_start_buttons', {}).values():
                btn = self.wid_safe(btn_id)
                if btn is None:
                    continue
                btn.opacity = 0
                btn.disabled = True
                btn.set_live(False)
            if pause_cover.parent is not None:
                self.remove_widget(pause_cover)
            # Keep start buttons order consistent
            self.bring_start_buttons_to_front()

    def build_round_pause_message(self, next_info: Optional[Dict[str, Any]]) -> str:
        base = (
            "Pause. Atmen Sie kurz durch, wenn Sie bereit für die nächste Runde sind, "
            "spielen Sie weiter."
        )
        if not next_info:
            return base
        block = next_info.get('block') or {}
        payout = block.get('payout')
        if payout:
            suffix = 'In der nächsten Runde spielen Sie um Punkte und Lose.'
        else:
            suffix = 'In der nächsten Runde spielen Sie zum Spaß.'
        return f"{base}\n{suffix}"

    def describe_level(self, level:str) -> str:
        return self.format_signal_choice(level) or (level or '-')

    def is_practice_block_active(self) -> bool:
        return bool(self.current_block_info and self.current_block_info.get('practice'))

    def choice_labels_for_vp(self, vp: int):
        physical = self.physical_by_role.get(vp)
        if physical not in (1, 2):
            return (None, None)
        if physical == self.signaler:
            return 'Eigenes Signal', 'Anderes Urteil'
        if physical == self.judge:
            return 'Eigenes Urteil', 'Anderes Signal'
        return (None, None)

    def update_role_assignments(self):
        """Stelle sicher, dass die Versuchspersonen fest ihren Sitzplätzen zugeordnet bleiben."""
        # Die Sitzordnung ist fix: Spieler 1 unten = VP1, Spieler 2 oben = VP2.
        # Rollenwechsel (Signaler/Judge) wird separat über self.signaler/self.judge abgebildet.
        self.role_by_physical = self._fixed_role_mapping.copy()
        self.physical_by_role = {role: player for player, role in self.role_by_physical.items()}

    def update_turn_order(self):
        self.controller.update_turn_order()

    def phase_for_player(self, player: int, which: str):
        return self.controller.phase_for_player(player, which)

    def card_widget_for_player(self, player: int, which: str):
        if player == 1:
            if which == 'inner':
                return self.wid_safe('p1_inner')
            if which == 'outer':
                return self.wid_safe('p1_outer')
        elif player == 2:
            if which == 'inner':
                return self.wid_safe('p2_inner')
            if which == 'outer':
                return self.wid_safe('p2_outer')
        return None

    def current_engine_phase(self):
        return to_engine_phase(self.phase)

    def _actor_label(self, player: Optional[int]) -> str:
        if player not in (1, 2):
            return "SYS"
        role = None
        try:
            role = self.player_roles.get(player)
        except Exception:
            role = None
        if role == 1:
            return "P1"
        if role == 2:
            return "P2"
        return "P1" if player == 1 else "P2"

    def log_event(self, player: int, action: str, payload=None):
        if not self.logger or not self.session_configured:
            return
        payload = payload or {}
        actor = self._actor_label(player)
        round_idx = max(0, self.round - 1)
        self.logger.log(
            round_idx,
            self.current_engine_phase(),
            actor,
            action,
            payload
        )
        write_round_log(self, actor, action, payload, player)
        bridge_payload = {
            "actor": actor,
            "game_player": player,
            "phase": self.current_engine_phase(),
            "round_index": round_idx,
            "payload": payload,
        }
        if player in (1, 2):
            role_value = self.player_roles.get(player)
            if role_value is not None:
                bridge_payload["player_role"] = role_value
        if self.marker_bridge and action != 'round_start':
            # non-blocking: moved bridge send to async enqueue
            self.marker_bridge.enqueue(f"action.{action}", bridge_payload)

    def prompt_session_number(self):
        if self.session_popup:
            return

        content = BoxLayout(orientation='vertical', spacing=12, padding=12)

        header = Label(text='Bitte Session ID eingeben:', size_hint_y=None, height='32dp')
        session_input = TextInput(
            hint_text='Session ID',
            multiline=False,
            size_hint_y=None,
            height='40dp'
        )

        row1 = BoxLayout(size_hint_y=None, height='40dp', spacing=8)
        row1.add_widget(Label(text='Aruco-Overlay aktivieren'))
        overlay_switch = Switch(active=self.aruco_enabled)
        row1.add_widget(overlay_switch)

        row2 = BoxLayout(size_hint_y=None, height='40dp', spacing=8)
        row2.add_widget(Label(text='Startblock (1=Übung, 2–5=Experimental)'))
        block_spinner = Spinner(
            text=str(self.start_block),
            values=[str(i) for i in range(1, 6)],
            size_hint=(None, None),
            size=('120dp', '40dp')
        )
        row2.add_widget(block_spinner)

        error_label = Label(text='', color=(1, 0, 0, 1), size_hint_y=None, height='24dp')

        buttons = BoxLayout(size_hint_y=None, height='44dp', spacing=8)
        ok_button = Button(text='OK')
        cancel_button = Button(text='Abbrechen')
        buttons.add_widget(ok_button)
        buttons.add_widget(cancel_button)

        content.add_widget(header)
        content.add_widget(session_input)
        content.add_widget(row1)
        content.add_widget(row2)
        content.add_widget(error_label)
        content.add_widget(buttons)

        popup = Popup(
            title='Session starten',
            content=content,
            size_hint=(0.6, 0.6),
            auto_dismiss=False
        )
        self.session_popup = popup

        def _on_ok(_btn):
            session_text = session_input.text.strip()
            if not session_text:
                error_label.text = 'Bitte Session ID eingeben.'
                return

            try:
                start_block_choice = int(block_spinner.text)
            except Exception:
                start_block_choice = 1
            start_block_choice = self._clamp_start_block_choice(start_block_choice)
            aruco_active = bool(overlay_switch.active)

            popup.dismiss()
            self.session_popup = None

            self._finalize_session_setup(
                session_text,
                start_block_value=start_block_choice,
                aruco_enabled=aruco_active,
            )

        def _on_cancel(_btn):
            popup.dismiss()
            self.session_popup = None

        ok_button.bind(on_release=_on_ok)
        cancel_button.bind(on_release=_on_cancel)
        popup.open()

    def _start_overlay_with_path(self, process: Optional[Any]) -> Optional[Any]:
        """Start the ArUco overlay process with the relocated script path."""

        try:
            return self.start_overlay(
                process,
                overlay_path=ARUCO_OVERLAY_PATH,
                display_index=self.overlay_display_index,
            )
        except TypeError:
            return self.start_overlay(process)

    def _apply_session_options_and_start(self):
        if self._aruco_proc is None and getattr(self, 'overlay_process', None):
            self._aruco_proc = self.overlay_process

        if self.aruco_enabled:
            self._aruco_proc = self._start_overlay_with_path(self._aruco_proc)
        else:
            self._aruco_proc = self.stop_overlay(self._aruco_proc)
        self.overlay_process = self._aruco_proc

        if not self._blocks:
            self._blocks = load_blocks()

        available_blocks = list(self._blocks) if self._blocks else []
        self._blocks = available_blocks
        if not available_blocks:
            self.blocks = []
            self.apply_phase()
            return

        start_index = max(0, min(len(available_blocks) - 1, self.start_block - 1))
        selected_blocks = available_blocks[start_index:]
        if not selected_blocks:
            selected_blocks = available_blocks[-1:]

        self.blocks = selected_blocks
        self.current_block_idx = 0
        self.current_round_idx = 0
        self.current_block_info = None
        self.round_in_block = 0
        self.current_block_total_rounds = 0
        self.session_finished = False
        self.in_block_pause = False
        self.pause_message = ''
        self.in_round_pause = False
        self.next_block_preview = None
        self.fixation_required = False
        self.pending_round_start_log = False
        self.round = 1
        self.outcome_score_applied = False
        self.score_state = None
        self.score_state_block = None
        self.score_state_round_start = None
        self.phase = UXPhase.WAIT_BOTH_START
        self.intro_active = True
        self.p1_pressed = False
        self.p2_pressed = False

        self.total_rounds_planned = sum(
            len(block.get('rounds') or []) for block in self.blocks
        )

        self.reset_ui_for_new_block()

    def reset_ui_for_new_block(self):
        self.setup_round()
        self.apply_phase()
        self.update_user_displays()
        self.update_intro_overlay()

    def log_round_start(self):
        if not self.session_configured:
            return
        self.log_event(None, 'round_start', {
            'round': self.round,
            'block': self.current_block_info['index'] if self.current_block_info else None,
            'round_in_block': self.round_in_block if self.current_block_info else None,
            'payout': bool(self.current_round_has_stake),
            'signaler': self.signaler,
            'judge': self.judge,
            'vp_roles': self.role_by_physical.copy(),
            'player_roles': self.player_roles.copy(),
        })
        self.pending_round_start_log = False

    def log_round_start_if_pending(self):
        if self.pending_round_start_log:
            self.log_round_start()

    def record_action(self, player:int, text:str):
        self.status_lines[player].append(text)
        self.update_status_label(player)

    def update_status_label(self, player:int):
        label = self.status_labels.get(player)
        if label is None:
            return
        role = 'Signal' if self.signaler == player else 'Judge'
        header = [f"Du bist Spieler {player}", f"Rolle: {role}"]
        body = self.status_lines[player]
        self.status_labels[player].text = "\n".join(header + body)


__all__ = ["TabletopRoot"]
