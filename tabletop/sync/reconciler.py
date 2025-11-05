"""Background reconciliation of UI event timestamps with device timelines."""

from __future__ import annotations

import logging
import math
import queue
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

from tabletop.engine import EventLogger
from tabletop.pupil_bridge import PupilBridge

log = logging.getLogger(__name__)

RECENCY_TAU_SECONDS = 180.0
SLOPE_FREEZE_CONFIDENCE = 0.90
SLOPE_FREEZE_PPM = 20e-6
SLOPE_CLAMP_PPM = 50e-6
SLOPE_CLAMP_MIN = 1.0 - SLOPE_CLAMP_PPM
SLOPE_CLAMP_MAX = 1.0 + SLOPE_CLAMP_PPM
RMS_LOCK_THRESHOLD_MS = 2.0
SIGN_STABILITY_COUNT = 3
SIGN_WEIGHT_THRESHOLD_NS = 1_000_000.0
CONF_GATE_HIGH_RMS_MS = 3.0
CONF_GATE_LOW_RMS_MS = 1.0
CONF_GATE_HIGH = 0.90
CONF_GATE_LOW = 0.70
CONF_GATE_DEFAULT = 0.80
MICRO_REFINE_HORIZON_S = 600.0
MICRO_REFINE_OFFSET_MS = 1.0
REREFINE_EVENT_WINDOW = 200
REREFINE_RATE_LIMIT_S = 120.0
REREFINE_RMS_IMPROVEMENT_MS = 1.0
REREFINE_CONF_IMPROVEMENT = 0.10

@dataclass
class _DeviceMarkerEvent:
    device_ns: int
    name: str
    payload: Dict[str, Any]
    ingest_ts: float


@dataclass
class _HostMirrorEvent:
    host_ns: int
    ingest_ts: float
    payload: Dict[str, Any]


@dataclass
class _PlayerState:
    """Calibration state for a single player/device."""

    player: str
    samples: Deque[Tuple[int, int, float, float]] = field(default_factory=deque)
    raw_offsets: Deque[Tuple[int, int, float, float]] = field(default_factory=deque)
    intercept_ns: float = 0.0
    slope: float = 1.0
    rms_ns: float = 0.0
    confidence: float = 0.0
    mapping_version: int = 0
    sample_count: int = 0
    offset_sign: int = 1
    last_update: float = field(default_factory=time.monotonic)
    offset_sign_locked: bool = False
    sign_streak: int = 0
    rms_history_ms: Deque[float] = field(
        default_factory=lambda: deque(maxlen=SIGN_STABILITY_COUNT)
    )
    mad_ns: float = 0.0
    slope_raw: float = 1.0
    slope_mode: str = "free"
    weights_last3: Tuple[float, float, float] = (1.0, 1.0, 1.0)
    confidence_gate: float = CONF_GATE_DEFAULT
    last_rerefine_ts: float = 0.0
    weighted_offset_sign: int = 0


@dataclass
class _FitMetrics:
    intercept_ns: float
    intercept_raw_ns: float
    slope_raw: float
    slope_applied: float
    rms_ns: float
    mad_ns: float
    weights: List[float]
    slope_mode: str


class TimeReconciler:
    """Continuously estimates clock offsets and refines provisional events."""

    CONF_MIN = 0.8

    def __init__(
        self,
        bridge: PupilBridge,
        logger: EventLogger,
        window_size: int = 10,
        *,
        micro_refine_horizon_s: float = MICRO_REFINE_HORIZON_S,
        micro_refine_offset_ms: float = MICRO_REFINE_OFFSET_MS,
        marker_pair_weight: float = 2.5,
        pending_pair_limit: int = 200,
    ) -> None:
        self._bridge = bridge
        self._logger = logger
        self._window_size = max(3, int(window_size))
        self._state_lock = threading.Lock()
        self._player_states: Dict[str, _PlayerState] = {}
        self._task_queue: queue.Queue[Tuple[str, Tuple[Any, ...]]] = queue.Queue(
            maxsize=2000
        )
        self._queue_drop = 0
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._known_events: Dict[str, Tuple[int, Dict[str, int]]] = {}
        self._event_order: Deque[str] = deque()
        self._event_retention = max(2000, self._window_size * 200)
        self._heartbeat_count = 0
        self._intercept_epsilon_ns = 5_000.0
        self._slope_epsilon = 5e-6
        self._huber_delta_ns = 5_000_000.0
        self._recency_tau = RECENCY_TAU_SECONDS
        self._micro_refine_horizon_s = float(micro_refine_horizon_s)
        self._micro_refine_offset_ms = float(micro_refine_offset_ms)
        self._micro_refine_slope_max = self._slope_epsilon
        self._rerefine_window = REREFINE_EVENT_WINDOW
        self._rerefine_rate_limit_s = REREFINE_RATE_LIMIT_S
        self._marker_pair_weight = max(0.5, float(marker_pair_weight))
        self._pending_pair_limit = max(1, int(pending_pair_limit))
        self._pending_device_markers: Dict[str, Dict[str, _DeviceMarkerEvent]] = {}
        self._pending_host_mirrors: Dict[str, Dict[str, _HostMirrorEvent]] = {}

    # ------------------------------------------------------------------
    @property
    def current_mapping_version(self) -> int:
        with self._state_lock:
            if not self._player_states:
                return 0
            return max(state.mapping_version for state in self._player_states.values())

    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker = threading.Thread(
            target=self._run,
            name="TimeReconciler",
            daemon=True,
        )
        self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._enqueue("stop")
        if self._worker is not None:
            self._worker.join(timeout=1.0)
            self._worker = None

    # ------------------------------------------------------------------
    def submit_marker(self, label: str, t_local_ns: int) -> None:
        self._enqueue("marker", label, int(t_local_ns))

    def on_event(self, event_id: str, t_local_ns: int) -> None:
        self._enqueue("event", event_id, int(t_local_ns))

    def register_device_event(
        self,
        player: str,
        name: str,
        t_device_ns: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._enqueue(
            "device_event",
            player,
            name,
            int(t_device_ns),
            dict(payload or {}),
        )

    # ------------------------------------------------------------------
    def _enqueue(self, kind: str, *args: Any) -> None:
        try:
            self._task_queue.put_nowait((kind, args))
        except queue.Full:
            self._queue_drop += 1
            log.warning(
                "TimeReconciler queue full – dropping %s (%d drops)",
                kind,
                self._queue_drop,
            )

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                kind, args = self._task_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "stop":
                break
            try:
                if kind == "marker":
                    self._process_marker(str(args[0]), int(args[1]))
                elif kind == "event":
                    self._process_event(str(args[0]), int(args[1]))
                elif kind == "rerefine":
                    self._perform_rerefine(str(args[0]))
                elif kind == "device_event":
                    payload = dict(args[3]) if len(args) > 3 else {}
                    self._process_device_event(
                        str(args[0]),
                        str(args[1]),
                        int(args[2]),
                        payload,
                    )
            except Exception:
                log.exception("Error processing %s task", kind)
            finally:
                self._task_queue.task_done()
        # Drain remaining items gracefully
        while True:
            try:
                kind, args = self._task_queue.get_nowait()
            except queue.Empty:
                break
            if kind in {"marker", "event"}:
                try:
                    if kind == "marker":
                        self._process_marker(str(args[0]), int(args[1]))
                    elif kind == "event":
                        self._process_event(str(args[0]), int(args[1]))
                    elif kind == "rerefine":
                        self._perform_rerefine(str(args[0]))
                    elif kind == "device_event":
                        payload = dict(args[3]) if len(args) > 3 else {}
                        self._process_device_event(
                            str(args[0]),
                            str(args[1]),
                            int(args[2]),
                            payload,
                        )
                except Exception:
                    log.exception("Error processing %s during shutdown", kind)
            self._task_queue.task_done()

    # ------------------------------------------------------------------
    def _process_marker(self, label: str, t_local_ns: int) -> None:
        players = self._connected_players_snapshot()
        if not players:
            return
        offsets_ns: Dict[str, int] = {}
        for player in players:
            try:
                offset_seconds = self._bridge.estimate_time_offset(player)
            except Exception:
                log.debug("estimate_time_offset failed for %s", player, exc_info=True)
                continue
            if offset_seconds is None:
                continue
            offsets_ns[player] = int(offset_seconds * 1_000_000_000)
        if not offsets_ns:
            return

        self._heartbeat_count += 1

        for player, offset_ns in offsets_ns.items():
            self._ingest_marker(player, t_local_ns, offset_ns)

        log.debug(
            "Sync marker %s processed (hb=%d, players=%d)",
            label,
            self._heartbeat_count,
            len(offsets_ns),
        )

    def _process_device_event(
        self,
        player: str,
        name: str,
        t_device_ns: int,
        payload: Dict[str, Any],
    ) -> None:
        event_id_raw = payload.get("event_id") or payload.get("id")
        if not event_id_raw:
            return
        event_id = str(event_id_raw)
        now = time.monotonic()
        if name == "sync.host_ns":
            host_candidate = payload.get("t_host_ns", payload.get("t_local_ns"))
            try:
                host_ns = int(host_candidate)
            except Exception:
                return
            with self._state_lock:
                device_events = self._pending_device_markers.setdefault(player, {})
                device_entry = device_events.pop(event_id, None)
                if device_entry is None:
                    mirrors = self._pending_host_mirrors.setdefault(player, {})
                    mirrors[event_id] = _HostMirrorEvent(
                        host_ns=host_ns,
                        ingest_ts=now,
                        payload=dict(payload),
                    )
                    self._prune_pending(mirrors)
                    return
            ingest_ts = min(now, device_entry.ingest_ts)
            self._ingest_sync_pair(
                player,
                host_ns,
                device_entry.device_ns,
                weight=self._marker_pair_weight,
                ingest_ts=ingest_ts,
            )
        elif name.startswith(("sync.", "fix.")):
            with self._state_lock:
                mirrors = self._pending_host_mirrors.setdefault(player, {})
                mirror_entry = mirrors.pop(event_id, None)
                if mirror_entry is None:
                    device_events = self._pending_device_markers.setdefault(player, {})
                    device_events[event_id] = _DeviceMarkerEvent(
                        device_ns=int(t_device_ns),
                        name=name,
                        payload=dict(payload),
                        ingest_ts=now,
                    )
                    self._prune_pending(device_events)
                    return
            ingest_ts = min(now, mirror_entry.ingest_ts)
            self._ingest_sync_pair(
                player,
                mirror_entry.host_ns,
                int(t_device_ns),
                weight=self._marker_pair_weight,
                ingest_ts=ingest_ts,
            )

    def _ingest_sync_pair(
        self,
        player: str,
        t_host_ns: int,
        t_device_ns: int,
        *,
        weight: float,
        ingest_ts: Optional[float] = None,
    ) -> None:
        offset_ns = int(t_device_ns - t_host_ns)
        self._ingest_marker(
            player,
            int(t_host_ns),
            offset_ns,
            weight=weight,
            ingest_ts=ingest_ts,
        )

    def _prune_pending(self, mapping: Dict[str, Any]) -> None:
        while len(mapping) > self._pending_pair_limit:
            oldest_key = next(iter(mapping))
            mapping.pop(oldest_key, None)

    def _ingest_marker(
        self,
        player: str,
        t_local_ns: int,
        offset_ns: int,
        *,
        weight: float = 1.0,
        ingest_ts: Optional[float] = None,
    ) -> None:
        ingest_ts = ingest_ts if ingest_ts is not None else time.monotonic()
        sample_weight = max(float(weight), 0.001)
        with self._state_lock:
            state = self._player_states.get(player)
            if state is None:
                state = _PlayerState(
                    player=player,
                    samples=deque(maxlen=self._window_size),
                    raw_offsets=deque(maxlen=self._window_size),
                )
                self._player_states[player] = state
            elif state.samples.maxlen != self._window_size:
                state.samples = deque(state.samples, maxlen=self._window_size)
                state.raw_offsets = deque(state.raw_offsets, maxlen=self._window_size)
            raw_samples = list(state.raw_offsets)
            current_sign = state.offset_sign or 1
            current_intercept = state.intercept_ns
            current_slope = state.slope
            current_count = state.sample_count
            previous_samples = list(state.samples)

        raw_candidates = self._trimmed_raw_samples(
            raw_samples, (int(t_local_ns), int(offset_ns), ingest_ts, sample_weight)
        )

        candidate_pos = self._raw_to_samples(raw_candidates, 1)
        candidate_neg = self._raw_to_samples(raw_candidates, -1)

        pos_metrics = self._evaluate_candidate(candidate_pos)
        neg_metrics = self._evaluate_candidate(candidate_neg)

        chosen_sign = current_sign
        chosen_samples = candidate_pos
        chosen_metrics = pos_metrics

        offset_residual_pos = float("inf")
        offset_residual_neg = float("inf")
        if pos_metrics is not None:
            offset_residual_pos = self._offset_residual(
                raw_candidates,
                pos_metrics.intercept_raw_ns,
                pos_metrics.slope_raw,
                1,
            )
        if neg_metrics is not None:
            offset_residual_neg = self._offset_residual(
                raw_candidates,
                neg_metrics.intercept_raw_ns,
                neg_metrics.slope_raw,
                -1,
            )
        align_pos = (
            self._offset_alignment(
                raw_candidates,
                pos_metrics.intercept_raw_ns,
                pos_metrics.slope_raw,
                1,
            )
            if pos_metrics is not None
            else 0.0
        )
        align_neg = (
            self._offset_alignment(
                raw_candidates,
                neg_metrics.intercept_raw_ns,
                neg_metrics.slope_raw,
                -1,
            )
            if neg_metrics is not None
            else 0.0
        )
        if offset_residual_neg + 1e-3 < offset_residual_pos * 0.8:
            chosen_sign = -1
            chosen_samples = candidate_neg
            chosen_metrics = neg_metrics
        elif offset_residual_pos + 1e-3 < offset_residual_neg * 0.8:
            chosen_sign = 1
            chosen_samples = candidate_pos
            chosen_metrics = pos_metrics
        elif align_neg > 0 and align_pos < 0:
            chosen_sign = -1
            chosen_samples = candidate_neg
            chosen_metrics = neg_metrics
        elif align_pos > 0 and align_neg < 0:
            chosen_sign = 1
            chosen_samples = candidate_pos
            chosen_metrics = pos_metrics

        if current_count >= 2:
            predicted_ns = current_intercept + current_slope * t_local_ns
            pos_residual = abs(predicted_ns - candidate_pos[-1][1])
            neg_residual = abs(predicted_ns - candidate_neg[-1][1])
            if neg_residual + 1e-6 < pos_residual * 0.8:
                chosen_sign = -1
                chosen_samples = candidate_neg
                chosen_metrics = neg_metrics
            elif pos_residual + 1e-6 < neg_residual * 0.8:
                chosen_sign = 1
                chosen_samples = candidate_pos
                chosen_metrics = pos_metrics
            elif current_sign < 0 and neg_residual + 1e-6 < pos_residual * 1.05:
                chosen_sign = -1
                chosen_samples = candidate_neg
                chosen_metrics = neg_metrics
            elif current_sign > 0 and pos_residual + 1e-6 < neg_residual * 1.05:
                chosen_sign = 1
                chosen_samples = candidate_pos
                chosen_metrics = pos_metrics

        if pos_metrics is None and neg_metrics is not None:
            chosen_sign = -1
            chosen_samples = candidate_neg
            chosen_metrics = neg_metrics
        elif pos_metrics is not None and neg_metrics is not None:
            pos_rms = pos_metrics.rms_ns
            neg_rms = neg_metrics.rms_ns
            if neg_rms + 1e-9 < pos_rms * 0.8:
                chosen_sign = -1
                chosen_samples = candidate_neg
                chosen_metrics = neg_metrics
            elif pos_rms + 1e-9 < neg_rms * 0.8:
                chosen_sign = 1
                chosen_samples = candidate_pos
                chosen_metrics = pos_metrics
            else:
                chosen_sign = current_sign
                chosen_samples = candidate_pos if current_sign >= 0 else candidate_neg
                chosen_metrics = pos_metrics if current_sign >= 0 else neg_metrics
        elif pos_metrics is None and neg_metrics is None:
            chosen_sign = current_sign
            chosen_samples = candidate_pos if current_sign >= 0 else candidate_neg
            chosen_metrics = None

        # Recency-weighted sign hint based on raw offsets
        trend = 0
        if pos_metrics is not None or neg_metrics is not None:
            weights = self._recency_weights(raw_candidates)
            weighted_offset = 0.0
            for ( _, offset, _, sample_weight), rec_weight in zip(
                raw_candidates, weights
            ):
                combined = rec_weight * sample_weight
                weighted_offset += combined * offset
            if (
                chosen_metrics is not None
                and abs(weighted_offset) > SIGN_WEIGHT_THRESHOLD_NS
            ):
                trend = 1 if weighted_offset > 0 else -1
                preferred_sign = None
                if (
                    pos_metrics is not None
                    and weighted_offset * pos_metrics.intercept_raw_ns >= 0
                ):
                    preferred_sign = 1
                if (
                    neg_metrics is not None
                    and weighted_offset * neg_metrics.intercept_raw_ns >= 0
                ):
                    if preferred_sign is None:
                        preferred_sign = -1
                if (
                    preferred_sign is not None
                    and preferred_sign != chosen_sign
                    and (trend == 0 or preferred_sign == trend)
                ):
                    if preferred_sign > 0 and pos_metrics is not None:
                        chosen_sign = 1
                        chosen_samples = candidate_pos
                        chosen_metrics = pos_metrics
                    elif preferred_sign < 0 and neg_metrics is not None:
                        chosen_sign = -1
                        chosen_samples = candidate_neg
                        chosen_metrics = neg_metrics

        with self._state_lock:
            state = self._player_states[player]
            prev_sign = state.offset_sign or 1
            prev_locked = state.offset_sign_locked
            prev_history = list(state.rms_history_ms)
            prev_trend = state.weighted_offset_sign

            if (
                chosen_metrics is not None
                and trend != 0
                and prev_trend != 0
                and trend != prev_trend
            ):
                state.offset_sign_locked = False
                prev_locked = False
                state.sign_streak = 1
                log.info(
                    "offset trend change for %s (trend=%+d -> %+d)",
                    player,
                    prev_trend,
                    trend,
                )
                if trend > 0 and pos_metrics is not None:
                    chosen_sign = 1
                    chosen_samples = candidate_pos
                    chosen_metrics = pos_metrics
                elif trend < 0 and neg_metrics is not None:
                    chosen_sign = -1
                    chosen_samples = candidate_neg
                    chosen_metrics = neg_metrics

            if chosen_metrics is None:
                state.raw_offsets = deque(raw_candidates, maxlen=self._window_size)
                state.samples = deque(
                    chosen_samples if chosen_samples else previous_samples,
                    maxlen=self._window_size,
                )
                samples_list = list(state.samples)
                state.sample_count = len(state.samples)
                state.last_update = time.monotonic()
                log.debug("Accumulating samples for %s (insufficient data)", player)
            else:
                rms_ms = chosen_metrics.rms_ns / 1_000_000.0
                median_prev = (
                    statistics.median(prev_history) if prev_history else None
                )
                allow_switch = True
                if prev_locked:
                    baseline_ms = max(median_prev or 0.0, 0.1)
                    if rms_ms > 2.0 * baseline_ms:
                        state.offset_sign_locked = False
                        prev_locked = False
                        log.info(
                            "offset_sign unlock for %s (rms=%.3fms median=%.3fms)",
                            player,
                            rms_ms,
                            median_prev or 0.0,
                        )
                    else:
                        allow_switch = False
                if not allow_switch:
                    chosen_sign = prev_sign
                    fallback_samples = (
                        candidate_pos if prev_sign >= 0 else candidate_neg
                    )
                    chosen_samples = fallback_samples or previous_samples
                    fallback_metrics = (
                        pos_metrics if prev_sign >= 0 else neg_metrics
                    )
                    if fallback_metrics is not None:
                        chosen_metrics = fallback_metrics
                        rms_ms = chosen_metrics.rms_ns / 1_000_000.0
                    else:
                        chosen_metrics = None

                if chosen_metrics is None:
                    state.raw_offsets = deque(raw_candidates, maxlen=self._window_size)
                    state.samples = deque(
                        chosen_samples if chosen_samples else previous_samples,
                        maxlen=self._window_size,
                    )
                    samples_list = list(state.samples)
                    state.sample_count = len(state.samples)
                    state.last_update = time.monotonic()
                else:
                    if chosen_sign != prev_sign and allow_switch:
                        state.sign_streak = 1
                    else:
                        state.sign_streak = min(
                            state.sign_streak + 1, SIGN_STABILITY_COUNT
                        )
                    if (
                        not state.offset_sign_locked
                        and state.sign_streak >= SIGN_STABILITY_COUNT
                        and rms_ms < RMS_LOCK_THRESHOLD_MS
                    ):
                        state.offset_sign_locked = True
                        log.info(
                            "offset_sign lock for %s (sign=%+d rms=%.3fms)",
                            player,
                            chosen_sign,
                            rms_ms,
                        )
                    if chosen_sign != state.offset_sign and allow_switch:
                        log.warning(
                            "Offset semantics inverted for %s (sign=%+d)",
                            player,
                            chosen_sign,
                        )
                    state.offset_sign = chosen_sign
                    state.rms_history_ms.append(rms_ms)
                    state.raw_offsets = deque(raw_candidates, maxlen=self._window_size)
                    state.samples = deque(chosen_samples, maxlen=self._window_size)
                    samples_list = list(state.samples)
                    state.sample_count = len(state.samples)
                    state.last_update = time.monotonic()
                    log.debug(
                        "sign decision %s: offset_sign=%+d locked=%s rms=%.3fms",
                        player,
                        state.offset_sign,
                        state.offset_sign_locked,
                        rms_ms,
                    )

            if trend != 0:
                state.weighted_offset_sign = trend

        self._recompute_model_from_samples(player, samples_list)

    def _trimmed_raw_samples(
        self,
        base: list[Tuple[int, int, float, float]],
        sample: Tuple[int, int, float, float],
    ) -> list[Tuple[int, int, float, float]]:
        samples = list(base)
        samples.append(sample)
        if len(samples) > self._window_size:
            samples = samples[-self._window_size :]
        return samples

    def _raw_to_samples(
        self, raw: list[Tuple[int, int, float, float]], sign: int
    ) -> list[Tuple[int, int, float, float]]:
        result: list[Tuple[int, int, float, float]] = []
        for t_local_ns, offset_ns, ingest_ts, weight in raw:
            device_ns = t_local_ns + sign * offset_ns
            result.append((t_local_ns, device_ns, ingest_ts, weight))
        return result

    def _offset_residual(
        self,
        raw: list[Tuple[int, int, float, float]],
        intercept: float,
        slope: float,
        sign: int,
    ) -> float:
        if not raw:
            return float("inf")
        weighted_error = 0.0
        total_weight = 0.0
        for t_local_ns, measured_offset, _, weight in raw:
            predicted_device = intercept + slope * t_local_ns
            predicted_raw = sign * (predicted_device - t_local_ns)
            residual = predicted_raw - measured_offset
            weighted_error += weight * (residual ** 2)
            total_weight += weight
        if total_weight <= 0:
            return float("inf")
        mean_square = weighted_error / total_weight
        return math.sqrt(mean_square)

    def _offset_alignment(
        self,
        raw: list[Tuple[int, int, float, float]],
        intercept: float,
        slope: float,
        sign: int,
    ) -> float:
        if not raw:
            return 0.0
        alignment = 0.0
        total_weight = 0.0
        for t_local_ns, measured_offset, _, weight in raw:
            predicted_device = intercept + slope * t_local_ns
            predicted_raw = sign * (predicted_device - t_local_ns)
            alignment += weight * predicted_raw * measured_offset
            total_weight += weight
        if total_weight <= 0:
            return 0.0
        return alignment / total_weight

    def _evaluate_candidate(
        self, samples: list[Tuple[int, int, float, float]]
    ) -> Optional["_FitMetrics"]:
        if len(samples) < 2:
            return None
        return self._robust_fit(samples)

    def _recompute_model_from_samples(
        self, player: str, samples: list[Tuple[int, int, float]]
    ) -> bool:
        sample_count = len(samples)
        if sample_count < 2:
            with self._state_lock:
                state = self._player_states[player]
                state.sample_count = sample_count
                state.last_update = time.monotonic()
            return False

        metrics = self._robust_fit(samples)
        confidence = self._confidence_from_rms(metrics.rms_ns)
        confidence_gate = self._dynamic_conf_threshold(metrics.rms_ns)
        now_mono = time.monotonic()

        changed = False
        micro_refine = False
        schedule_rerefine = False
        mapping_version = 0
        offset_sign = 1
        locked = False
        weights_last3: Tuple[float, float, float]

        with self._state_lock:
            state = self._player_states[player]
            prev_intercept = state.intercept_ns
            prev_slope = state.slope
            prev_confidence = state.confidence
            prev_rms_ns = state.rms_ns
            prev_mode = state.slope_mode
            prev_sample_count = state.sample_count

            delta_intercept = abs(prev_intercept - metrics.intercept_ns)
            delta_slope = abs(prev_slope - metrics.slope_applied)
            changed = (
                delta_intercept > self._intercept_epsilon_ns
                or delta_slope > self._slope_epsilon
                or prev_sample_count != sample_count
                or prev_mode != metrics.slope_mode
            )

            projected_offset_ms = (
                delta_slope * self._micro_refine_horizon_s * 1_000.0
            )
            micro_refine = (
                not changed
                and delta_slope > 0.0
                and delta_slope <= self._micro_refine_slope_max
                and projected_offset_ms >= self._micro_refine_offset_ms
            )

            rms_improvement_ms = (prev_rms_ns - metrics.rms_ns) / 1_000_000.0
            confidence_improvement = confidence - prev_confidence
            if (
                (rms_improvement_ms > REREFINE_RMS_IMPROVEMENT_MS)
                or (confidence_improvement > REREFINE_CONF_IMPROVEMENT)
            ) and (now_mono - state.last_rerefine_ts >= self._rerefine_rate_limit_s):
                schedule_rerefine = True
                state.last_rerefine_ts = now_mono

            state.intercept_ns = metrics.intercept_ns
            state.slope = metrics.slope_applied
            state.slope_raw = metrics.slope_raw
            state.slope_mode = metrics.slope_mode
            state.rms_ns = metrics.rms_ns
            state.mad_ns = metrics.mad_ns
            state.confidence = confidence
            state.confidence_gate = confidence_gate
            state.sample_count = sample_count
            state.last_update = now_mono
            weights_last3 = self._tail_weights(metrics.weights, 3)
            state.weights_last3 = weights_last3

            if changed or state.mapping_version == 0:
                state.mapping_version += 1
            mapping_version = state.mapping_version
            offset_sign = state.offset_sign
            locked = state.offset_sign_locked

        weights_repr = ", ".join(f"{w:.2f}" for w in weights_last3)
        log_fn = log.info if (changed or micro_refine) else log.debug
        log_fn(
            (
                "Mapping update %s: a=%.0fns b_raw=%.9f b=%.9f mode=%s "
                "rms=%.3fms rms_ns=%.0f mad=%.3fms conf=%.3f gate=%.2f "
                "samples=%d v%s sign=%+d locked=%s weights=[%s]"
            ),
            player,
            metrics.intercept_ns,
            metrics.slope_raw,
            metrics.slope_applied,
            metrics.slope_mode,
            metrics.rms_ns / 1_000_000.0,
            metrics.rms_ns,
            metrics.mad_ns / 1_000_000.0,
            confidence,
            confidence_gate,
            sample_count,
            mapping_version,
            offset_sign,
            locked,
            weights_repr,
        )

        if changed:
            self._refine_all_pending_for_player(
                player, mapping_version, reason="regular"
            )
        elif micro_refine:
            log.info(
                "Micro refine trigger for %s (Δb=%.6e, proj=%.3fms)",
                player,
                metrics.slope_applied - prev_slope,
                projected_offset_ms,
            )
            self._refine_all_pending_for_player(
                player,
                mapping_version,
                force=True,
                reason="micro",
            )

        if schedule_rerefine:
            log.info(
                "Scheduling re-refine for %s (Δrms=%.3fms Δconf=%.3f)",
                player,
                rms_improvement_ms,
                confidence_improvement,
            )
            self._schedule_rerefine(player)

        return changed

    def _refine_all_pending_for_player(
        self,
        player: str,
        version: int,
        *,
        force: bool = False,
        reason: str = "regular",
    ) -> None:
        pending: list[Tuple[str, int]] = []
        with self._state_lock:
            for event_id, (t_local_ns, versions) in self._known_events.items():
                last_version = versions.get(player, 0)
                if version > last_version:
                    pending.append((event_id, t_local_ns))
        for event_id, t_local_ns in pending:
            self._refine_event_for_player(
                player,
                event_id,
                t_local_ns,
                version_hint=version,
                force=force,
                reason=reason,
            )

    def _schedule_rerefine(self, player: str) -> None:
        self._enqueue("rerefine", player)

    def _perform_rerefine(self, player: str) -> None:
        with self._state_lock:
            state = self._player_states.get(player)
            if state is None:
                return
            mapping_version = state.mapping_version
            event_ids = list(reversed(self._event_order))
            selected: list[Tuple[str, int]] = []
            for event_id in event_ids:
                entry = self._known_events.get(event_id)
                if entry is None:
                    continue
                t_local_ns, _ = entry
                selected.append((event_id, t_local_ns))
                if len(selected) >= self._rerefine_window:
                    break
        if not selected:
            log.debug("No events available for re-refine of %s", player)
            return
        log.info(
            "Re-refine batch for %s: %d events (v%s)",
            player,
            len(selected),
            mapping_version,
        )
        for event_id, t_local_ns in selected:
            self._refine_event_for_player(
                player,
                event_id,
                t_local_ns,
                version_hint=mapping_version,
                force=True,
                reason="rerefine",
            )

    def _process_event(self, event_id: str, t_local_ns: int) -> None:
        with self._state_lock:
            previous = self._known_events.get(event_id)
            versions: Dict[str, int]
            if previous is None:
                versions = {}
            else:
                _, versions = previous
            self._known_events[event_id] = (t_local_ns, dict(versions))
            self._event_order.append(event_id)
            while len(self._event_order) > self._event_retention:
                stale = self._event_order.popleft()
                if stale == event_id:
                    continue
                self._known_events.pop(stale, None)
        self._refine_single_event(event_id)

    def _refine_single_event(
        self, event_id: str, *, version_hint: Optional[int] = None
    ) -> None:
        with self._state_lock:
            entry = self._known_events.get(event_id)
        if entry is None:
            return
        t_local_ns, _ = entry
        players = set(self._connected_players_snapshot())
        with self._state_lock:
            players.update(self._player_states.keys())
        for player in sorted(players):
            self._refine_event_for_player(
                player, event_id, t_local_ns, version_hint=version_hint
            )

    def _refine_event_for_player(
        self,
        player: str,
        event_id: str,
        t_local_ns: int,
        version_hint: Optional[int] = None,
        *,
        force: bool = False,
        reason: str = "regular",
    ) -> None:
        with self._state_lock:
            state = self._player_states.get(player)
            entry = self._known_events.get(event_id)
            if state is None or entry is None:
                return
            _, version_by_player = entry
            last_version = version_by_player.get(player, 0)
            mapping_version = state.mapping_version
            intercept = state.intercept_ns
            slope = state.slope
            confidence = state.confidence
            rms_ns = state.rms_ns
            sample_count = state.sample_count
            offset_sign = state.offset_sign
            confidence_gate = state.confidence_gate
            locked = state.offset_sign_locked
            effective_version = version_hint or mapping_version
        if not force and effective_version <= last_version:
            return
        if sample_count < 2:
            log.debug(
                "Skipping refinement for %s (event %s): insufficient samples %d",
                player,
                event_id,
                sample_count,
            )
            return
        if confidence < confidence_gate:
            log.info(
                "Refinement skipped for %s (event %s): confidence %.3f < gate %.2f (%s)",
                player,
                event_id,
                confidence,
                confidence_gate,
                reason,
            )
            return
        t_ref_ns = int(intercept + slope * t_local_ns)
        extra = {
            "rms_error_ns": int(rms_ns),
            "heartbeat_count": self._heartbeat_count,
            "samples": sample_count,
            "offset_sign": offset_sign,
            "locked": locked,
            "reason": reason,
        }
        queue_size, queue_capacity = self._bridge.event_queue_load()
        extra["queue"] = {"size": queue_size, "capacity": queue_capacity}
        try:
            self._bridge.refine_event(
                player,
                event_id,
                t_ref_ns,
                confidence=confidence,
                mapping_version=effective_version,
                extra=dict(extra),
            )
        except Exception:
            log.exception("Refinement dispatch failed for %s (%s)", event_id, player)
            return
        try:
            self._logger.upsert_refinement(
                event_id,
                player,
                t_ref_ns,
                effective_version,
                confidence,
                reason,
            )
        except Exception:
            log.exception("Persisting refinement failed for %s (%s)", event_id, player)
        else:
            log.info(
                "event %s refined for %s (t_local=%d, t_ref=%d, v%s, conf=%.3f, reason=%s)",
                event_id,
                player,
                t_local_ns,
                t_ref_ns,
                effective_version,
                confidence,
                reason,
            )
            with self._state_lock:
                entry = self._known_events.get(event_id)
                if entry is not None:
                    t_ns, version_by_player = entry
                    version_by_player = dict(version_by_player)
                    version_by_player[player] = effective_version
                    self._known_events[event_id] = (t_ns, version_by_player)

    # ------------------------------------------------------------------
    def _connected_players_snapshot(self) -> list[str]:
        try:
            players = self._bridge.connected_players()
        except Exception:
            log.debug("connected_players lookup failed", exc_info=True)
            players = []
        if not players:
            with self._state_lock:
                players = list(self._player_states.keys())
        return players

    def _robust_fit(self, samples: list[Tuple[int, int, float, float]]) -> _FitMetrics:
        xs = [float(a) for a, _, _, _ in samples]
        ys = [float(b) for _, b, _, _ in samples]
        recency_weights = self._recency_weights(samples)
        base_weights = [max(weight, 0.0) for _, _, _, weight in samples]
        weights = [bw * rw for bw, rw in zip(base_weights, recency_weights)]
        if not any(weights):
            weights = recency_weights[:] or [1.0 for _ in samples]
        intercept = ys[0]
        slope = 1.0
        for _ in range(6):
            sum_w = sum(weights)
            if sum_w <= 0:
                intercept = ys[0]
                slope = 1.0
                break
            mean_x = sum(w * x for w, x in zip(weights, xs)) / sum_w
            mean_y = sum(w * y for w, y in zip(weights, ys)) / sum_w
            var = sum(w * (x - mean_x) ** 2 for w, x in zip(weights, xs))
            if var <= 0:
                slope = 1.0
            else:
                cov = sum(
                    w * (x - mean_x) * (y - mean_y)
                    for w, x, y in zip(weights, xs, ys)
                )
                slope = cov / var if var else 1.0
            intercept = mean_y - slope * mean_x
            residuals = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
            huber_weights = [
                1.0
                if abs(r) <= self._huber_delta_ns
                else self._huber_delta_ns / max(abs(r), 1e-9)
                for r in residuals
            ]
            weights = [
                rec * hub * base
                for rec, hub, base in zip(recency_weights, huber_weights, base_weights)
            ]

        intercept_raw = intercept
        residuals_raw = [y - (intercept + slope * x) for x, y in zip(xs, ys)]
        sum_w = sum(weights) or 1.0
        rms_raw_ns = math.sqrt(
            sum(w * (r ** 2) for w, r in zip(weights, residuals_raw)) / sum_w
        )
        confidence_raw = self._confidence_from_rms(rms_raw_ns)
        intercept_adj, slope_applied, mode = self._apply_slope_guardrails(
            intercept, slope, xs, ys, weights, confidence_raw
        )
        residuals_applied = [
            y - (intercept_adj + slope_applied * x) for x, y in zip(xs, ys)
        ]
        sum_w_applied = sum(weights) or 1.0
        rms_ns = math.sqrt(
            sum(w * (r ** 2) for w, r in zip(weights, residuals_applied)) / sum_w_applied
        )
        abs_residuals = [abs(r) for r in residuals_applied]
        mad_ns = self._weighted_median(abs_residuals, weights)
        norm_weights = self._normalize_weights(weights)
        return _FitMetrics(
            intercept_adj,
            intercept_raw,
            slope,
            slope_applied,
            rms_ns,
            mad_ns,
            norm_weights,
            mode,
        )

    def _confidence_from_rms(self, rms_ns: float) -> float:
        if rms_ns <= 0:
            return 1.0
        rms_ms = rms_ns / 1_000_000.0
        # Map 0ms -> 1.0, 5ms -> ~0.37, >=20ms -> ~0.018
        return max(0.0, min(1.0, math.exp(-rms_ms / 5.0)))

    def _dynamic_conf_threshold(self, rms_ns: float) -> float:
        rms_ms = rms_ns / 1_000_000.0
        if rms_ms > CONF_GATE_HIGH_RMS_MS:
            return CONF_GATE_HIGH
        if rms_ms < CONF_GATE_LOW_RMS_MS:
            return CONF_GATE_LOW
        return CONF_GATE_DEFAULT

    def _apply_slope_guardrails(
        self,
        intercept: float,
        slope: float,
        xs: List[float],
        ys: List[float],
        weights: List[float],
        confidence_raw: float,
    ) -> Tuple[float, float, str]:
        slope_applied = slope
        mode = "free"
        delta_ppm = abs(slope - 1.0) * 1_000_000.0
        freeze_threshold = SLOPE_FREEZE_PPM * 1_000_000.0 - 0.5
        if (
            confidence_raw >= SLOPE_FREEZE_CONFIDENCE
            and delta_ppm < freeze_threshold
        ):
            slope_applied = 1.0
            mode = "frozen"
        else:
            clamped = min(max(slope, SLOPE_CLAMP_MIN), SLOPE_CLAMP_MAX)
            if abs(clamped - slope) > 0:
                slope_applied = clamped
                mode = "clamped"
        sum_w = sum(weights) or 1.0
        intercept_applied = (
            sum(w * (y - slope_applied * x) for w, x, y in zip(weights, xs, ys))
            / sum_w
        )
        return intercept_applied, slope_applied, mode

    def _recency_weights(
        self, samples: list[Tuple[int, int, float, float]]
    ) -> List[float]:
        now = time.monotonic()
        tau = max(self._recency_tau, 1e-3)
        weights: List[float] = []
        for _, _, ingest_ts, _ in samples:
            age = max(0.0, now - ingest_ts)
            weights.append(math.exp(-age / tau))
        return weights

    @staticmethod
    def _normalize_weights(weights: List[float]) -> List[float]:
        total = sum(weights)
        if total <= 0:
            return [0.0 for _ in weights]
        return [w / total for w in weights]

    @staticmethod
    def _weighted_median(values: Iterable[float], weights: Iterable[float]) -> float:
        pairs = sorted(zip(values, weights), key=lambda item: item[0])
        total = sum(weight for _, weight in pairs)
        if total <= 0 or not pairs:
            return 0.0
        cumulative = 0.0
        half = total / 2.0
        for value, weight in pairs:
            cumulative += weight
            if cumulative >= half:
                return value
        return pairs[-1][0]

    @staticmethod
    def _tail_weights(weights: List[float], count: int) -> Tuple[float, float, float]:
        if count <= 0:
            return (0.0, 0.0, 0.0)
        tail = list(weights[-count:])
        if len(tail) < count:
            tail = [0.0] * (count - len(tail)) + tail
        if len(tail) < 3:
            tail = [0.0] * (3 - len(tail)) + tail
        return tuple(tail[-3:])  # type: ignore[return-value]


__all__ = ["TimeReconciler"]
