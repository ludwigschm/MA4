"""Latency metrics collection and presentation helpers."""

from __future__ import annotations

import atexit
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict

from core.logging import get_logger

log = get_logger("metrics.latency")


def _as_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.lower() not in {"0", "false", "off", "no"}


@dataclass
class _LatencyEstimate:
    offset_ns: float
    slope_ppb: float
    rms_ns: float
    samples: int
    updated_at: float


class LatencyMetrics:
    """Collect latency estimator outputs and expose convenient summaries."""

    def __init__(
        self,
        *,
        log_interval_s: float = 5.0,
        run_root: Path | str | None = None,
    ) -> None:
        self._log_interval_s = max(0.0, float(log_interval_s))
        self._run_root = Path(run_root) if run_root is not None else Path("runs")
        self._run_dir = self._initialise_run_directory(self._run_root)
        self._jsonl_path = self._run_dir / "latency_metrics.jsonl"
        self._csv_path = self._run_dir / "latency_metrics.csv"
        self._csv_header_written = False

        self._lock = threading.Lock()
        self._export_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None
        self._estimates: Dict[str, _LatencyEstimate] = {}
        self._subscribers: list[Callable[[dict[str, Any]], None]] = []
        self._last_snapshot: dict[str, Any] = {
            "timestamp": time.time(),
            "estimates": {},
        }

        if self._log_interval_s > 0.0:
            self._worker = threading.Thread(
                target=self._log_worker,
                name="LatencyMetricsLogger",
                daemon=True,
            )
            self._worker.start()

        atexit.register(self.close)

    # ------------------------------------------------------------------
    @property
    def run_directory(self) -> Path:
        return self._run_dir

    # ------------------------------------------------------------------
    def update_estimate(
        self,
        key: str,
        *,
        offset_ns: float,
        slope: float,
        rms_ns: float,
        samples: int,
    ) -> None:
        """Record the latest latency estimator output for ``key``."""

        now = time.time()
        estimate = _LatencyEstimate(
            offset_ns=float(offset_ns),
            slope_ppb=float(slope) * 1_000_000_000.0,
            rms_ns=float(rms_ns),
            samples=int(samples),
            updated_at=now,
        )

        with self._lock:
            self._estimates[key] = estimate
            snapshot = self._build_snapshot_locked(now)
            self._last_snapshot = snapshot
            subscribers = list(self._subscribers)

        for callback in subscribers:
            try:
                callback(snapshot)
            except Exception:  # pragma: no cover - defensive
                log.debug("LatencyMetrics subscriber failed", exc_info=True)

    # ------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot = dict(self._last_snapshot)
            snapshot["estimates"] = {
                key: dict(value)
                for key, value in self._last_snapshot["estimates"].items()
            }
        return snapshot

    # ------------------------------------------------------------------
    def subscribe(self, callback: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        with self._lock:
            self._subscribers.append(callback)
        callback(self.snapshot())

        def _unsubscribe() -> None:
            with self._lock:
                try:
                    self._subscribers.remove(callback)
                except ValueError:  # pragma: no cover - defensive
                    pass

        return _unsubscribe

    # ------------------------------------------------------------------
    def export_snapshot(self) -> None:
        self._export_snapshot()

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._worker and self._worker.is_alive():
            self._stop_event.set()
            self._worker.join(timeout=1.5)
        self._worker = None

    # ------------------------------------------------------------------
    def _log_worker(self) -> None:  # pragma: no cover - background thread
        while not self._stop_event.wait(self._log_interval_s):
            self._export_snapshot()

    # ------------------------------------------------------------------
    def _build_snapshot_locked(self, now: float | None = None) -> dict[str, Any]:
        if now is None:
            now = time.time()
        estimates = {
            key: {
                "offset_ns": est.offset_ns,
                "offset_ms": est.offset_ns / 1_000_000.0,
                "slope_ppb": est.slope_ppb,
                "rms_ns": est.rms_ns,
                "rms_ms": est.rms_ns / 1_000_000.0,
                "samples": est.samples,
                "age_s": max(0.0, now - est.updated_at),
            }
            for key, est in self._estimates.items()
        }
        return {"timestamp": now, "estimates": estimates}

    # ------------------------------------------------------------------
    def _export_snapshot(self) -> None:
        snapshot = self.snapshot()
        timestamp = snapshot.get("timestamp", time.time())
        estimates = snapshot.get("estimates", {})

        log_messages: list[str] = []
        for key in sorted(estimates):
            est = estimates[key]
            log_messages.append(
                (
                    "%s: offset=%.3fms slope=%+.3fppb rms=%.3fms samples=%d age=%.1fs"
                )
                % (
                    key,
                    est["offset_ms"],
                    est["slope_ppb"],
                    est["rms_ms"],
                    est["samples"],
                    est["age_s"],
                )
            )

        if log_messages:
            log.info("latency %s", " | ".join(log_messages))
        else:
            log.info("latency no-samples")

        with self._export_lock:
            if not estimates:
                return

            self._write_jsonl(timestamp, estimates)
            self._write_csv(timestamp, estimates)

    # ------------------------------------------------------------------
    def _write_jsonl(self, timestamp: float, estimates: Dict[str, Dict[str, float]]) -> None:
        with self._jsonl_path.open("a", encoding="utf-8") as handle:
            for key in sorted(estimates):
                est = estimates[key]
                entry = {
                    "timestamp": timestamp,
                    "source": key,
                    "offset_ns": est["offset_ns"],
                    "slope_ppb": est["slope_ppb"],
                    "rms_ns": est["rms_ns"],
                    "samples": est["samples"],
                    "age_s": est["age_s"],
                }
                handle.write(json.dumps(entry, sort_keys=True))
                handle.write("\n")

    # ------------------------------------------------------------------
    def _write_csv(self, timestamp: float, estimates: Dict[str, Dict[str, float]]) -> None:
        header = "timestamp,source,offset_ns,slope_ppb,rms_ns,samples,age_s\n"
        with self._csv_path.open("a", encoding="utf-8") as handle:
            if not self._csv_header_written:
                handle.write(header)
                self._csv_header_written = True
            for key in sorted(estimates):
                est = estimates[key]
                line = (
                    f"{timestamp:.6f},{key},{est['offset_ns']:.6f},{est['slope_ppb']:.6f},"
                    f"{est['rms_ns']:.6f},{int(est['samples'])},{est['age_s']:.3f}\n"
                )
                handle.write(line)

    # ------------------------------------------------------------------
    @staticmethod
    def _initialise_run_directory(root: Path) -> Path:
        root.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        candidate = root / timestamp
        counter = 1
        while candidate.exists():
            candidate = root / f"{timestamp}_{counter:02d}"
            counter += 1
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate


_DEFAULT_METRICS: LatencyMetrics | None = None
_DEFAULT_LOCK = threading.Lock()


def default_latency_metrics() -> LatencyMetrics:
    global _DEFAULT_METRICS
    with _DEFAULT_LOCK:
        if _DEFAULT_METRICS is None:
            interval = os.getenv("LATENCY_METRICS_INTERVAL", "5.0")
            try:
                interval_value = float(interval)
            except ValueError:
                interval_value = 5.0
            _DEFAULT_METRICS = LatencyMetrics(log_interval_s=interval_value)
    return _DEFAULT_METRICS


def create_latency_panel(metrics: LatencyMetrics | None = None):
    if not _as_bool(os.getenv("SHOW_LATENCY_PANEL"), True):
        return None

    metrics = metrics or default_latency_metrics()

    try:
        from kivy.clock import Clock  # type: ignore
        from kivy.uix.boxlayout import BoxLayout  # type: ignore
        from kivy.uix.label import Label  # type: ignore
    except Exception:  # pragma: no cover - optional dependency
        log.debug("Latency panel disabled – Kivy unavailable", exc_info=True)
        return None

    class _LatencyPanel(BoxLayout):  # type: ignore
        def __init__(self, metrics_obj: LatencyMetrics, **kwargs: Any) -> None:
            super().__init__(orientation="vertical", padding=(8, 6), spacing=2, **kwargs)
            self.size_hint = (None, None)
            self.size = (240, 70)
            self.opacity = 0.8
            self.pos_hint = {"top": 0.98, "right": 0.985}
            self._label = Label(
                text="Latency: n/a",
                font_size="16sp",
                halign="right",
                valign="top",
                color=(1, 1, 1, 0.85),
            )
            self._label.bind(size=self._label.setter("text_size"))
            self.add_widget(self._label)
            self._metrics = metrics_obj
            self._unsubscribe = metrics_obj.subscribe(self._on_snapshot)

        def _on_snapshot(self, snapshot: dict[str, Any]) -> None:
            estimates = snapshot.get("estimates", {})
            if not estimates:
                text = "Latency: n/a"
            else:
                lines: list[str] = []
                for key in sorted(estimates):
                    est = estimates[key]
                    lines.append(
                        f"{key}: {est['offset_ms']:+.2f} ms / {est['slope_ppb']:+.1f} ppb"
                    )
                text = "\n".join(lines)

            def _apply(_dt: float) -> None:
                self._label.text = text

            Clock.schedule_once(_apply, 0)

        def on_parent(self, instance, parent):  # type: ignore
            if parent is None:
                unsubscribe = getattr(self, "_unsubscribe", None)
                if callable(unsubscribe):
                    unsubscribe()
            return super().on_parent(instance, parent)

        def on_kv_post(self, base_widget):  # type: ignore
            self.canvas.before.clear()
            with self.canvas.before:
                from kivy.graphics import Color, RoundedRectangle  # type: ignore

                Color(0.05, 0.05, 0.05, 0.7)
                self._background = RoundedRectangle(
                    pos=self.pos,
                    size=self.size,
                    radius=[8],
                )
            self.bind(pos=self._update_background, size=self._update_background)
            return super().on_kv_post(base_widget)

        def _update_background(self, *_args: Any) -> None:
            background = getattr(self, "_background", None)
            if background is not None:
                background.pos = self.pos
                background.size = self.size

    return _LatencyPanel(metrics)
