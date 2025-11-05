#!/usr/bin/env python3
"""Compute latency baseline statistics from sync logs.

The script scans the provided log files (JSON lines) for host/device timestamp
pairs or explicit offset information. It computes offset mean, median, RMS and
an estimated drift (ns/s) over a configurable trailing window (default 60 s).
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Iterable, List, Optional, Sequence, Tuple

# Candidate keys that may contain monotonic timestamps in seconds
_SECOND_KEYS = ("ingest_ts", "timestamp", "ts", "time_s")
# Candidate keys that contain monotonic timestamps in nanoseconds
_NANOSECOND_KEYS = (
    "t_host_ns",
    "t_local_ns",
    "host_ns",
    "device_ns",
    "t_device_ns",
    "t_ref_ns",
)


@dataclass
class Sample:
    time_s: Optional[float]
    offset_ns: float


def _iter_source_files(paths: Sequence[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(
                p
                for pattern in ("*.jsonl", "*.json", "*.log")
                for p in path.rglob(pattern)
                if p.is_file()
            )
        elif path.is_file():
            yield path


def _default_sources(repo_root: Path) -> List[Path]:
    latency_dir = repo_root / "docs" / "latency"
    if latency_dir.exists():
        return list(_iter_source_files([latency_dir]))
    return []


def _extract_time(entry: dict) -> Optional[float]:
    for key in _SECOND_KEYS:
        if key in entry:
            value = entry[key]
            if isinstance(value, (int, float)):
                return float(value)
    for key in _NANOSECOND_KEYS:
        if key in entry:
            value = entry[key]
            if isinstance(value, (int, float)):
                return float(value) / 1_000_000_000.0
    return None


def _extract_offset(entry: dict) -> Optional[float]:
    if "offset_ns" in entry and isinstance(entry["offset_ns"], (int, float)):
        return float(entry["offset_ns"])
    if "offset" in entry and isinstance(entry["offset"], (int, float)):
        # Assume offset given in seconds if value looks small
        value = float(entry["offset"])
        if abs(value) < 1e-3:
            return value * 1_000_000_000.0
        return value
    key_pairs: Tuple[Tuple[str, str], ...] = (
        ("t_device_ns", "t_host_ns"),
        ("device_ns", "host_ns"),
        ("t_device_ns", "t_local_ns"),
        ("device_ns", "t_local_ns"),
    )
    for device_key, host_key in key_pairs:
        if device_key in entry and host_key in entry:
            device_value = entry[device_key]
            host_value = entry[host_key]
            if isinstance(device_value, (int, float)) and isinstance(
                host_value, (int, float)
            ):
                return float(device_value - host_value)
    return None


def _load_samples(paths: Sequence[Path]) -> List[Sample]:
    samples: List[Sample] = []
    for path in paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        entry = json.loads(stripped)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    offset = _extract_offset(entry)
                    if offset is None:
                        continue
                    time_s = _extract_time(entry)
                    samples.append(Sample(time_s=time_s, offset_ns=float(offset)))
        except FileNotFoundError:
            continue
    return samples


def _filter_window(samples: Sequence[Sample], window_s: float) -> List[Sample]:
    times = [s.time_s for s in samples if s.time_s is not None]
    if not times:
        return list(samples)
    latest = max(times)
    cutoff = latest - window_s
    return [s for s in samples if s.time_s is None or s.time_s >= cutoff]


def _compute_drift(samples: Sequence[Sample]) -> Optional[float]:
    timed_samples = [(s.time_s, s.offset_ns) for s in samples if s.time_s is not None]
    if len(timed_samples) < 2:
        return None
    mean_time = mean(t for t, _ in timed_samples)
    mean_offset = mean(o for _, o in timed_samples)
    numerator = sum((t - mean_time) * (o - mean_offset) for t, o in timed_samples)
    denominator = sum((t - mean_time) ** 2 for t, _ in timed_samples)
    if denominator <= 0:
        return None
    return numerator / denominator


def compute_metrics(samples: Sequence[Sample], window_s: float) -> dict:
    if not samples:
        raise ValueError("no samples available")
    windowed = _filter_window(samples, window_s)
    if not windowed:
        raise ValueError("no samples available in the requested window")
    offsets = [s.offset_ns for s in windowed]
    offset_mean = mean(offsets)
    offset_median = median(offsets)
    offset_rms = math.sqrt(sum(o * o for o in offsets) / len(offsets))
    drift = _compute_drift(windowed)
    times = [s.time_s for s in windowed if s.time_s is not None]
    duration = max(times) - min(times) if len(times) >= 2 else None
    return {
        "sample_count": len(windowed),
        "window_s": window_s,
        "duration_s": duration,
        "mean_offset_ns": offset_mean,
        "median_offset_ns": offset_median,
        "rms_offset_ns": offset_rms,
        "drift_ns_per_s": drift,
    }


def format_summary(metrics: dict) -> str:
    lines = [
        f"Samples used: {metrics['sample_count']} (window={metrics['window_s']} s)",
        f"Mean offset: {metrics['mean_offset_ns'] / 1_000_000.0:.3f} ms",
        f"Median offset: {metrics['median_offset_ns'] / 1_000_000.0:.3f} ms",
        f"RMS: {metrics['rms_offset_ns'] / 1_000_000.0:.3f} ms",
    ]
    drift = metrics.get("drift_ns_per_s")
    if drift is not None:
        lines.append(f"Drift: {drift:.3f} ns/s")
    else:
        lines.append("Drift: n/a")
    duration = metrics.get("duration_s")
    if duration is not None:
        lines.append(f"Observed duration: {duration:.3f} s")
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Compute latency baseline metrics")
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Log files or directories to scan (defaults to docs/latency)",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=60.0,
        help="Trailing window in seconds used for the statistics (default: 60)",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Only print the JSON payload (omit the human readable summary)",
    )
    args = parser.parse_args(argv)

    sources = args.paths or _default_sources(repo_root)
    if not sources:
        parser.error("no log sources specified and docs/latency is empty")

    source_files = list(_iter_source_files(sources))
    if not source_files:
        parser.error("no readable log files found in the provided paths")

    samples = _load_samples(source_files)
    if not samples:
        parser.error("no latency samples found in the provided log files")

    try:
        metrics = compute_metrics(samples, args.window)
    except ValueError as exc:  # pragma: no cover - defensive branch
        parser.error(str(exc))
        return 2

    if not args.json_only:
        print(format_summary(metrics))
        print("-" * 40)
    metrics_json = json.dumps(metrics, indent=2, sort_keys=True)
    print(metrics_json)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
