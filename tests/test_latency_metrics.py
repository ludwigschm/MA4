from __future__ import annotations

import json
from pathlib import Path

from metrics.latency_metrics import LatencyMetrics, create_latency_panel


def test_latency_metrics_export(tmp_path: Path) -> None:
    metrics = LatencyMetrics(log_interval_s=0.0, run_root=tmp_path)
    metrics.update_estimate(
        "VP1",
        offset_ns=1_200_000.0,
        slope=1.5e-9,
        rms_ns=380_000.0,
        samples=42,
    )
    metrics.export_snapshot()

    run_dirs = list(tmp_path.iterdir())
    assert len(run_dirs) == 1
    export_dir = run_dirs[0]
    jsonl_path = export_dir / "latency_metrics.jsonl"
    csv_path = export_dir / "latency_metrics.csv"
    assert jsonl_path.exists()
    assert csv_path.exists()

    data = json.loads(jsonl_path.read_text().strip())
    assert data["source"] == "VP1"
    assert data["samples"] == 42
    assert abs(data["offset_ns"] - 1_200_000.0) < 1e-3
    assert abs(data["slope_ppb"] - 1.5) < 1e-6

    csv_lines = [line for line in csv_path.read_text().splitlines() if line]
    assert len(csv_lines) == 2  # header + entry
    assert "VP1" in csv_lines[1]


def test_latency_panel_disable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHOW_LATENCY_PANEL", "0")
    panel = create_latency_panel(LatencyMetrics(log_interval_s=0.0, run_root=tmp_path))
    assert panel is None
