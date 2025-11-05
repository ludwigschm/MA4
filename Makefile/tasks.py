#!/usr/bin/env python3
"""Helper entry points for project make-style tasks."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Dict, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def _run(cmd: Iterable[str]) -> None:
    printable = " ".join(shlex.quote(str(part)) for part in cmd)
    print(f"$ {printable}")
    subprocess.run(list(cmd), check=True)


def latency_smoke() -> None:
    """Run the latency smoke-test pipeline."""

    with tempfile.TemporaryDirectory(prefix="latency-smoke-") as tmpdir:
        tmp_path = Path(tmpdir)
        markers_path = tmp_path / "markers.jsonl"
        shifted_path = tmp_path / "markers_shifted.jsonl"

        _run(
            (
                PYTHON,
                str(REPO_ROOT / "tools" / "emit_test_markers.py"),
                "--output",
                str(markers_path),
            )
        )

        _run(
            (
                PYTHON,
                str(REPO_ROOT / "tools" / "replay_events_shift.py"),
                str(markers_path),
                str(shifted_path),
                "--offset-ns",
                "5000000",
                "--slope",
                "0.999999",
            )
        )

        print(f"Shifted events stored in {shifted_path}")
        try:
            preview = [
                json.loads(line)
                for line in shifted_path.read_text(encoding="utf-8").splitlines()[:4]
            ]
        except FileNotFoundError:
            preview = []
        if preview:
            print(json.dumps(preview, indent=2, ensure_ascii=False))


TASKS: Dict[str, Callable[[], None]] = {
    "latency-smoke": latency_smoke,
}


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute helper tasks")
    parser.add_argument("task", choices=sorted(TASKS), help="Task to execute")
    args = parser.parse_args(list(argv) if argv is not None else None)

    TASKS[args.task]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
