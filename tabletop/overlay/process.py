"""Utilities for managing the external ArUco overlay process."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional, Union

import os

from tabletop.data.config import ARUCO_OVERLAY_PATH

PathLike = Union[str, Path]
OverlayProcess = Optional[subprocess.Popen]


def _resolve_overlay_path(overlay_path: Optional[PathLike]) -> Path:
    if overlay_path is None:
        return ARUCO_OVERLAY_PATH
    return Path(overlay_path)


def start_overlay(
    process: OverlayProcess = None,
    overlay_path: Optional[PathLike] = None,
    *,
    display_index: Optional[int] = None,
) -> OverlayProcess:
    """Ensure the overlay process is running and return its handle.

    Args:
        process: Existing overlay process handle, if any.
        overlay_path: Optional path to the overlay script. Defaults to
            :data:`tabletop.data.config.ARUCO_OVERLAY_PATH` when not provided.
        display_index: Zero-based display index that should host the overlay.
            When provided the value is forwarded to the overlay script via
            command line argument and environment variable so both the PyQt
            overlay and the Kivy host window share the same screen.

    Returns:
        A running overlay process handle or ``None`` when the overlay could not
        be started.
    """

    if process and process.poll() is None:
        return process

    path = _resolve_overlay_path(overlay_path)
    if not path.exists():
        return None

    cmd = [sys.executable, str(path)]
    popen_kwargs = {}

    if display_index is not None:
        cmd.append(f"--display={int(display_index)}")
        env = os.environ.copy()
        env["TABLETOP_DISPLAY_INDEX"] = str(int(display_index))
        popen_kwargs["env"] = env

    try:
        return subprocess.Popen(cmd, **popen_kwargs)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Warnung: Overlay konnte nicht gestartet werden: {exc}")
        return None


def stop_overlay(process: OverlayProcess) -> OverlayProcess:
    """Stop a previously started overlay process.

    Args:
        process: Process handle to stop.

    Returns:
        ``None``. The return type mirrors :func:`start_overlay` so callers can
        assign the result back to their stored handle without extra conditionals.
    """

    if not process:
        return None

    if process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=5)
        except Exception:  # pragma: no cover - defensive fallback
            try:
                process.kill()
            except Exception:
                pass

    return None


def start_overlay_process(
    process: OverlayProcess = None,
    overlay_path: Optional[PathLike] = None,
    *,
    display_index: Optional[int] = None,
) -> OverlayProcess:
    """Compatibility wrapper returning :func:`start_overlay`."""

    return start_overlay(process, overlay_path, display_index=display_index)


def stop_overlay_process(process: OverlayProcess) -> OverlayProcess:
    """Compatibility wrapper returning :func:`stop_overlay`."""

    return stop_overlay(process)


__all__ = [
    "OverlayProcess",
    "start_overlay",
    "stop_overlay",
    "start_overlay_process",
    "stop_overlay_process",
]
