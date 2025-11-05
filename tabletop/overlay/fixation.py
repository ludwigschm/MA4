"""Utilities for managing the fixation overlay sequence and tone playback."""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

import numpy as np
import sounddevice as sd
import threading

if TYPE_CHECKING:
    from tabletop.pupil_bridge import PupilBridge


_FIXATION_CROSS_ATTR = "_fixation_cross_overlay"

_GRAPHICS_SPEC = importlib.util.find_spec("kivy.graphics")
if _GRAPHICS_SPEC is not None:
    _graphics = importlib.import_module("kivy.graphics")
    _Color = getattr(_graphics, "Color", None)
    _Line = getattr(_graphics, "Line", None)
else:  # pragma: no cover - executed only when Kivy is unavailable
    _Color = None
    _Line = None


def generate_fixation_tone(
    sample_rate: int = 44100,
    duration: float = 0.2,
    frequency: float = 1000.0,
    amplitude: float = 0.9,
):
    """Create the sine-wave tone that is played during the fixation sequence."""

    t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
    return amplitude * np.sin(2 * np.pi * frequency * t)


def play_fixation_tone(controller: Any) -> None:
    """Play the fixation tone asynchronously using sounddevice."""

    tone = getattr(controller, "fixation_tone", None)
    if tone is None:
        return

    sample_rate = getattr(controller, "fixation_tone_fs", 44100)
    tone_data = tone.copy()
    beep_callback = getattr(controller, "fixation_beep_callback", None)

    def _play():
        try:
            sd.play(tone_data, sample_rate)
            if callable(beep_callback):
                try:
                    beep_callback()
                except Exception:  # pragma: no cover - defensive callback guard
                    pass
            sd.wait()
        except Exception as exc:  # pragma: no cover - audio hardware dependent
            print(f"Warnung: Ton konnte nicht abgespielt werden: {exc}")
        finally:
            if hasattr(controller, "fixation_beep_callback"):
                controller.fixation_beep_callback = None

    threading.Thread(target=_play, daemon=True).start()


def run_fixation_sequence(
    controller: Any,
    *,
    schedule_once: Callable[[Callable[[float], None], float], Any],
    stop_image: Optional[Path | str],
    live_image: Optional[Path | str],
    on_complete: Optional[Callable[[], None]] = None,
    bridge: Optional["PupilBridge"] = None,
    players: Optional[Iterable[str]] = None,
    player: Optional[str] = None,
    session: Optional[int] = None,
    block: Optional[int] = None,
) -> None:
    """Execute the fixation sequence using the provided controller state."""

    if getattr(controller, "fixation_running", False):
        return

    overlay = getattr(controller, "fixation_overlay", None)
    image = getattr(controller, "fixation_image", None)
    if overlay is None or image is None:
        if hasattr(controller, "fixation_required"):
            controller.fixation_required = False
        if on_complete:
            on_complete()
        return

    player_targets = {
        p for p in (players or []) if p
    }
    if player:
        player_targets.add(player)
    player_list = tuple(sorted(player_targets))

    def _send_sync_event(name: str) -> None:
        if bridge is None or not player_list:
            return
        for target in player_list:
            if not bridge.is_connected(target):
                continue
            payload: dict[str, Any] = {"player": target}
            if session is not None:
                payload["session"] = session
            if block is not None:
                payload["block"] = block
            bridge.send_event(name, target, payload)

    def _log_fixation_event(kind: str) -> None:
        log_event = getattr(controller, "log_event", None)
        if not callable(log_event):
            return
        payload: dict[str, Any] = {}
        if player_list:
            payload["players"] = list(player_list)
        if session is not None:
            payload["session"] = session
        if block is not None:
            payload["block"] = block
        log_event(None, kind, payload or None)

    controller.fixation_running = True
    controller.pending_fixation_callback = on_complete
    overlay.opacity = 1
    overlay.disabled = False

    if getattr(overlay, "parent", None) is not None:
        controller.remove_widget(overlay)
    controller.add_widget(overlay)
    for attr in ("btn_start_p1", "btn_start_p2"):
        btn = getattr(controller, attr, None)
        if btn is not None and hasattr(btn, "set_live"):
            btn.set_live(False)

    image.opacity = 1
    _set_image_source(image, live_image, fallback="cross")

    def finish(_dt: float) -> None:
        if getattr(overlay, "parent", None) is not None:
            controller.remove_widget(overlay)
        overlay.opacity = 0
        overlay.disabled = True
        _remove_cross_overlay(image)
        controller.fixation_running = False
        if hasattr(controller, "fixation_required"):
            controller.fixation_required = False
        callback = getattr(controller, "pending_fixation_callback", None)
        controller.pending_fixation_callback = None
        if callback:
            callback()

    def show_final_live(_dt: float) -> None:
        _set_image_source(image, live_image, fallback="cross")
        schedule_once(finish, 5)

    def show_stop_and_tone(_dt: float) -> None:
        _set_image_source(image, stop_image, fallback="blank")
        _log_fixation_event("fixation_flash")
        setattr(controller, "fixation_beep_callback", lambda: _log_fixation_event("fixation_beep"))
        play_fixation_tone(controller)
        if hasattr(controller, "fixation_beep_callback"):
            controller.fixation_beep_callback = None
        _send_sync_event("sync.flash_beep")
        schedule_once(show_final_live, 0.2)

    schedule_once(show_stop_and_tone, 5)


def _path_to_source(image_path: Optional[Path | str]) -> str:
    if image_path is None:
        return ""
    if isinstance(image_path, Path):
        candidate = image_path
    else:
        candidate = Path(image_path)
    return str(candidate) if candidate.exists() else ""


def _set_image_source(image: Any, image_path: Optional[Path | str], *, fallback: str) -> None:
    source = _path_to_source(image_path)
    if source:
        image.source = source
        _remove_cross_overlay(image)
        return

    image.source = ""
    if fallback == "cross":
        _ensure_cross_overlay(image)
    else:
        _remove_cross_overlay(image)


def _ensure_cross_overlay(image: Any) -> None:
    if _Color is None or _Line is None:
        return
    if getattr(image, _FIXATION_CROSS_ATTR, None) is None:
        with image.canvas.after:
            color = _Color(1, 1, 1, 1)
            line1 = _Line(points=[], width=2, cap="square")
            line2 = _Line(points=[], width=2, cap="square")
        image.bind(size=_update_cross_overlay, pos=_update_cross_overlay)
        setattr(image, _FIXATION_CROSS_ATTR, (color, line1, line2))
    _update_cross_overlay(image)


def _remove_cross_overlay(image: Any) -> None:
    cross = getattr(image, _FIXATION_CROSS_ATTR, None)
    if not cross:
        return
    image.unbind(size=_update_cross_overlay, pos=_update_cross_overlay)
    color, line1, line2 = cross
    canvas = image.canvas.after
    for instruction in (line2, line1, color):
        if instruction in canvas.children:
            canvas.remove(instruction)
    delattr(image, _FIXATION_CROSS_ATTR)


def _update_cross_overlay(image: Any, *_: Any) -> None:
    cross = getattr(image, _FIXATION_CROSS_ATTR, None)
    if not cross:
        return
    _, line1, line2 = cross
    width, height = image.size
    if width <= 0 or height <= 0:
        line1.points = []
        line2.points = []
        return

    margin = min(width, height) * 0.2
    x1 = image.x + margin
    x2 = image.x + width - margin
    y1 = image.y + margin
    y2 = image.y + height - margin
    line1.points = [x1, y1, x2, y2]
    line2.points = [x1, y2, x2, y1]
    stroke = max(min(width, height) * 0.05, 2.0)
    line1.width = stroke
    line2.width = stroke


__all__ = [
    "generate_fixation_tone",
    "play_fixation_tone",
    "run_fixation_sequence",
]
