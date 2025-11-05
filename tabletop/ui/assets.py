"""Asset management helpers for the tabletop UI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from kivy.core.image import Image as CoreImage

from tabletop.data.config import CARD_DIR, UX_DIR


# --- File locations -------------------------------------------------------

BACKGROUND_IMAGE: Path = UX_DIR / "Hintergrund.png"
FIX_STOP_IMAGE: Path = UX_DIR / "fix_stop.png"
FIX_LIVE_IMAGE: Path = UX_DIR / "fix_live.png"


# --- Asset catalogue ------------------------------------------------------

ASSETS: Dict[str, Any] = {
    "play": {
        "live": str(UX_DIR / "play_live.png"),
        "stop": str(UX_DIR / "play_stop.png"),
    },
    "signal": {
        "low": {
            "live": str(UX_DIR / "tief_live.png"),
            "stop": str(UX_DIR / "tief_stop.png"),
        },
        "mid": {
            "live": str(UX_DIR / "mittel_live.png"),
            "stop": str(UX_DIR / "mittel_stop.png"),
        },
        "high": {
            "live": str(UX_DIR / "hoch_live.png"),
            "stop": str(UX_DIR / "hoch_stop.png"),
        },
    },
    "decide": {
        "bluff": {
            "live": str(UX_DIR / "bluff_live.png"),
            "stop": str(UX_DIR / "bluff_stop.png"),
        },
        "wahr": {
            "live": str(UX_DIR / "wahr_live.png"),
            "stop": str(UX_DIR / "wahr_stop.png"),
        },
    },
    "cards": {
        "back": str(CARD_DIR / "back.png"),
        "back_stop": str(CARD_DIR / "back_stop.png"),
    },
}


def resolve_background_texture():
    """Load the tabletop background texture if available."""

    if not BACKGROUND_IMAGE.exists():
        return None
    try:
        return CoreImage(str(BACKGROUND_IMAGE)).texture
    except Exception:
        return None


__all__ = [
    "ASSETS",
    "BACKGROUND_IMAGE",
    "FIX_STOP_IMAGE",
    "FIX_LIVE_IMAGE",
    "resolve_background_texture",
]

