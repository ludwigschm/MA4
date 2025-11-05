# requirements:
#   pip install pyqt6 opencv-contrib-python
#
# Nutzung (kompatibel zu deinem Game):
#   # Alte Art (unbedingt 12 IDs übergeben, sonst werden nur die ersten Positionen belegt):
#   MarkerOverlay(geo, marker_ids=[
#       1, 55, 71, 7, 23, 89, 101, 37, 117, 133, 147, 163
#   ])
#   # Empfohlen (feste Zuordnung, siehe MARKER_LAYOUT):
#   MarkerOverlay(geo, layout=MARKER_LAYOUT)
#
# Tasten im Overlay:
#   M   -> Marker ein/ausblenden
#   +   -> Marker um +5% größer (nur wenn USE_FIXED_SIZE=False)
#   -   -> Marker um -5% kleiner (nur wenn USE_FIXED_SIZE=False)
#   Esc -> Programm beenden

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from PyQt6.QtWidgets import QApplication, QLabel, QMainWindow
from PyQt6.QtGui import QPixmap, QImage, QKeyEvent
from PyQt6.QtCore import Qt, QRect, QTimer
import cv2
import numpy as np

if TYPE_CHECKING:
    from PyQt6.QtGui import QScreen

# -------------------- EMPFOHLENE IDs & Positionen ----------------------------
# Robuste, weit auseinanderliegende AprilTag-IDs (tag36h11)
MARKER_LAYOUT: Dict[str, int] = {
    "top_left":            1,
    "top_inner_left":      55,
    "top_inner_right":     71,
    "top_right":           7,
    "bottom_left":         23,
    "bottom_inner_left":   89,
    "bottom_inner_right":  101,
    "bottom_right":        37,
    "left_inner_top":      117,
    "left_inner_bottom":   133,
    "right_inner_top":     147,
    "right_inner_bottom":  163,
}
# Reihenfolge der Platzierung (und Mapping-Reihenfolge für marker_ids)
POSITION_ORDER: List[str] = [
    "top_left", "top_inner_left", "top_inner_right", "top_right",
    "bottom_left", "bottom_inner_left", "bottom_inner_right", "bottom_right",
    "left_inner_top", "left_inner_bottom", "right_inner_top", "right_inner_bottom",
]

# -------------------- RENDER-PARAMETER ---------------------------------------
APRILTAG_DICT = cv2.aruco.DICT_APRILTAG_36h11
QUIET_ZONE_RATIO = 0.08                          # Weißer Rand (schmaler gemacht)
BG_WHITE_CSS = "background: white;"
# WAR: LABEL_CSS = "background: white; color: black; font: 12pt 'Segoe UI';"
# NEU: transparent, damit kein weißes Feld unterhalb sichtbar ist
LABEL_CSS   = "background: transparent; color: black; font: 12pt 'Segoe UI';"

# Markergröße: entweder FIX (deterministisch) ODER prozentual
USE_FIXED_SIZE = True
TARGET_CM = 6.0
FALLBACK_SIZE_PX = 240                              # ≈6 cm @ ~100 PPI (43" 4K Referenz)
SIZE_PERCENT = 0.16                                # falls USE_FIXED_SIZE=False
MIN_SIZE_PX = 160
MAX_SIZE_PX = 560


def _calculate_fixed_size(screen: Optional["QScreen"]) -> int:
    """Return the fixed marker size in pixels for the given screen."""

    ppi: Optional[float] = None
    fallback_reason = "no screen information"

    if screen is not None:
        for attr in (
            "physicalDotsPerInch",
            "physicalDotsPerInchX",
            "logicalDotsPerInch",
        ):
            getter = getattr(screen, attr, None)
            if callable(getter):
                try:
                    value = float(getter())
                except Exception:
                    continue
                if value > 0:
                    ppi = value
                    break

        if ppi is None:
            try:
                geom = screen.geometry()
                physical = screen.physicalSize()
                width_accessor = getattr(physical, "width", None)
                if callable(width_accessor):
                    width_mm = float(width_accessor())
                elif width_accessor is not None:
                    width_mm = float(width_accessor)
                else:
                    width_mm = 0.0

                if width_mm > 0:
                    ppi = geom.width() / (width_mm / 25.4)
                else:
                    fallback_reason = "physical width unavailable"
            except Exception:
                fallback_reason = "screen geometry unavailable"

    if ppi and ppi > 0:
        size_px = int(round(ppi * (TARGET_CM / 2.54)))
        print(
            f"ArUco overlay: target screen ≈ {ppi:.1f} PPI, target size {size_px}px for {TARGET_CM:.1f} cm"
        )
        return size_px

    print(
        "ArUco overlay: PPI unavailable"
        f" ({fallback_reason}), using fallback size {FALLBACK_SIZE_PX}px (~{TARGET_CM:.1f} cm)"
    )
    return int(FALLBACK_SIZE_PX)

# -------------------- TAG-RENDERING ------------------------------------------
def generate_apriltag_qpixmap(tag_id: int, size: int, quiet_zone_ratio: float = QUIET_ZONE_RATIO) -> QPixmap:
    """Render AprilTag in weißem Quadrat (size x size) mit Quiet-Zone."""
    size = int(size)
    q = max(0.05, min(quiet_zone_ratio, 0.40))      # clamp 5..40%
    inner = int(round(size * (1.0 - 2.0 * q)))
    inner = max(32, inner)

    canvas = np.full((size, size), 255, dtype=np.uint8)  # weiß
    aruco_dict = cv2.aruco.getPredefinedDictionary(APRILTAG_DICT)
    tag_img = np.zeros((inner, inner), dtype=np.uint8)   # schwarz
    cv2.aruco.generateImageMarker(aruco_dict, tag_id, inner, tag_img, 1)

    y0 = (size - inner) // 2
    x0 = (size - inner) // 2
    canvas[y0:y0 + inner, x0:x0 + inner] = tag_img

    qimg = QImage(canvas.data, size, size, size, QImage.Format.Format_Grayscale8)
    return QPixmap.fromImage(qimg)

# -------------------- OVERLAY-FENSTER ----------------------------------------
class MarkerOverlay(QMainWindow):
    def __init__(
        self,
        screen_geometry: QRect,
        layout: Optional[Dict[str, int]] = None,
        marker_ids: Optional[List[int]] = None,   # Abwärtskompatibel
        *,
        screen: Optional["QScreen"] = None,
    ):
        """
        Entweder 'layout' übergeben (empfohlen) ODER 'marker_ids' (werden in POSITION_ORDER gemappt).
        """
        super().__init__()
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.Tool |
            Qt.WindowType.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setStyleSheet(BG_WHITE_CSS)
        self.setGeometry(screen_geometry)
        self._target_screen = screen

        # --- Eingabe normalisieren ---
        if layout is not None:
            self.layout: Dict[str, int] = {name: layout[name] for name in POSITION_ORDER if name in layout}
        elif marker_ids is not None:
            n = min(len(marker_ids), len(POSITION_ORDER))
            self.layout = {POSITION_ORDER[i]: int(marker_ids[i]) for i in range(n)}
        else:
            # Default: alle 12 empfohlenen Marker
            self.layout = {name: MARKER_LAYOUT[name] for name in POSITION_ORDER}

        self.pos_order: List[str] = [name for name in POSITION_ORDER if name in self.layout]

        self.marker_labels: List[QLabel] = []
        self.text_labels: List[QLabel] = []
        self.markers_visible = True

        self._pixmap_cache: Dict[Tuple[int, int], QPixmap] = {}
        self._layout_timer = QTimer(self)
        self._layout_timer.setSingleShot(True)
        self._layout_timer.setInterval(33)
        self._layout_timer.timeout.connect(self._perform_layout_update)
        self._layout_pending = False

        # Größen-Parameter
        self.size_percent = SIZE_PERCENT
        self.min_size = MIN_SIZE_PX
        self.max_size = MAX_SIZE_PX
        self.use_fixed = USE_FIXED_SIZE
        self.fixed_size = _calculate_fixed_size(screen)

        # UI-Objekte
        for _ in self.pos_order:
            lab = QLabel(self)
            lab.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            lab.setStyleSheet(BG_WHITE_CSS)    # weißes Label = sichere Quiet-Zone
            lab.setScaledContents(False)
            self.marker_labels.append(lab)

            txt = QLabel(self)
            txt.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
            txt.setStyleSheet(LABEL_CSS)       # transparent -> keine weiße Leiste
            txt.hide()                         # direkt verstecken
            self.text_labels.append(txt)

        # Zuordnung ausgeben & speichern
        print("Feste Marker-Zuordnung (Position → ID):")
        for name in self.pos_order:
            print(f"  {name:12s} -> {self.layout[name]}")
        try:
            mapping_path = os.path.join(os.getcwd(), "marker_layout.json")
            with open(mapping_path, "w", encoding="utf-8") as f:
                json.dump({name: int(self.layout[name]) for name in self.pos_order}, f, ensure_ascii=False, indent=2)
            print(f"(Gespeichert als {mapping_path})")
        except Exception as e:
            print(f"Warnung: Konnte marker_layout.json nicht schreiben: {e}")

        self._request_layout_update()

    @staticmethod
    def _positions_full(w: int, h: int, msize: int, margin: int) -> Dict[str, Tuple[int, int]]:
        """Return pixel positions for all supported marker locations."""

        def _linspace(start: int, end: int, count: int) -> List[int]:
            if count <= 1:
                return [start]
            span = end - start
            return [int(round(start + span * i / (count - 1))) for i in range(count)]

        positions: Dict[str, Tuple[int, int]] = {}

        top_keys = ["top_left", "top_inner_left", "top_inner_right", "top_right"]
        top_x = _linspace(margin, max(margin, w - margin - msize), len(top_keys))
        for key, x in zip(top_keys, top_x):
            positions[key] = (x, margin)

        bottom_keys = ["bottom_left", "bottom_inner_left", "bottom_inner_right", "bottom_right"]
        bottom_x = _linspace(margin, max(margin, w - margin - msize), len(bottom_keys))
        bottom_y = h - margin - msize
        for key, x in zip(bottom_keys, bottom_x):
            positions[key] = (x, bottom_y)

        left_keys = ["top_left", "left_inner_top", "left_inner_bottom", "bottom_left"]
        left_y = _linspace(margin, max(margin, h - margin - msize), len(left_keys))
        for key, y in zip(left_keys, left_y):
            if key in ("top_left", "bottom_left"):
                continue
            positions[key] = (margin, y)

        right_keys = ["top_right", "right_inner_top", "right_inner_bottom", "bottom_right"]
        right_y = _linspace(margin, max(margin, h - margin - msize), len(right_keys))
        right_x = w - margin - msize
        for key, y in zip(right_keys, right_y):
            if key in ("top_right", "bottom_right"):
                continue
            positions[key] = (right_x, y)

        return positions

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._request_layout_update()

    def keyPressEvent(self, event: QKeyEvent):
        if event.key() == Qt.Key.Key_M:
            self.toggle_markers()
        elif event.key() in (Qt.Key.Key_Plus, Qt.Key.Key_Equal):
            if self.use_fixed:
                event.accept()
                return
            self.size_percent *= 1.05
            self._request_layout_update()
        elif event.key() == Qt.Key.Key_Minus:
            if self.use_fixed:
                event.accept()
                return
            self.size_percent /= 1.05
            self._request_layout_update()
        elif event.key() == Qt.Key.Key_Escape:
            QApplication.instance().quit()

    def toggle_markers(self):
        self.markers_visible = not self.markers_visible
        self._request_layout_update()

    def _request_layout_update(self) -> None:
        if self._layout_pending:
            return
        self._layout_pending = True
        self._layout_timer.start()

    def _perform_layout_update(self) -> None:
        self._layout_pending = False
        self._layout_and_render_markers()

    def _layout_and_render_markers(self):
        w = max(1, self.width())
        h = max(1, self.height())

        # Markergröße
        if self.use_fixed:
            msize = int(self.fixed_size)
        else:
            base = int(min(w, h) * self.size_percent)
            msize = max(self.min_size, min(base, self.max_size))

        margin = max(6, int(msize * 0.08))  # Abstand zum Rand
        pos_map = self._positions_full(w, h, msize, margin)

        # Alle Labels erst verstecken, dann neu zeichnen
        for lab, txt in zip(self.marker_labels, self.text_labels):
            lab.setVisible(False)
            txt.setVisible(False)

        for (name, tag_id), lab in zip(
            [(n, self.layout[n]) for n in self.pos_order],
            self.marker_labels,
        ):
            x, y = pos_map[name]
            lab.resize(msize, msize)
            lab.move(x, y)
            cache_key = (tag_id, msize)
            pixmap = self._pixmap_cache.get(cache_key)
            if pixmap is None:
                pixmap = generate_apriltag_qpixmap(tag_id, msize, QUIET_ZONE_RATIO)
                self._pixmap_cache[cache_key] = pixmap
            lab.setPixmap(pixmap)
            lab.setVisible(self.markers_visible)

        # WICHTIG: keine Textlabels setzen/anzeigen -> keine weiße Fläche unterhalb

# -------------------- STANDALONE-TEST ----------------------------------------
def _parse_cli_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the ArUco marker overlay")
    parser.add_argument(
        "--display",
        type=int,
        default=None,
        help="Zero-based display index that should present the overlay.",
    )
    return parser.parse_args(argv)


def _set_process_priority_low() -> None:
    """Lower process priority when supported to reduce scheduling pressure."""

    try:
        os.nice(5)
        return
    except AttributeError:
        pass
    except OSError:
        return

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        IDLE_PRIORITY_CLASS = 0x00000040
        kernel32.SetPriorityClass(kernel32.GetCurrentProcess(), IDLE_PRIORITY_CLASS)
    except Exception:
        pass


def main(argv: Optional[List[str]] = None):
    if argv is None:
        argv = sys.argv[1:]

    args = _parse_cli_args(argv)
    app = QApplication([sys.argv[0]])
    _set_process_priority_low()

    # Standard: 8 Marker aus MARKER_LAYOUT
    layout = MARKER_LAYOUT

    overlays: List[MarkerOverlay] = []
    screens = app.screens()
    env_display = os.environ.get("TABLETOP_DISPLAY_INDEX")
    env_display_index: Optional[int] = None
    if env_display is not None:
        try:
            env_display_index = int(env_display)
        except ValueError:
            print(f"Warnung: Ungültiger TABLETOP_DISPLAY_INDEX={env_display!r}, ignoriere Wert")

    target_display: Optional[int] = args.display if args.display is not None else env_display_index

    if not screens:
        geom = QRect(100, 100, 1280, 720)
        win = MarkerOverlay(geom, layout=layout)
        win.show()
        overlays.append(win)
    else:
        default_display = 1 if len(screens) >= 2 else 0
        if target_display is None:
            target_display = default_display
        target_display = max(0, min(target_display, len(screens) - 1))

        screen = screens[target_display]
        geom = screen.geometry()
        win = MarkerOverlay(geom, layout=layout, screen=screen)
        screen_name_attr = getattr(screen, "name", None)
        if callable(screen_name_attr):
            try:
                screen_name = screen_name_attr()
            except Exception:
                screen_name = None
        else:
            screen_name = screen_name_attr

        if screen_name:
            print(f"ArUco overlay: using screen '{screen_name}' (index {target_display})")
        else:
            print(f"ArUco overlay: using display index {target_display}")

        win.showFullScreen()
        overlays.append(win)

    sys.exit(app.exec())

if __name__ == "__main__":
    main()
