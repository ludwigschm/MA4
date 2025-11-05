"""Reusable Kivy widget classes for the tabletop application."""

import os

from kivy.graphics import PopMatrix, PushMatrix, Rotate
from kivy.properties import ListProperty, StringProperty
from kivy.uix.behaviors import ButtonBehavior
from kivy.uix.button import Button
from kivy.uix.image import Image
from kivy.uix.label import Label


class RotatableLabel(Label):
    """Label, das rotiert werden kann (z.B. 180° für die obere Tisch-Seite)."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.rotation_angle = 0
        with self.canvas.before:
            self._push_matrix = PushMatrix()
            self._rotation = Rotate(angle=0, origin=self.center)
        with self.canvas.after:
            self._pop_matrix = PopMatrix()
        self.bind(pos=self._update_transform, size=self._update_transform)

    def set_rotation(self, angle: float):
        self.rotation_angle = angle
        self._update_transform()

    def _update_transform(self, *args):
        if hasattr(self, "_rotation"):
            self._rotation.origin = self.center
            self._rotation.angle = self.rotation_angle


class CardWidget(Button):
    """Karten-Slot: zeigt back_stop bis aktiv und/oder aufgedeckt."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.live = False
        self.face_up = False
        self.front_image = ASSETS['cards']['back']
        self.border = (0, 0, 0, 0)
        self.background_normal = ASSETS['cards']['back_stop']
        self.background_down = ASSETS['cards']['back_stop']
        self.background_disabled_normal = ASSETS['cards']['back_stop']
        self.background_disabled_down = ASSETS['cards']['back_stop']
        self.disabled_color = (1, 1, 1, 1)
        self.update_visual()

    def set_live(self, v: bool):
        self.live = v
        self.disabled = not v
        self.update_visual()

    def flip(self):
        if not self.live:
            return
        self.face_up = True
        self.set_live(False)

    def reset(self):
        self.live = False
        self.face_up = False
        self.disabled = True
        self.update_visual()

    def set_front(self, img_path: str):
        self.front_image = img_path
        if not os.path.exists(img_path):
            self.front_image = ASSETS['cards']['back']
        self.update_visual()

    def update_visual(self):
        if self.face_up:
            img = self.front_image
        elif self.live:
            img = ASSETS['cards']['back']
        else:
            img = ASSETS['cards']['back_stop']
        self.background_normal = img
        self.background_down = img
        self.background_disabled_normal = img
        self.background_disabled_down = img
        self.opacity = 1.0 if (self.live or self.face_up) else 0.55


class IconButton(ButtonBehavior, Image):
    """Button, der automatisch live/stop-Grafiken nutzt."""

    source_normal = StringProperty("")
    source_down = StringProperty("")
    asset_pair = ListProperty([])

    def __init__(self, **kw):
        self.live = False
        self.selected = False
        self.rotation_angle = 0
        super().__init__(**kw)
        if not getattr(self, "fit_mode", None):
            self.fit_mode = "contain"
        with self.canvas.before:
            self._push_matrix = PushMatrix()
            self._rotation = Rotate(angle=0, origin=self.center)
        with self.canvas.after:
            self._pop_matrix = PopMatrix()
        self.bind(pos=self._update_transform, size=self._update_transform)
        if getattr(self, "state", "normal") == "down" and getattr(self, "source_down", ""):
            self.source = self.source_down
        else:
            if getattr(self, "source_normal", ""):
                self.source = self.source_normal
        self.update_visual()

    def on_state(self, instance, value):
        # Kein super().on_state(...) aufrufen!
        # Umschalten der Icon-Quelle je nach Zustand
        if getattr(self, "source_down", ""):
            self.source = self.source_down if value == "down" else self.source_normal
        else:
            # Falls nur source_normal gesetzt ist, bleib bei dieser
            if getattr(self, "source_normal", ""):
                self.source = self.source_normal

    def on_disabled(self, *args):
        # Kein super()-Aufruf; Basisklassen implementieren das nicht stabil.
        self.opacity = 0.5 if self.disabled else 1.0

    def on_source_normal(self, *args):
        if not self.source_down:
            self.source = self.source_normal
        self.update_visual()

    def on_source_down(self, *args):
        self.update_visual()

    def on_asset_pair(self, _instance, value):
        normal = ""
        down = ""
        if isinstance(value, dict):
            normal = value.get("normal") or value.get("stop") or ""
            down = value.get("down") or value.get("live") or ""
        else:
            try:
                normal = value[0] if len(value) > 0 else ""
                down = value[1] if len(value) > 1 else ""
            except TypeError:
                normal = ""
                down = ""
        self.source_normal = normal
        self.source_down = down
        self._apply_sources()

    def set_live(self, v: bool):
        self.live = v
        self.disabled = not v
        self.update_visual()

    def set_pressed_state(self):
        # nach Auswahl bleibt die live-Grafik sichtbar, ohne dass der Button live bleibt
        self.selected = True
        self.live = False
        self.disabled = True
        self.update_visual()

    def reset(self):
        self.selected = False
        self.live = False
        self.disabled = True
        self.update_visual()

    def set_rotation(self, angle: float):
        self.rotation_angle = angle
        self._update_transform()

    def _update_transform(self, *args):
        if hasattr(self, "_rotation"):
            self._rotation.origin = self.center
            self._rotation.angle = self.rotation_angle

    def _apply_sources(self):
        if self.live or self.selected:
            self.source = self.source_down or self.source_normal
        elif self.state == "down" and self.source_down:
            self.source = self.source_down
        else:
            self.source = self.source_normal

    def update_visual(self):
        self._apply_sources()
        if self.disabled:
            self.opacity = 0.5
        else:
            self.opacity = 1.0 if (self.live or self.selected) else 0.6
