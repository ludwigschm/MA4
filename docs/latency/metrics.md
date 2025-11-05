# Latenzmetriken

Das Modul `metrics.latency_metrics` stellt einen zentralen Sammelpunkt für die wichtigsten Kenngrößen des Zeitabgleichs bereit. Die erfassten Werte basieren auf den Ergebnissen des `TimeReconciler` und werden laufend aktualisiert, protokolliert und exportiert.

## Laufende Kennzahlen

Für jeden Spieler (bzw. jede Quelle) wird der aktuell geschätzte Offset, die Drift sowie die Streuung gepflegt:

- `offset_ns`: Mittelwert des Abstands zwischen Host- und Gerätezeit in Nanosekunden.
- `slope_ppb`: Zeitdrift in Parts per Billion (positiv = Gerät läuft schneller).
- `rms_ns`: Quadratischer Mittelwert (RMS) der Residuen in Nanosekunden.
- `samples`: Anzahl der Samples, die in die aktuelle Schätzung einfließen.

Die Werte werden automatisch aktualisiert, sobald der `TimeReconciler` neue Messwerte verarbeitet.

## Periodisches Logging

Alle fünf Sekunden schreibt der Hintergrund-Logger eine Zusammenfassung in das reguläre Log. Beispiel:

```
latency VP1: offset=0.120ms slope=+1.500ppb rms=0.380ms samples=42 age=0.5s | VP2: offset=-0.085ms …
```

Über die Umgebungsvariable `LATENCY_METRICS_INTERVAL` lässt sich das Logging-Intervall optional anpassen (Standard: 5 Sekunden).

## Export (JSONL & CSV)

Bei jedem Log-Durchlauf wird zusätzlich ein Export durchgeführt. Die Dateien liegen unterhalb von `runs/<timestamp>/` und enthalten pro Messpunkt eine Zeile:

- `latency_metrics.jsonl`: JSON-Zeilenformat für die Weiterverarbeitung in Analysewerkzeugen.
- `latency_metrics.csv`: Kompaktes CSV mit denselben Spalten für Tabellenkalkulationen.

Der Zielordner wird beim ersten Zugriff automatisch erzeugt. Über den Konstruktor von `LatencyMetrics` lässt sich ein alternativer Root-Pfad übergeben (z. B. für Tests).

## Mini-Panel (Kivy)

Für das Tabletop-UI steht ein unaufdringliches Panel zur Verfügung, das die aktuellen Offset- und Driftwerte in der rechten oberen Ecke darstellt. Das Panel lässt sich über die Umgebungsvariable `SHOW_LATENCY_PANEL` ein- oder ausschalten (`1`/`0`).

Das Panel aktualisiert sich automatisch, sobald neue Daten eintreffen. Ist Kivy nicht verfügbar, wird das Panel stillschweigend deaktiviert.

## Programmatische Nutzung

```python
from metrics import default_latency_metrics

metrics = default_latency_metrics()
current = metrics.snapshot()
for player, values in current["estimates"].items():
    print(player, values["offset_ns"], values["slope_ppb"])
```

Für Tests oder spezielle Auswertungen kann `LatencyMetrics` auch direkt instantiiert und über `export_snapshot()` eine manuelle Sicherung ausgelöst werden.
