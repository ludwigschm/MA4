# Latency Baseline

Das Skript [`tools/latency_baseline.py`](../../tools/latency_baseline.py) berechnet
auf Basis von vorhandenen Sync-Logs eine Baseline der gemessenen Latenz. Es
sucht nach JSON-Zeilen mit Host-/Device-Timestamps oder expliziten Offsets und
aggregiert daraus Kennzahlen über ein 60-Sekunden-Fenster.

## Aufruf

```bash
python tools/latency_baseline.py [<pfad-oder-datei> ...]
```

- Werden keine Pfade angegeben, durchsucht das Skript `docs/latency` nach
  `.jsonl`-, `.json`- oder `.log`-Dateien.
- Mit `--window <sekunden>` lässt sich die Fenstergröße anpassen.
- Mit `--json-only` kann die textuelle Ausgabe deaktiviert werden, sodass nur
  die JSON-Zusammenfassung ausgegeben wird.

## Ausgabe

Neben einer menschenlesbaren Zusammenfassung auf der Konsole erzeugt das Skript
auch ein JSON-Objekt mit folgenden Kennzahlen:

- `sample_count`: Anzahl der ausgewerteten Samples im Fenster
- `window_s`: Größe des betrachteten Fensters in Sekunden
- `duration_s`: Tatsächlich abgedeckter Zeitraum (falls Zeitinformationen
  vorhanden sind)
- `mean_offset_ns`: Mittelwert des Offsets in Nanosekunden
- `median_offset_ns`: Median des Offsets in Nanosekunden
- `rms_offset_ns`: Quadratischer Mittelwert (RMS) des Offsets in Nanosekunden
- `drift_ns_per_s`: Geschätzte Drift in Nanosekunden pro Sekunde (falls
  ableitbar)
