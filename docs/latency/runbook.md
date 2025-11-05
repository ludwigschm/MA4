# Latenz Quick-Tests

Dieses Runbook beschreibt den schnellen Abgleich der neuen CLI-Tools für
Latenz-Messungen. Alle Befehle werden aus dem Repository-Root ausgeführt.

## Smoke-Test ausführen

```bash
python Makefile/tasks.py latency-smoke
```

Der Task erzeugt zunächst zehn Doppel-Marker mit synthetischen Daten und wertet
den `TimeReconciler` aus. Anschließend wird die generierte JSONL-Datei mit
`replay_events_shift.py` transformiert, um die Offset/Slope-Abbildung zu testen.
Im Terminal erscheint eine kurze Vorschau der verschobenen Events.

## Erwartete Werte

* `emit_test_markers.py` meldet einen Offset von etwa **12,5 ms** und eine
  Drift von rund **+5 ppm** (entspricht der Standardkonfiguration).
* Die verschobenen Events aus `replay_events_shift.py` zeigen `_ns`-Felder, die
  um **5 ms** erhöht und mit einer Slope von **0,999999** skaliert wurden.

## Einzelne Werkzeuge

### Doppel-Marker senden

```bash
python tools/emit_test_markers.py --output /tmp/markers.jsonl
```

Wichtige Parameter:

* `--offset-ns`: Erwarteter Host→Geräte-Offset in Nanosekunden.
* `--drift-ppm`: Gewünschte Drift in ppm (wird in eine Slope konvertiert).
* `--jitter-ns`: Optionales Rauschen, um realistische Schwankungen zu simulieren.

### Ereignisse verschieben

```bash
python tools/replay_events_shift.py /tmp/markers.jsonl /tmp/markers_shifted.jsonl   --offset-ns 5_000_000 --slope 0.999999
```

Das Skript passt alle Felder mit dem Suffix `_ns` rekursiv an. Die transformierte
Datei kann anschließend für weitere Analysen verwendet werden.
