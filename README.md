Folgende Pakete müssen installiert werden: 

kivy	
pandas
numpy	
opencv-contrib-python
sounddevice


pip install kivy pandas numpy opencv-contrib-python sounddevice

## Event-Zeitmodell & Refinement

Die Tabletop-App vergibt jetzt für jedes UI-Ereignis sofort eine eindeutige
`event_id` sowie einen hochauflösenden Zeitstempel (`t_local_ns`) auf Basis von
`time.perf_counter_ns()`. Diese Metadaten werden zusammen mit
`mapping_version`, `origin_device` und `provisional=True` an beide Pupil-Brillen
geschickt. Der `PupilBridge` stellt sicher, dass alle Events – auch Fallbacks –
dieses Format einhalten und bietet mit `refine_event(...)` eine API zum späteren
Verfeinern.

Der neue `TimeReconciler` läuft im Hintergrund, verarbeitet regelmäßige
`sync.heartbeat`-Marker und schätzt daraus Offset und Drift je Gerät (robuste
lineare Regression mit Huber-Loss). Sobald verlässliche Parameter vorliegen,
werden alle `provisional`-Events mit einem gemeinsamen Referenzzeitpunkt
(`t_ref_ns`) aktualisiert. Die Ergebnisse landen sowohl über die Bridge (Cloud)
als auch lokal in den Session-Datenbanken (`logs/events_<session>.sqlite3`)
innerhalb der Tabelle `event_refinements`.

Einen schnellen Smoke-Test liefert:

```bash
python -m tabletop.app --demo
```

Der Demo-Lauf simuliert Button-Events sowie Heartbeats, zeigt den Übergang von
„provisional“ → „refined“ mit `event_id`, aktueller `mapping_version`,
Konfidenz und Queue-Last direkt in der Konsole und erzeugt eine Demo-Datenbank
(`logs/demo_refinement.sqlite3`).
