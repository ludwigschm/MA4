# Latency & Sync Leitfaden

## Prioritäten
- Verwende `priority="high"` für alle `sync.*`- und `fix.*`-Events. Diese Events werden sofort dispatcht und umgehen Batching komplett.
- Normale Events laufen über die Batch-Queue (`priority="normal"`). Sie profitieren vom reduzierten Fenster (`~5 ms`) und der Batch-Größe (4 Events).

## Doppel-Marker
- Kritische Marker werden doppelt gesendet:
  - Primärmarker (`sync.*`/`fix.*`) mit Gerätestempel.
  - Host-Spiegel (`sync.host_ns`) mit Host-Timestamp (`t_host_ns`) und derselben `event_id`.
- Der TimeReconciler verknüpft beide Einträge zu einem Sync-Paar und gewichtet diese stärker als reguläre Offset-Samples.

## RMS & Confidence
- Die Mapping-Logs enthalten `rms=…`, `rms_ns=…`, `samples`, `slope_mode`, `offset_sign` und `confidence`.
- `rms_ns` beschreibt den quadratischen Fehler im Nanosekundenbereich. Sinkende Werte deuten auf eine stabile Verbindung hin.
- `confidence` wird dynamisch an den RMS gebunden. Werte ≥ `0.8` aktivieren automatische Refines.
- `offset_sign` bleibt stabil, bis mehrere hochwertige Samples (inkl. Host-Mirror) eine Umkehr unterstützen.

## Batch-Parameter anpassen
- Standardwerte: Fenster `5 ms`, Batch-Größe `4`.
- Umgebungsvariablen:
  - `EVENT_BATCH_WINDOW_MS` – neues Fenster in Millisekunden.
  - `EVENT_BATCH_SIZE` – neue Batch-Größe (Minimum 1).
- `LOW_LATENCY_DISABLED=1` deaktiviert die Queue komplett (alle Events werden synchron gesendet).
- `PERF_LOGGING=1` aktiviert Latenzlogs mit `t_ui_ns`, `t_enqueue_ns` und `t_dispatch_ns`.
