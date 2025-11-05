# Latency configuration

The tabletop bridge exposes a couple of environment variables to tune the
behaviour of the UI event pipeline:

| Variable | Default | Description |
| --- | --- | --- |
| `EVENT_BATCH_WINDOW_MS` | `5` | Maximum batching window in milliseconds before queued UI events are flushed to the device. Set to `0` for immediate dispatch. |
| `EVENT_BATCH_SIZE` | `4` | Maximum number of UI events combined into a single dispatch window before being forwarded. |
| `LOW_LATENCY_DISABLED` | `0` | When set to `1`, batching is fully disabled and events are dispatched immediately regardless of the other settings. |

The values are evaluated at application startup. Invalid inputs fall back to the
respective defaults.
