# OpenTelemetry Collector (Local Dual Publish)

This collector receives OTLP telemetry from CardinalSin services and fans metrics out to:

- CardinalSin ingester OTLP receiver (`ingester:4317`)
- Prometheus remote-write receiver (`prometheus:9090/api/v1/write`)

The same input stream is used for both destinations, so Grafana can compare CardinalSin and Prometheus side-by-side.
