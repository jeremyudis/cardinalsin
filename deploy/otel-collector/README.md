# OpenTelemetry Collector Wiring

This collector receives OTLP telemetry from CardinalSin services and forwards it to the ingester OTLP gRPC receiver.

## Local docker-compose

- Collector config: `deploy/otel-collector/config.yaml`
- Service name: `otel-collector`
- Upstream export target: `ingester:4317`

Bring up the stack:

```bash
docker-compose -f deploy/docker-compose.yml up --build
```

## Kubernetes wiring

Apply collector resources:

```bash
kubectl apply -f deploy/kubernetes/otel-collector.yaml
```

The ingester, query, and compactor deployments export OTLP to `http://otel-collector:4317`.

## Troubleshooting

1. Check collector health:
   - `curl http://localhost:13133/`
2. Check collector logs for export retries/backoff:
   - `docker logs cardinalsin-otel-collector`
3. Confirm ingester OTLP gRPC endpoint is reachable:
   - `docker exec cardinalsin-otel-collector sh -c "nc -z ingester 4317"`
4. Validate service env wiring:
   - `docker exec cardinalsin-query env | grep OTEL_`
