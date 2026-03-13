# Dapr RabbitMQ Pub/Sub — DLQ During Graceful Shutdown Reproduction

Minimal reproduction demonstrating that the Dapr RabbitMQ pub/sub component's `handleMessage` NACKs messages to the dead-letter queue when the handler returns `context.Canceled` during graceful shutdown.

Related issues:
- [dapr/dapr#9604](https://github.com/dapr/dapr/issues/9604) — Runtime-side fix (PR #9619)
- [dapr/components-contrib bug report](bug-report.md) — The components-contrib gap this repo demonstrates

## Prerequisites

- [minikube](https://minikube.sigs.k8s.io/docs/start/)
- [skaffold](https://skaffold.dev/docs/install/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [stern](https://github.com/stern/stern) (optional, for log tailing)

## Setup

```bash
minikube start --kubernetes-version=v1.33.0
skaffold run
```

This deploys:
- Dapr (from the official Helm chart)
- RabbitMQ (Bitnami, management UI on port 15672)
- A demo Go app subscribed to `incoming-events` via Dapr pub/sub

The app processes each event for ~20 seconds (simulated work). The Dapr sidecar is configured with:
- `block-shutdown-duration: 60s`
- `enable-app-health-check: true` (interval 3s, threshold 2)
- RabbitMQ component: `prefetchCount: 1`, `enableDeadLetter: true`

## Reproduce

```bash
# 1. Tail logs
stern -n dapr-repro dapr-repro

# 2. Publish 8 events to create a backlog
for i in $(seq 1 8); do
  kubectl exec -n dapr-repro deploy/dapr-repro -c dapr-repro -- \
    wget -qO- --post-data="{\"id\":\"$i\"}" \
    --header='Content-Type: application/json' \
    http://localhost:3500/v1.0/publish/pubsub/incoming-events
done

# 3. Wait for processing tick logs to appear (the first event is being processed)

# 4. Delete the pod to trigger graceful shutdown
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

## Expected vs Actual

| Scenario | Messages to DLQ | Error in daprd logs |
|----------|----------------|---------------------|
| Standard Dapr (pre-#9619) | Multiple | `"subscription is closed"` |
| Dapr with #9619 fix | **1** | `"context canceled"` |
| Dapr with #9619 + components-contrib fix | **0** | None |

Check the RabbitMQ management UI at http://localhost:15672 (credentials: `rabbit`/`rabbit`). Look at the `dlq-dapr-repro-incoming-events` queue for messages that were incorrectly NACKed during shutdown.

## Root Cause

`handleMessage` in `pubsub/rabbitmq/rabbitmq.go` unconditionally NACKs any non-nil error from the handler, including `context.Canceled`. During graceful shutdown, the runtime's subscription handler (with #9619 applied) blocks on `ctx.Done()` then returns `ctx.Err()` — this flows back as `context.Canceled` and triggers the NACK.

## Proposed Fix

See [bug-report.md](bug-report.md) for the full analysis and proposed code change.

## Cleanup

```bash
skaffold delete
minikube stop
```
