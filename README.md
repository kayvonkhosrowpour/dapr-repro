# Dapr RabbitMQ Pub/Sub — DLQ During Graceful Shutdown Reproduction

Minimal reproduction demonstrating that the Dapr RabbitMQ pub/sub component's `handleMessage`
NACKs messages to the dead-letter queue when the handler returns `context.Canceled` during
graceful shutdown.

**Status as of Dapr v1.18.1 / components-contrib v1.18.1 (latest):** The bug is **still present**.
The runtime-level fix landed in dapr v1.14+ (dapr/dapr#9619), changing the error from
`"subscription is closed"` → `"context canceled"`. But `handleMessage` in the RabbitMQ component
still NACKs on any non-nil error, including `context.Canceled`. This repo demonstrates the
remaining components-contrib gap.

Related issues:
- [dapr/dapr#9604](https://github.com/dapr/dapr/issues/9604) — Original runtime-side bug report
- [dapr/dapr#9619](https://github.com/dapr/dapr/pull/9619) — Runtime fix (merged)
- [dapr/components-contrib#4281](https://github.com/dapr/components-contrib/issues/4281) — Component-level bug report (filed Mar 2026, closed as stale — unfixed)

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
- Dapr **v1.18.1** (from the official Helm chart, pinned in `skaffold.yaml`)
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
| Standard Dapr (pre-#9619, v1.16.x) | Multiple | `"subscription is closed"` |
| Dapr v1.18.1 (runtime fix only) | **1** | `"context canceled"` |
| Dapr v1.18.1 + components-contrib fix | **0** | None |

Check the RabbitMQ management UI at http://localhost:15672 (credentials: `rabbit`/`rabbit`).
Look at the `dlq-dapr-repro-incoming-events` queue for messages that were incorrectly NACKed
during shutdown.

**With Dapr v1.18.1 you will observe exactly 1 message in the DLQ** — the one being processed
when the pod was deleted. The daprd log will show:

```
level=error msg="rabbitmq pub/sub error: handling message from topic 'incoming-events', context canceled"
```

## Root Cause

`handleMessage` in `pubsub/rabbitmq/rabbitmq.go` unconditionally NACKs any non-nil error from
the handler, including `context.Canceled`. During graceful shutdown, the runtime (v1.18.1)
blocks on `ctx.Done()` then returns `ctx.Err()` — this flows back as `context.Canceled` and
triggers the NACK.

The fix is a 5-line guard in `handleMessage`. See [bug-report.md](bug-report.md) for full
analysis and the proposed code change.

## Cleanup

```bash
skaffold delete
minikube stop
```
