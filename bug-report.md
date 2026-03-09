# Pub/Sub: Messages NACKed to dead-letter queue during graceful shutdown due to premature subscription close

## In what area(s)?

/area runtime

## What version of Dapr?

> 1.16.9

## Expected Behavior

When a pod receives SIGTERM and `block-shutdown-duration` is configured, queued pub/sub messages that have not yet been delivered to the application should either:

1. Remain on the broker queue (requeued) so another consumer can pick them up, or
2. Not be fetched from the broker at all once shutdown begins.

Messages should never be routed to the dead-letter queue during a graceful shutdown when the application has done nothing wrong.

## Actual Behavior

Queued messages are NACKed **without requeue** during graceful shutdown, sending them to the dead-letter queue. This happens because of a race condition in the runtime's shutdown sequence:

1. SIGTERM arrives. The runtime calls `StopAllSubscriptionsForever()` **immediately**, setting `subscription.closed = true` — before the `block-shutdown-duration` wait.
2. The currently in-flight event drains correctly (protected by `wg.Wait()`).
3. When the in-flight event's ACK frees the `prefetchCount=1` slot, the broker delivers the next queued message(s). But the subscription handler sees `s.closed == true` and instantly returns `errors.New("subscription is closed")`.
4. The RabbitMQ component's `handleMessage` calls `d.Nack(false, false)` (no requeue). With `enableDeadLetter: true`, the NACKed messages route to the DLX/DLQ.
5. The subscription context is only canceled ~400ms later (after `Stop()` finishes), which is when the consumer channel actually closes. During that window, multiple messages are delivered and NACKed.

Relevant error from daprd logs:

```
level=error msg="rabbitmq pub/sub error: handling message from topic 'incoming-events', subscription is closed"
```

**Key insight:** `block-shutdown-duration` keeps the Dapr HTTP/gRPC API alive so the app can still publish, but it does **not** keep subscriptions alive. Subscriptions are stopped immediately on SIGTERM regardless of `block-shutdown-duration`.

**Relevant source locations:**
- `pkg/runtime/runtime.go` ~L467-481: `StopAllSubscriptionsForever` called before the block-shutdown select
- `pkg/runtime/subscription/subscription.go` ~L145-146: `s.closed` check returns error
- `pkg/runtime/subscription/subscription.go` ~L382-404: `Stop()` with ~400ms window before context cancelation

## Steps to Reproduce the Problem

Full reproduction repo with one-command setup: https://github.com/kayvonkhosrowpour/dapr-repro

**Setup:** A Go app subscribes to a RabbitMQ topic via Dapr declarative subscription. Each event takes ~20s to process. The Dapr sidecar is configured with `block-shutdown-duration: 60s`, app health checks (interval 3s, threshold 2), `prefetchCount: 1`, and `enableDeadLetter: true`.

```bash
# 1. Start minikube and deploy everything (Dapr 1.16.9, RabbitMQ, demo app)
minikube start --kubernetes-version=v1.33.0
skaffold run

# 2. Tail logs
stern -n dapr-repro dapr-repro

# 3. Publish 8 events to create a backlog
for i in $(seq 1 8); do
  kubectl exec -n dapr-repro deploy/dapr-repro -c dapr-repro -- \
    wget -qO- --post-data="{\"id\":\"$i\"}" \
    --header='Content-Type: application/json' \
    http://localhost:3500/v1.0/publish/pubsub/incoming-events
done

# 4. Wait for tick logs to appear, then kill the pod
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

**Observe:** The in-flight event drains correctly, but immediately after, multiple `"subscription is closed"` errors appear and those messages land in the `dlq-dapr-repro-incoming-events` queue (visible in the RabbitMQ management UI at `localhost:15672`, credentials `rabbit`/`rabbit`).

## Release Note

RELEASE NOTE: **FIX** Pub/sub subscriptions are now stopped after `block-shutdown-duration` expires instead of immediately on SIGTERM, preventing queued messages from being incorrectly NACKed to the dead-letter queue during graceful shutdown.
