# Dapr Graceful Shutdown Repro

Minimal Go application that demonstrates a Dapr pub/sub shutdown race condition
where queued messages land in the dead-letter queue (DLQ) during pod termination,
even when the application correctly implements graceful shutdown with health
checks and `block-shutdown-duration`.

## Prerequisites

| Tool | Tested Version |
|------|---------------|
| [minikube](https://minikube.sigs.k8s.io/) | v1.35.0 |
| [skaffold](https://skaffold.dev/) | v1.39.1 |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | any recent |
| [stern](https://github.com/stern/stern) (optional, for log tailing) | any recent |

Kubernetes must be >= 1.28 for native sidecar support (`dapr.io/enable-native-sidecar`).

## Quick Start

```bash
# Start minikube (if not already running)
minikube start --kubernetes-version=v1.33.0

# Deploy everything: Dapr, RabbitMQ, and the demo app
skaffold run

# Tail logs (in a separate terminal)
stern -n dapr-repro dapr-repro
```

Skaffold deploys three Helm releases:
1. **Dapr** (v1.16.9) into `dapr-system`
2. **RabbitMQ** (bitnami 16.0.14) into `rabbit`
3. **dapr-repro** (this app) into `dapr-repro`

## How It Works

The app subscribes to a RabbitMQ topic (`incoming-events`) via a Dapr
declarative subscription. When an event arrives:

1. The handler logs a tick every 1 second for 20 seconds (simulating work).
2. After 20 seconds, it publishes a completion event to `completed-events`
   via the Dapr HTTP API.
3. It returns `{"status":"SUCCESS"}` to Dapr, which ACKs the message in
   RabbitMQ.

On SIGTERM the app:
1. Sets `/healthz` to return 503 (Dapr detects this and stops routing events).
2. Sleeps 10 seconds for probe propagation.
3. Calls `server.Shutdown()` to drain in-flight handlers (the 20s event).
4. Exits.

The Helm chart configures the Dapr sidecar with:
- `dapr.io/enable-native-sidecar: "true"` (sidecar outlives app container)
- `dapr.io/block-shutdown-duration: "60s"` (Dapr HTTP API stays alive)
- `dapr.io/enable-app-health-check: "true"` with 3s probe interval

## Reproducing the DLQ Issue

### Step 1: Publish Multiple Events

Queue several events so there is a backlog. With `prefetchCount: 1`, Dapr
processes them one at a time (each takes ~20 seconds).

```bash
# From within the cluster
kubectl exec -n dapr-repro deploy/dapr-repro -c dapr-repro -- \
  wget -qO- --post-data='{"id":"1"}' \
  --header='Content-Type: application/json' \
  http://localhost:3500/v1.0/publish/pubsub/incoming-events

# Repeat to queue more events (5-8 is a good number)
for i in $(seq 1 8); do
  kubectl exec -n dapr-repro deploy/dapr-repro -c dapr-repro -- \
    wget -qO- --post-data="{\"id\":\"$i\"}" \
    --header='Content-Type: application/json' \
    http://localhost:3500/v1.0/publish/pubsub/incoming-events
done
```

### Step 2: Delete the Pod Mid-Processing

Wait until the app is processing an event (you'll see the tick logs), then
kill the pod:

```bash
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

### Step 3: Observe the Logs

In the stern output you will see:

1. The in-flight event **correctly drains** (ticks continue, completion
   publishes, handler returns SUCCESS).
2. Immediately after, several `"subscription is closed"` errors appear.
3. Those messages are NACKed to RabbitMQ **without requeue**, sending them
   to the dead-letter queue.

```
daprd  level=error msg="rabbitmq pub/sub error: handling message from topic
       'incoming-events', subscription is closed"
```

### Step 4: Verify Messages in the DLQ

Open the RabbitMQ management UI (port-forwarded by skaffold):

```
http://localhost:15672
# username: rabbit / password: rabbit
```

Navigate to **Queues** and check `dlq-dapr-repro-incoming-events`. You will
see messages that were lost to the DLQ during shutdown.

## Root Cause

The DLQ entries are **not** caused by the application code. They are caused
by an internal race condition in the Dapr runtime:

1. When SIGTERM arrives, the Dapr runtime calls `StopAllSubscriptionsForever()`
   **immediately** (before the `block-shutdown-duration` wait). This sets
   `subscription.closed = true`.

2. The in-flight event is protected by `wg.Wait()` and drains correctly.

3. After the in-flight event's ACK frees the `prefetchCount=1` slot, RabbitMQ
   delivers the next queued messages. But the subscription's handler checks
   `s.closed` and instantly returns `errors.New("subscription is closed")`.

4. The RabbitMQ component's `handleMessage` calls `d.Nack(false, false)`
   (no requeue). With `enableDeadLetter: true`, the NACKed messages route to
   the DLX/DLQ.

5. The subscription context is only canceled ~400ms later (after `Stop()`
   finishes), which is when the consumer channel actually closes. During that
   400ms window, multiple messages can be delivered and NACKed.

Key insight: `block-shutdown-duration` keeps the Dapr **HTTP/gRPC API** alive
(so the app can still publish). It does **not** keep subscriptions alive. The
subscriptions are stopped immediately on SIGTERM regardless of
`block-shutdown-duration`.

## Relevant Dapr Source

- `dapr/pkg/runtime/runtime.go` lines 467-481 (`StopAllSubscriptionsForever`
  called before the block-shutdown select)
- `dapr/pkg/runtime/subscription/subscription.go` lines 145-146
  (`s.closed` check returns the error)
- `dapr/pkg/runtime/subscription/subscription.go` lines 382-404
  (`Stop()` with the 400ms window)
- `components-contrib/pubsub/rabbitmq/rabbitmq.go` lines 624-656
  (`handleMessage` NACKs with `requeue=false`)

## File Layout

```
dapr-repro/
├── main.go          # Go app: chi mux, /healthz, event handler, graceful shutdown
├── go.mod
├── go.sum
├── Dockerfile       # Multi-stage: golang:1.26.0-alpine3.23 → alpine:3.23
├── skaffold.yaml    # Deploys Dapr, RabbitMQ, and this app to minikube
└── chart/
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
        ├── deployment.yaml         # Dapr annotations, native sidecar, health checks
        ├── pubsub-component.yaml   # RabbitMQ pubsub.rabbitmq component
        └── subscription.yaml       # Declarative subscription: incoming-events → /events/incoming
```
