# Dapr SQS Binding Graceful Shutdown Repro

Minimal Go application that tests Dapr's SQS input binding behavior during pod
termination. This is a companion to the RabbitMQ pub/sub repro (see `main`
branch) that demonstrated a race condition causing messages to land in the DLQ.

The hypothesis: because the SQS binding uses pull-based polling (not push),
and `StopReadingFromBindings` directly cancels the polling context, queued
messages should **not** be affected during graceful shutdown — no DLQ, no
visibility timeout penalty for messages still in the queue.

## Prerequisites

| Tool | Tested Version |
|------|---------------|
| [minikube](https://minikube.sigs.k8s.io/) | v1.35.0 |
| [skaffold](https://skaffold.dev/) | v1.39.1 |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | any recent |
| [stern](https://github.com/stern/stern) (optional, for log tailing) | any recent |
| [AWS CLI](https://aws.amazon.com/cli/) | v2 |

Kubernetes must be >= 1.28 for native sidecar support (`dapr.io/enable-native-sidecar`).

You need AWS credentials with permission to send/receive messages on the
`pcsf-scan-dev` queue in `us-west-2`.

## Quick Start

```bash
# Start minikube (if not already running)
minikube start --kubernetes-version=v1.33.0

# Set your AWS credentials in the Helm values before deploying.
# Option A: edit chart/values.yaml directly
# Option B: pass via skaffold overrides
skaffold run --set aws.accessKey=AKIA... --set aws.secretKey=...

# Tail logs (in a separate terminal)
stern -n dapr-repro dapr-repro
```

Skaffold deploys three Helm releases:
1. **Dapr** (v1.16.9) into `dapr-system`
2. **RabbitMQ** (bitnami 16.0.14) into `rabbit` (used for completion events only)
3. **dapr-repro** (this app) into `dapr-repro`

The SQS queue `pcsf-scan-dev` in `us-west-2` must already exist in AWS.

## How It Works

The app receives messages from SQS via a Dapr **input binding** (component
type `bindings.aws.sqs`). Dapr polls SQS and POSTs each message to the app at
`/sqs-incoming`. When a message arrives:

1. The handler logs a tick every 1 second for 20 seconds (simulating work).
2. After 20 seconds, it publishes a completion event to `completed-events`
   via the Dapr pub/sub HTTP API (RabbitMQ).
3. It returns HTTP 200 to Dapr, which deletes the message from SQS.

On SIGTERM the app:
1. Sets `/healthz` to return 503 (Dapr detects this and stops routing events).
2. Sleeps 10 seconds for probe propagation.
3. Calls `server.Shutdown()` to drain in-flight handlers (the 20s event).
4. Exits.

The Helm chart configures the Dapr sidecar with:
- `dapr.io/enable-native-sidecar: "true"` (sidecar outlives app container)
- `dapr.io/block-shutdown-duration: "60s"` (Dapr HTTP API stays alive)
- `dapr.io/enable-app-health-check: "true"` with 3s probe interval

## Reproducing the Test

### Step 1: Send Multiple Messages to SQS

Queue several messages so there is a backlog. The SQS binding fetches one
message at a time (`MaxNumberOfMessages: 1`), so each takes ~20 seconds.

```bash
for i in $(seq 1 8); do
  aws sqs send-message \
    --queue-url https://sqs.us-west-2.amazonaws.com/YOUR_ACCOUNT_ID/pcsf-scan-dev \
    --message-body "{\"id\":\"$i\"}" \
    --region us-west-2
done
```

### Step 2: Delete the Pod Mid-Processing

Wait until the app is processing a message (you'll see the tick logs), then
kill the pod:

```bash
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

### Step 3: Observe the Logs

In the stern output you should see:

1. The in-flight event **correctly drains** (ticks continue, completion
   publishes, handler returns 200, Dapr deletes from SQS).
2. **No** `"subscription is closed"` errors (this is a binding, not pub/sub).
3. Queued messages remain in SQS and are picked up by the replacement pod.

### Step 4: Verify No Messages in DLQ

Check the SQS console or CLI:

```bash
# Check DLQ message count (should be 0)
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-west-2.amazonaws.com/YOUR_ACCOUNT_ID/pcsf-scan-dev-dlq \
  --attribute-names ApproximateNumberOfMessages \
  --region us-west-2

# Check main queue (messages should still be available)
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-west-2.amazonaws.com/YOUR_ACCOUNT_ID/pcsf-scan-dev \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  --region us-west-2
```

## Expected vs RabbitMQ Pub/Sub Behavior

| Aspect | RabbitMQ Pub/Sub (main branch) | SQS Binding (this branch) |
|--------|-------------------------------|--------------------------|
| Shutdown trigger | `StopAllSubscriptionsForever()` sets `s.closed = true` | `StopReadingFromBindings(true)` cancels polling context |
| Queued messages | Fetched and NACKed to DLQ during ~400ms window | Never fetched — remain visible in SQS |
| In-flight message | Drains correctly | Drains correctly; `DeleteMessage` uses `context.Background()` |
| DLQ impact | Multiple messages lost per restart | None expected |

## File Layout

```
dapr-repro/
├── main.go          # Go app: chi mux, /healthz, SQS binding handler, graceful shutdown
├── go.mod
├── go.sum
├── Dockerfile       # Multi-stage: golang:1.26.0-alpine3.23 → alpine:3.23
├── skaffold.yaml    # Deploys Dapr, RabbitMQ, and this app to minikube
└── chart/
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
        ├── deployment.yaml         # Dapr annotations, native sidecar, health checks
        ├── pubsub-component.yaml   # RabbitMQ pubsub.rabbitmq component (completion events)
        └── sqs-binding.yaml        # SQS bindings.aws.sqs input binding
```
