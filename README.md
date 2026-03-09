# RabbitMQ Graceful Shutdown Repro — Direct AMQP (No Dapr)

Minimal Go application that consumes from RabbitMQ directly via AMQP,
with app-controlled graceful shutdown. No Dapr sidecar.

This demonstrates that by owning the AMQP consumer lifecycle, the app can
shut down cleanly with zero messages lost to the DLQ and zero messages
stranded behind a visibility timeout.

## How It Works

The app connects to RabbitMQ directly and declares its own exchange, queue,
DLQ, and bindings on startup (idempotent). It consumes with `prefetchCount=1`.

When an event arrives:
1. The handler logs a tick every 1 second for 20 seconds (simulating work).
2. After 20 seconds, it publishes a completion event directly to RabbitMQ.
3. It ACKs the message on the AMQP channel.

On SIGTERM:
1. `channel.Cancel()` — RabbitMQ stops delivering immediately. Queued messages
   stay visible for other consumers. **No NACKs, no DLQ.**
2. Mark `/healthz` unhealthy.
3. Wait for the in-flight message to finish and ACK.
4. Close AMQP connection.
5. Shutdown HTTP server, exit.

## Prerequisites

| Tool | Tested Version |
|------|---------------|
| [minikube](https://minikube.sigs.k8s.io/) | v1.35.0 |
| [skaffold](https://skaffold.dev/) | v1.39.1 |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | any recent |
| [stern](https://github.com/stern/stern) (optional) | any recent |

## Quick Start

```bash
minikube start --kubernetes-version=v1.33.0
skaffold run

# Tail logs
stern -n dapr-repro dapr-repro
```

Skaffold deploys:
1. **RabbitMQ** (bitnami 16.0.14) into `rabbit`
2. **dapr-repro** (this app) into `dapr-repro`

## Reproducing the Test

### Step 1: Publish Events

Open the RabbitMQ management UI at `http://localhost:15672` (rabbit/rabbit),
go to the `incoming-events` exchange, and publish messages with routing key
`#` and body `{"id":"A"}`, `{"id":"B"}`, etc.

Or use `rabbitmqadmin` from within the cluster:

```bash
for i in A B C D E F G H; do
  kubectl exec -n rabbit svc/rabbitmq -- \
    rabbitmqadmin publish \
      exchange=incoming-events \
      routing_key=test \
      payload="{\"id\":\"$i\"}"
done
```

### Step 2: Delete the Pod Mid-Processing

Wait for tick logs, then:

```bash
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

### Step 3: Observe the Logs

You should see:
1. `canceling AMQP consumer` — immediate on SIGTERM
2. In-flight event ticks to completion
3. `completion published — ACKing message`
4. `in-flight messages drained`
5. `AMQP connection closed`
6. **No** messages in the DLQ
7. Remaining queued messages picked up by the replacement pod immediately

### Step 4: Verify DLQ is Empty

```
http://localhost:15672  (rabbit / rabbit)
```

Check `dlq-dapr-repro-incoming-events` — should have 0 messages.

## File Layout

```
dapr-repro/
├── main.go          # Direct AMQP consumer + publisher + graceful shutdown
├── go.mod
├── go.sum
├── Dockerfile
├── skaffold.yaml    # Deploys RabbitMQ + this app (no Dapr)
└── chart/
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
        └── deployment.yaml
```
