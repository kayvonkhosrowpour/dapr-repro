# RabbitMQ: `handleMessage` NACKs messages on `context.Canceled` during graceful shutdown, routing them to the dead-letter queue

## In what area(s)?

/area pubsub

## What component?

`pubsub.rabbitmq`

## What version of components-contrib / Dapr?

- components-contrib: v1.16.10 (and current `main`)
- Dapr runtime: v1.16.10 (also reproducible with dapr/dapr PR #9619 applied)

## Related Issues

- dapr/dapr#9604 — Pub/Sub: Messages NACKed to dead-letter queue during graceful shutdown due to premature subscription close
- dapr/dapr#9619 — fix: Prevent pub/sub messages from being NACKed during graceful shutdown

## Expected Behavior

When the Dapr runtime is shutting down and the subscription handler returns `context.Canceled` (because the subscription context was cancelled), the RabbitMQ component should **not** NACK the message. Instead, it should leave the message unacknowledged so that when the AMQP connection closes, RabbitMQ redelivers the message to another consumer.

## Actual Behavior

`handleMessage` in `pubsub/rabbitmq/rabbitmq.go` unconditionally NACKs any non-nil error returned by the handler — including `context.Canceled`. With `enableDeadLetter: true` and `requeueInFailure: false` (the default), this routes the message to the dead-letter queue.

This means that even after dapr/dapr#9619 fixes the runtime to block on `ctx.Done()` instead of returning `"subscription is closed"`, one message still gets NACKed to the DLQ during the narrow window when the context is cancelled and the AMQP connection hasn't yet closed.

### Relevant log from daprd

```
level=error msg="rabbitmq pub/sub error: handling message from topic 'incoming-events', context canceled"
```

Note: the error is `context canceled` (not `subscription is closed`), confirming the runtime fix from #9619 is working. But `handleMessage` still NACKs it.

## Root Cause

In [`pubsub/rabbitmq/rabbitmq.go` — `handleMessage`](https://github.com/dapr/components-contrib/blob/main/pubsub/rabbitmq/rabbitmq.go#L624-L656):

```go
func (r *rabbitMQ) handleMessage(ctx context.Context, d amqp.Delivery, topic string, handler pubsub.Handler) error {
    // ...
    err := handler(ctx, pubsubMsg)

    if err != nil {
        r.logger.Errorf("%s handling message from topic '%s', %s", errorMessagePrefix, topic, err)

        if !r.metadata.AutoAck {
            r.logger.Debugf("%s nacking message '%s' from topic '%s', requeue=%t", logMessagePrefix, d.MessageId, topic, r.metadata.RequeueInFailure)
            if err = d.Nack(false, r.metadata.RequeueInFailure); err != nil {
                r.logger.Errorf("%s error nacking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
            }
        }
    } else if !r.metadata.AutoAck {
        // ...ack...
    }
    return err
}
```

The `if err != nil` block has no check for context cancellation. When `handler` returns `context.Canceled` during shutdown, the message is NACKed with `requeue=false`, sending it to the dead-letter queue.

## Proposed Fix

Add a `ctx.Err()` guard before the NACK logic. If the context is done, skip both ACK and NACK — the message stays unacknowledged, and RabbitMQ will redeliver it to another consumer when the connection closes:

```go
err := handler(ctx, pubsubMsg)

if err != nil {
    if ctx.Err() != nil {
        r.logger.Debugf("%s context done while handling message from topic '%s'; skipping ack/nack to allow redelivery", logMessagePrefix, topic)
        return err
    }

    r.logger.Errorf("%s handling message from topic '%s', %s", errorMessagePrefix, topic, err)

    if !r.metadata.AutoAck {
        r.logger.Debugf("%s nacking message '%s' from topic '%s', requeue=%t", logMessagePrefix, d.MessageId, topic, r.metadata.RequeueInFailure)
        if err = d.Nack(false, r.metadata.RequeueInFailure); err != nil {
            r.logger.Errorf("%s error nacking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
        }
    }
} else if !r.metadata.AutoAck {
    // ...ack unchanged...
}
```

## Steps to Reproduce

Full reproduction repo: https://github.com/kayvonkhosrowpour/dapr-repro (branch `components-contrib-bug`)

**Setup:** A Go app subscribes to a RabbitMQ topic via Dapr programmatic subscription. Each event takes ~20s to process. The Dapr sidecar is configured with `block-shutdown-duration: 60s`, app health checks (interval 3s, threshold 2), `prefetchCount: 1`, and `enableDeadLetter: true`.

```bash
# 1. Start minikube and deploy everything
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

# 4. Wait for processing tick logs to appear, then delete the pod
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

**Observe:**
- With standard Dapr (pre-#9619): multiple messages NACKed with `"subscription is closed"` errors → multiple DLQ messages.
- With Dapr including #9619 fix: one message NACKed with `"context canceled"` → one DLQ message. This is the components-contrib gap.
- With the proposed `ctx.Err()` fix in `handleMessage`: zero DLQ messages.

Check the RabbitMQ management UI at `localhost:15672` (credentials `rabbit`/`rabbit`) to see DLQ queue counts.

## Release Note

RELEASE NOTE: **FIX** RabbitMQ pub/sub `handleMessage` now skips NACK when the context is cancelled (e.g. during graceful shutdown), leaving the message unacknowledged for redelivery by the broker instead of routing it to the dead-letter queue.
