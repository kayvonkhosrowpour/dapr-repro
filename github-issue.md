# RabbitMQ: `handleMessage` still NACKs messages on `context.Canceled` during graceful shutdown in v1.18.1

## In what area(s)?

/area pubsub

## What component?

`pubsub.rabbitmq`

## What version of components-contrib / Dapr?

- components-contrib: **v1.18.1** (latest release)
- Dapr runtime: **v1.18.1** (latest release)

## History

This was previously filed as **[#4281](https://github.com/dapr/components-contrib/issues/4281)**
(March 2026) against v1.16.10, and was **closed as stale without being fixed**. The bug
persists in the latest v1.18.1 release. This is a re-filing with updated version confirmation
and a ready-to-merge fix.

## Related Issues

- [dapr/dapr#9604](https://github.com/dapr/dapr/issues/9604) — Original runtime-side bug report (closed/fixed)
- [dapr/dapr#9619](https://github.com/dapr/dapr/pull/9619) — Runtime fix (merged into v1.14+)
- [dapr/components-contrib#4281](https://github.com/dapr/components-contrib/issues/4281) — Previous filing (closed as stale, unfixed)

## Expected Behavior

When the Dapr runtime is shutting down and the subscription handler returns `context.Canceled`
(because the subscription context was cancelled), the RabbitMQ component should **not** NACK
the message. Instead, it should leave the message unacknowledged so that when the AMQP
connection closes, RabbitMQ redelivers the message to another consumer.

## Actual Behavior

`handleMessage` in `pubsub/rabbitmq/rabbitmq.go` unconditionally NACKs any non-nil error
returned by the handler — including `context.Canceled`. With `enableDeadLetter: true` and
`requeueInFailure: false` (the default), this routes the message to the dead-letter queue.

The dapr runtime fix from #9619 is working correctly — the error changed from
`"subscription is closed"` to `"context canceled"` — but `handleMessage` still NACKs it:

```
level=error msg="rabbitmq pub/sub error: handling message from topic 'incoming-events', context canceled"
```

This means **one message per graceful shutdown is unconditionally routed to the DLQ** when
using RabbitMQ with `enableDeadLetter: true`, even on the latest Dapr v1.18.1.

## Root Cause

In [`pubsub/rabbitmq/rabbitmq.go` — `handleMessage`](https://github.com/dapr/components-contrib/blob/main/pubsub/rabbitmq/rabbitmq.go):

```go
err := handler(ctx, pubsubMsg)

if err != nil {
    r.logger.Errorf("%s handling message from topic '%s', %s", errorMessagePrefix, topic, err)

    if !r.metadata.AutoAck {
        // NACKs unconditionally — including when err == context.Canceled
        if err = d.Nack(false, r.metadata.RequeueInFailure); err != nil { ... }
    }
}
```

The `if err != nil` block has no check for context cancellation. When the subscription context
is cancelled during graceful shutdown, `handler` returns `context.Canceled`, and the message
is NACKed with `requeue=false`, routing it to the dead-letter queue.

## Proposed Fix

Add a `ctx.Err()` guard before the NACK logic. If the context is done, skip both ACK and NACK —
the message stays unacknowledged, and RabbitMQ redelivers it to another consumer when the
connection closes:

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
    // ack path unchanged
}
```

This is a **5-line change** with no new dependencies. I'm happy to submit a PR with this fix
and a unit test.

## Steps to Reproduce

Full reproduction repo (updated for v1.18.1):
**https://github.com/kayvonkhosrowpour/dapr-repro** (branch `components-contrib-bug`)

One-command setup:

```bash
minikube start --kubernetes-version=v1.33.0
skaffold run  # deploys Dapr v1.18.1, RabbitMQ, demo app
```

Then:

```bash
# Tail logs
stern -n dapr-repro dapr-repro

# Publish 8 events to create a backlog
for i in $(seq 1 8); do
  kubectl exec -n dapr-repro deploy/dapr-repro -c dapr-repro -- \
    wget -qO- --post-data="{\"id\":\"$i\"}" \
    --header='Content-Type: application/json' \
    http://localhost:3500/v1.0/publish/pubsub/incoming-events
done

# Wait for tick logs (first event processing), then delete the pod
kubectl delete pod -n dapr-repro -l app=dapr-repro
```

**Observe:**

| Scenario | DLQ messages | daprd error |
|---|---|---|
| Dapr v1.18.1 (stock) | **1** | `"context canceled"` |
| Dapr v1.18.1 + proposed fix | **0** | none |

Check the RabbitMQ management UI at `localhost:15672` (credentials `rabbit`/`rabbit`) —
`dlq-dapr-repro-incoming-events` queue will have 1 message after shutdown.

## Release Note

RELEASE NOTE: **FIX** RabbitMQ pub/sub `handleMessage` now skips NACK when the context is
cancelled (e.g. during graceful shutdown), leaving the message unacknowledged for redelivery
by the broker instead of routing it to the dead-letter queue.
