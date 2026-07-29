## [Unreleased]

### Fixed
- **order**: `order_processor_go/main.go` no longer uses auto-ack, so parse failures and failed downstream PUT requests were previously silently lost instead of being retried. Consumer now acks only on success and nacks (`requeue=false`) on failure, routing through the dead-letter exchange topology (30s TTL backoff, max 5 attempts, tracked via `x-death` header) before landing in the terminal dead queue for triage. `sendPutRequest` now returns an error (including non-2xx responses) instead of printing and swallowing it. Also switched from the unmaintained `streadway/amqp` to the maintained `rabbitmq/amqp091-go` fork. (`903fb87`)
- **order**: `order_processor_fastapi/app.py` no longer acks messages on a failed order update — previously `message.process()` would ack regardless of whether `update_order` succeeded, since failures were only logged rather than raised. Failed updates and malformed/incomplete message bodies now raise, causing the message to nack and flow through the dead-letter/retry topology. Messages that exceed the retry threshold (via `x-death` header count) are now explicitly published to a terminal DLQ (`queue1.dlq`) and acked off the main queue, instead of looping indefinitely. (`903fb87`)

### Changed
- **order**: `order_processor_go/main.go` now declares `queue1` with an `x-dead-letter-exchange` argument, matching the Node/Python consumers. `order_processor_fastapi/app.py` now declares the full DLX topology on startup (`dlx.orders` exchange, `orders.process.retry` queue with TTL, `queue1.dlq` terminal queue). ⚠️ Existing `queue1` instances created before this change must be deleted (once empty) prior to deploy across **all** consumers (Go, Node, Python, FastAPI), or the affected consumer will throw `PRECONDITION_FAILED` / `ChannelClosedByBroker` on startup due to mismatched queue arguments. (`903fb87`)

---

### Fixed
- **order**: Order consumers (`order_processor_node/consumer.js`, `order/consumer.py`) no longer silently drop messages on processing failure. Messages are now only acknowledged after successful processing, with failed messages retried via a dead-letter exchange (30s TTL backoff, max 5 attempts, tracked via `x-death` header) before landing in a terminal dead queue for triage. Fixed a broken `reject()` scope bug in the Node consumer caused by a stale single-shot Promise pattern, and switched the Python consumer from `auto_ack=True` to explicit ack/nack. (`00692ad`)

### Changed
- **order**: `queue1` now declares an `x-dead-letter-exchange` argument. ⚠️ Existing queues created before this change must be deleted (once empty) prior to deploy, or the consumer will throw `PRECONDITION_FAILED` / `ChannelClosedByBroker` on startup. (`00692ad`)
