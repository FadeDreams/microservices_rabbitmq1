
### Fixed
- **order**: Order consumers (`order_processor_node/consumer.js`, `order/consumer.py`) no longer silently drop messages on processing failure. Messages are now only acknowledged after successful processing, with failed messages retried via a dead-letter exchange (30s TTL backoff, max 5 attempts, tracked via `x-death` header) before landing in a terminal dead queue for triage. Fixed a broken `reject()` scope bug in the Node consumer caused by a stale single-shot Promise pattern, and switched the Python consumer from `auto_ack=True` to explicit ack/nack. (`00692ad`)

### Changed
- **order**: `queue1` now declares an `x-dead-letter-exchange` argument. ⚠️ Existing queues created before this change must be deleted (once empty) prior to deploy, or the consumer will throw `PRECONDITION_FAILED` / `ChannelClosedByBroker` on startup. (`00692ad`)
