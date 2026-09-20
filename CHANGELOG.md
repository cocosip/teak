# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-09-20

### Added

- Durable synchronous pending-record storage with atomic sequence tails and versioned envelopes.
- Independent out-of-order commit, explicit atomic dead letter, and origin-preserving requeue.
- Bounded competing-consumer dispatch with opaque receipts, visibility leases, retry backoff, and
  fresh/retry fairness.
- Restart recovery that redelivers every remaining pending key.
- Public byte API, fluent configuration, statistics, optional structured logging, and JSON typed API.
- Narrow panic recovery for caller codecs and the owned value-log maintenance worker.
- Crash-process, corruption, concurrency, backpressure, retry, race, and benchmark coverage.

### Guarantees

- Successful writes use synchronous Badger transactions.
- Pending data leaves the active queue only through successful commit or atomic dead-letter transfer.
- One uncommitted record does not impose a global consumption watermark on later records.
- Delivery is at least once; external side effects remain the consumer's idempotency responsibility.
