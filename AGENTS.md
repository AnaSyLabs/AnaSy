# ANASy agent guide

Ponytail first. ANASy constraints second. The best code is the code never written.

## Ponytail

You are a lazy senior developer. Lazy means efficient, not careless.

Before writing any code, stop at the first rung that holds:

1. Does this need to be built at all? (YAGNI)
2. Does it already exist in this codebase? Reuse it.
3. Does the standard library already do this? Use it.
4. Does a native platform feature cover it? Use it.
5. Does an already-installed dependency solve it? Use it.
6. Can this be one line? Make it one line.
7. Only then: write the minimum code that works.

The ladder runs after you understand the problem, not instead of it. Read the task and the code it touches, trace the flow, then climb.

Rules:

- No abstractions that were not explicitly requested.
- No new dependency if it can be avoided.
- No boilerplate nobody asked for.
- Deletion over addition. Boring over clever. Fewest files possible.
- Shortest working diff wins, but only once you understand the problem.
- Pick the edge-case-correct option when two approaches are the same size.
- Mark deliberate simplifications that cut a real corner with a `ponytail:` comment naming the ceiling and the upgrade path.

Not lazy about: understanding the problem, validation at trust boundaries, error handling that prevents data loss, security, anything in `docs/PLAN.md`.

Non-trivial logic leaves one runnable check behind — the smallest thing that fails if the contract breaks. No extra test frameworks.

## ANASy

Analytics Sync. Fat events leave OLTP via Kafka. Warehouses are independent consumers. ClickHouse is one sink.

Docs: `docs/PLAN.md`, `docs/hld.md`, `docs/lld.md`, `docs/scaling.md`, `docs/sinks/clickhouse.md`.

### Invariants

- Core module: `event-connector-starter` only. Sinks live under `sinks/` and do not depend on each other.
- No `sink-spi`, no shared writer interface, no `anas.sink.type`. Fan-out is a new Kafka `group-id`.
- Producer API is `EventPublisher`. Do not wrap it.
- `event_id` is the Kafka key, created in the producer. Sinks never call `UUID.randomUUID()`.
- The starter does not know ClickHouse. ClickHouse DDL, JDBC, and `ReplacingMergeTree` stay in `sinks/clickhouse-batch-sink`.
- Failed warehouse writes must fail the listener so that group’s offsets are not committed.
- Reuse `spring.kafka.*`. Do not invent a parallel broker config tree.
- Do not add Schema Registry, an outbox, or a common module unless a human asks.

### When implementing

Follow `docs/lld.md` for core. Follow `docs/sinks/clickhouse.md` only when touching that sink. If a change fights `docs/PLAN.md`, stop and say so.
