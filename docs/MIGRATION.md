# SolidJobs → SolidEngine: adapters, migration and rollback

## Adapters

Native clients (binary protocol v1, Unix socket or TCP) live in each SolidJobs repo:

| Language | File | Selector |
|----------|------|----------|
| Ruby   | `solid-jobs/lib/solid_jobs/backends/solid_engine.rb`  | `config.backend = :solid_engine` |
| Python | `solid-jobs-py/src/solid_jobs/backends/solid_engine.py` | `config.backend = "solid_engine"` |
| Node   | `solid-jobs-node/lib/backends/solid_engine.js`          | `config.backend = 'solid_engine'` |

```ruby
SolidJobs.configure do |config|
  config.backend = :solid_engine            # default: :redis
  config.url     = "unix:///var/run/solidengine.sock"
end
SolidJobs.config.engine_backend.enqueue({ "task" => "Mailer", "arguments" => [1] })
```

Common interface: `enqueue`, `schedule`, `claim`, `ack`, `nack`, `extend_lease`, `cancel`, `job`, `stats`, `ping`.

- Job ids must be UUIDs (mapped to the engine's u128); push with an existing id is idempotent.
- Channel names map to u16 ids through a CRC32 hash unless `channel_ids:` is given explicitly. Pin the mapping in production: collisions merge channels, and changing it re-routes jobs.
- A stale `ack`/`nack` (lease expired, job reclaimed) raises `stale_reservation`.
- Only idempotent reads (`ping`, `job`, `stats`) are retried after a lost connection; writes are never replayed silently (delivery is at-least-once).

## Current scope

Ruby: the runtime routes through the adapters. `config.backend` selects the default
backend and `config.channel_backends` overrides it per channel (see the SolidJobs
README, "SolidEngine backend"). Channels on SolidEngine are served by workers inside `Conductor`
(`config.execution_mode`: `:ractor` by default, one client per Ractor, or `:thread`); Redis channels keep the Ractor processors. `Catalog`,
`Observations` and `IntegrityCheck` remain Redis-only. Python and Node have the client
only; their routing layer is the remaining step.

## Conformance tests

```
cargo build
SOLID_ENGINE_BIN=$PWD/target/debug/solid-engine-server ruby -Ilib -Itest test/solid_engine_backend_test.rb   # solid-jobs
SOLID_ENGINE_BIN=... PYTHONPATH=src python3 -m unittest tests.test_solid_engine_backend                      # solid-jobs-py
SOLID_ENGINE_BIN=... node --test test/solid_engine_backend.test.js                                             # solid-jobs-node
```

## Migration

1. Run the engine with `durability = "strict"` or `"balanced"` and a `data_dir`.
2. Drain Redis queues (or keep a Redis worker running on the old channels until empty).
3. Switch producers first, then workers, to `backend = :solid_engine`.

## Rollback

Set `backend = :redis` and restart. Jobs still queued in the engine are not copied back: keep the engine running with a worker until its queues are empty (`stats`), or accept re-enqueueing them. Delivery is at-least-once, so jobs must be idempotent in both directions.
