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

The adapters are a standalone, tested storage backend. The existing SolidJobs runtime (Conductor, Recovery, Heartbeat, Catalog, Lua scripts) is **still Redis-only**; with `backend = :redis` nothing changes. Routing the worker loop through `engine_backend` is the remaining integration step.

## Conformance tests

```
cargo build
SOLID_ENGINE_BIN=$PWD/target/debug/solid-engine-server ruby -Ilib -Itest test/solid_engine_backend_test.rb   # solid-jobs
SOLID_ENGINE_BIN=... PYTHONPATH=src python3 -m unittest tests.test_solid_engine_backend                      # solid-jobs-py
SOLID_ENGINE_BIN=... node --test test/solid_engine_backend.test.js                                             # solid-jobs-node
```

## Migration (once the runtime uses the adapter)

1. Run the engine with `durability = "strict"` or `"balanced"` and a `data_dir`.
2. Drain Redis queues (or keep a Redis worker running on the old channels until empty).
3. Switch producers first, then workers, to `backend = :solid_engine`.

## Rollback

Set `backend = :redis` and restart. Jobs still queued in the engine are not copied back: keep the engine running with a worker until its queues are empty (`stats`), or accept re-enqueueing them. Delivery is at-least-once, so jobs must be idempotent in both directions.
