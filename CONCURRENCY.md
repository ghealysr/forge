# FORGE Concurrency Model

> This document captures five rounds of concurrency debugging.
> Every bug in the enrichment pipeline's first three months was a concurrency issue.
> This is the most important missing doc in the repo.

---

## Thread Model During Enrichment

FORGE's enrichment pipeline runs multiple threads concurrently. Understanding which thread does what is essential for debugging.

### CLI Mode (`forge enrich`)

When enrichment is launched from the CLI, three threads are active:

1. **Main thread** -- the CLI handler (`cmd_enrich` in `cli.py`). It calls `pipeline.run()`, which blocks until all worker threads complete. Signal handlers (`SIGINT`, `SIGTERM`) are registered on this thread to set the stop event.

2. **Email extraction thread** (`email-extractor`) -- runs the async web scraper via `asyncio.new_event_loop()` in a dedicated thread. The event loop is created fresh because the main thread has no running loop. This thread fetches businesses that have a website but no email, scrapes them concurrently using aiohttp with up to 50 async workers inside the single thread's event loop, and writes results back to the database.

3. **AI enrichment thread** (`ai-enricher`) -- runs sequential Gemma/Claude calls. Each business is processed one at a time: build prompt, call model, parse JSON response, write enrichment data. No async, no parallelism within this thread. The rate bottleneck is model inference time, not I/O.

### Dashboard Mode (`forge dashboard`)

When enrichment is triggered from the web dashboard:

1. **Main thread** -- the uvicorn ASGI server, handling HTTP requests.

2. **Dashboard enrichment thread** (`dashboard-enrichment`) -- a `threading.Thread` spawned by the `/api/enrich/start` endpoint. This thread runs `_run_enrichment_background()`, which internally creates the `EnrichmentPipeline` and calls `pipeline.run()`. The pipeline then spawns its own email and AI threads as sub-threads.

3. **Stop watcher thread** -- a daemon thread inside `_run_enrichment_loop()` that blocks on `_enrichment_stop.wait()` and calls `pipeline.stop()` when the event is set. This lets the dashboard's stop button propagate to the pipeline.

---

## Locks

Four locks protect shared mutable state. Each exists for a specific reason.

### `_SQLiteBackend._write_lock` (threading.RLock)

Serializes all SQLite writes. This is an **RLock** (reentrant lock), not a plain Lock, because `db.execute()` can be called inside `db.transaction()`. With a plain Lock, the transaction would acquire the lock, then `execute()` would try to acquire it again and deadlock. RLock allows the same thread to re-enter.

Location: `forge/db_schema.py`, `_SQLiteBackend.__init__`.

### `EnrichmentPipeline._lock` (threading.Lock)

Protects the `EnrichmentStats` counters (`emails_found`, `tech_stacks_found`, `total_processed`, etc.) from concurrent updates by the email extraction thread and the AI enrichment thread. Every stat increment is wrapped in `with self._lock`.

Location: `forge/enrichment/pipeline.py`, `EnrichmentPipeline.__init__`.

### `_enrichment_lock` (threading.Lock, dashboard module)

Prevents double-start of enrichment. The dashboard's `/api/enrich/start` handler checks `_enrichment_stats["running"]` and sets it to `True` inside a single `with _enrichment_lock` block. This prevents a TOCTOU race where two concurrent requests could both see `running=False` and both start enrichment threads.

Location: `forge/dashboard/app.py`, module level.

### `_db_lock` (threading.Lock, dashboard module)

Protects the lazy initialization of the `ForgeDB` singleton in the dashboard. Without this lock, two concurrent requests hitting `_get_db()` before initialization completes could create two database instances, wasting connections and potentially causing SQLite locking conflicts.

Location: `forge/dashboard/app.py`, module level.

---

## The `_in_transaction` Flag

### What it is

A `threading.local()` attribute on `ForgeDB`. Each thread gets its own independent `.active` boolean.

### How it works

- `db.transaction()` sets `self._in_transaction.active = True` on entry, `False` in the `finally` block.
- `db.execute()` reads `getattr(self._in_transaction, 'active', False)` before deciding whether to auto-commit.

### Why it exists

On **SQLite**, there is one shared connection. Both `transaction()` and `execute()` use it. If `execute()` auto-committed while a `transaction()` block was active, it would flush the transaction's pending writes prematurely, breaking atomicity. The `_in_transaction` flag tells `execute()` to skip auto-commit when it is running inside an active transaction on the same thread.

On **PostgreSQL**, the flag exists but is cosmetic. `execute()` gets a separate pool connection, so its auto-commit cannot affect the transaction's connection. The flag is checked for symmetry and as a guard against future refactoring that might change the pool behavior.

---

## The `transaction()` Context Manager

### SQLite path (`_sqlite_transaction`)

1. Acquire `_write_lock` (RLock).
2. Set `_in_transaction.active = True`.
3. Yield a `_Transaction` wrapper around the shared connection.
4. On clean exit: `conn.commit()`.
5. On exception: `conn.rollback()`, then re-raise.
6. In `finally`: set `_in_transaction.active = False`. The RLock releases when the `with` block exits.

### PostgreSQL path (`_pg_transaction`)

1. Acquire a connection from the pool (`_pool.getconn()`).
2. Set `_in_transaction.active = True`.
3. Yield a `_Transaction` wrapper around the acquired connection.
4. On clean exit: `conn.commit()`.
5. On exception: set `broken = True`, attempt `conn.rollback()` (wrapped in try/except because the connection might be dead), then re-raise.
6. In `finally`: set `_in_transaction.active = False`, return connection to pool with `putconn(conn, close=broken)`. If `broken` is True, the pool discards the connection instead of reusing it.

---

## Connection Pool Lifecycle (PostgreSQL)

The pool is `psycopg2.pool.ThreadedConnectionPool(min=2, max=10)`.

### `db.execute()` — one round-trip

1. `getconn()` -- acquire from pool.
2. `cursor.execute(query, params)` -- run the query.
3. `conn.commit()` -- commit immediately.
4. `putconn(conn)` -- return to pool.

On error: `conn.rollback()` before `putconn()`.

### `db.transaction()` — multiple operations, one connection

1. `getconn()` -- acquire from pool. This connection is held for the duration.
2. Multiple `tx.execute()` calls on the same connection.
3. `conn.commit()` on clean exit, or `conn.rollback()` on exception.
4. `putconn(conn, close=broken)` -- return or discard.

### Broken connections

When a connection encounters an unrecoverable error (network drop, server restart), the `broken` flag is set to `True`. `putconn(conn, close=True)` tells the pool to close and discard the connection rather than returning it for reuse. The pool will create a fresh connection on the next `getconn()` call.

---

## What Happens on Ctrl-C

1. The `SIGINT` handler (registered in `_run_enrichment_pipeline`) sets `pipeline._running = False` via the `_stop_requested` event, then re-registers the default `SIG_DFL` handler so a second Ctrl-C force-kills immediately.

2. The **email extraction thread** checks `self._running` at the top of each batch loop iteration. When it sees `False`, it exits the loop, runs `self._scraper.close()` to clean up the aiohttp session, and the thread terminates.

3. The **AI enrichment thread** checks `self._running` before processing each individual business record. When it sees `False`, it breaks out of the loop and the thread terminates.

4. In **dashboard mode**, the stop button sets `_enrichment_stop` (a `threading.Event`). A watcher thread blocks on `_enrichment_stop.wait()` and calls `pipeline.stop()` when triggered, which sets `pipeline._running = False`.

5. The main thread's `pipeline.run()` calls `thread.join()` on each worker thread, so it blocks until all threads have exited cleanly.

---

## Known Limitations

1. **SQLite is single-writer.** The RLock serializes all writes through one thread at a time. If you run multiple enrichment processes against the same SQLite file, they will block each other. This is a SQLite fundamental, not a FORGE bug. For multi-process workloads, use PostgreSQL.

2. **PostgreSQL pool size must accommodate all concurrent users.** The pool needs at least `worker_count + 2` connections: one per enrichment thread, one for the main thread's status queries, and one for monitoring. The default `max=10` is sufficient for the standard two-thread enrichment model but would need increasing if more parallel workers were added.

3. **The dashboard's enrichment thread has no timeout.** If the enrichment pipeline hangs (for example, if Ollama stops responding and the HTTP timeout is set very high), the background thread hangs indefinitely. The stop button sets the event, but if the pipeline is blocked in a synchronous call, it will not check `_running` until that call returns. A future improvement would be to add a watchdog timer that force-kills the thread after a configurable maximum duration.

4. **Thread-local `_in_transaction` is per-thread, not per-coroutine.** If async code were added to the SQLite path (it currently is not), multiple coroutines on the same thread would share the flag. This is not a problem today because SQLite operations are synchronous, but it is worth noting for future async refactoring.
