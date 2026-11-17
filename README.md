# Collection estate — config, three entries, one sink

A small, fully local system that implements the requirements:

- one collection task reads **an API, a directory file and downloaded pages**;
- all three read **the same field mapping and range**;
- the **config service, the three collectors and the sink** are separate
  processes;
- a config update can never combine a **new mapping with an old range**;
- a task can either **wait for all entries** or **prefetch per entry and
  publish later**;
- failures **withdraw prepared state**, including from an entry that vanished;
- the console can **publish config, observe per-entry confirmation, sever one
  entry's link and restore it**, and an old entry coming back **cannot submit
  an outdated version**;
- every result traces back to **per-entry evidence of the config that was
  actually read and the bytes that were actually fetched** — no fabricated
  confirmations.

Everything runs against a local mock site and local files; there are no
external dependencies beyond the Python standard library.

## Quick start

```bash
python3 scripts/demo.py up          # boots all 7 processes on fixed ports
# open http://127.0.0.1:18088/
python3 scripts/demo.py down
```

Ports: mock `18080`, config `18081`, api/file/page entries `18082–18084`,
sink `18085`, console `18088`. All state lives under `.run/`.

Run the tests (ephemeral ports, fresh temp dir each case):

```bash
python3 -m unittest discover -s tests -v
```

## Architecture

```
            ┌──────────────────┐
            │  config service  │  immutable (mapping, scope) versions
            │    config.db     │  monotonic published pointer
            └────────┬─────────┘
                     │ GET /published (no-store, ETag, live only)
      ┌──────────────┼──────────────┐
      │              │              │
 ┌────▼────┐    ┌────▼────┐    ┌────▼────┐        ┌──────────────────┐
 │  api    │    │  file   │    │  page   │        │   mock estate    │
 │ entry   │    │ entry   │    │ entry   │◄───────┤ catalog / pages  │
 │ process │    │ process │    │ process │  /e/id │ file + receipts  │
 └────┬────┘    └────┬────┘    └────┬────┘        └──────────────────┘
      │  POST /prepare, /confirm, /rollback, /heartbeat
      └──────────────┬──────────────┘
              ┌──────▼───────┐
              │    sink      │  tasks, per-entry attempts, staged rows
              │   sink.db    │  independent re-read before commit
              └──────┬───────┘
              ┌──────▼───────┐
              │  web console │
              └──────────────┘
```

### Config service (`scripts/config_service.py`)

A version is a **pair**: a mapping and a scope, plus a fingerprint over the
mapping, a hash over the pair, and a seal over the whole body.

- `POST /versions` rejects any request without **both** mapping and scope, and
  rejects unknown fields — clients cannot hand-supply hashes.
- The body row is written in one transaction, so a reader sees either the
  whole old pair or the whole new pair.
- `POST /publish` only moves a pointer. Publishing an **older** version is
  refused with 409, so a stale component cannot rewind the estate.
- Every read is logged with the body hash and the caller's `User-Agent`; this
  is what the evidence trail cites.

### The three entries (`scripts/collector.py`)

One process per source kind, each with its own private route into the mock
(`/e/api`, `/e/file`, `/e/page`) and its own prepared directory.

- `POST /confirm` performs a live read of the *published* pair and refuses any
  body whose hash differs from the task's pin.
- `POST /prepare` performs a **fresh live config read**, applies the pair,
  reads its source with the task nonce, stages rows on disk, and returns a
  report.
- There is **no config cache**. If the config service is unreachable, the read
  fails; nothing is served from disk.
- `POST /admin/link` simulates the entry itself losing its network: both its
  config read and its source read are refused. That is the console's
  sever/restore control.
- A restarted entry **purges all staged directories** on startup; it can only
  participate in a new task via a new live read.

### The sink (`scripts/sink.py`)

- A task **pins the published body hash** at creation. Every prepare must
  present that exact hash, and the sink re-reads the config service itself.
- After all prepares it **verifies each entry independently**: it re-reads the
  same source from the mock directly, recomputes the projection, and compares
  raw byte digests, projected digests, row counts, config version, body hash,
  ETag, mapping fingerprint and scope hash.
- Publication is refused if the published pointer has moved to another
  version.
- All rows of a task flip to published in **one transaction**.
- `barrier` waits for all three live confirmations before any source is read;
  `prefetch` lets each entry stage immediately but still gates publication on
  all three verifying. With `publish=manual` the task stops in `ready` with
  rows staged and unpublished; a later `POST /tasks/<id>/commit` **re-verifies
  everything** (including re-reading the sources), so delayed publication
  cannot ship stale rows.

### Failure and rollback

On any failure the sink, in one transaction:

1. deletes every **staged** (unpublished) record for the task,
2. marks the task `aborted` and each participating entry rolled back,
3. queues a rollback for **every** entry (a durable `rollback_queue` row).

It then calls `/rollback` on each entry; reached entries have their prepared
directory deleted and their queue row closed. Unreached entries keep their
queue row, which is flushed on their next heartbeat. If the entry process was
killed instead, its startup purge removes the same state. Evidence columns
(config read, source bytes, verification) are **kept** on the aborted task, so
the failure remains auditable.

A watchdog aborts in-flight tasks whose entries stop heartbeating.

### Why a confirmation cannot be faked

Three independent mechanisms, all exercised by tests:

1. **Live-only reads.** A collector has no cache path. `source` is set to
   `live` only by the code path that just performed the HTTP read.
2. **Per-read receipts.** The mock signs every response with
   `HMAC-SHA256(source_key, nonce | path | sha256(body))`. The key lives in
   `.run/secrets/source.key`, readable by the sink only. A collector can only
   obtain a valid receipt by actually fetching that URL with **this task's**
   nonce. For the file entry the equivalent is a per-task nonce file written
   beside the source and read in the same call.
3. **Independent re-read.** The sink fetches the same bytes itself and
   compares digests, so even a colluding collector cannot submit rows the
   source does not contain.

The console's "matches published" bit requires `source == "live"` **and** the
published body hash; a cached or unreachable observation is shown as a
mismatch, never as a confirmation. Presence (`reachable`) is owned solely by
the entry's own heartbeat — a proxied observation cannot mark an entry online.

### Stale versions

- tasks pin the body hash and the sink refuses anything else;
- the config service refuses backward publication;
- a delayed manual publish re-checks the pointer and re-reads the sources;
- a restarted entry purges prepared state and must read the live config again;
- publication is refused if the pointer moved while the task was `ready`.

## Evidence trail

Every entry of every task records:

| field | meaning |
|---|---|
| `config_evidence.source` | always `live` for an accepted entry |
| `config_evidence.url`, `http_status` | the actual config request |
| `config_evidence.version`, `body_sha256`, `etag` | what was served |
| `config_evidence.fingerprint`, `scope_hash` | the pair that was used |
| `config_evidence.read_at`, `read_id` | when, and a per-read id |
| `source_evidence.raw_sha256`, `urls`/`pages` | bytes actually fetched |
| `source_evidence.receipts`, `echoed_nonces` | provider proof of this read |
| `verification.re_read_*` | the sink's own independent re-read |

The config service's `/access` log independently records each read with the
caller's `User-Agent` and body hash, so a claimed confirmation can be matched
to a real read.

## Layout

```
collect/          shared protocol, source reading, presets, HTTP helpers
scripts/          mock_estate, config_service, collector, sink, demo
web/              console (stdlib HTTP + one static page)
tests/            harness + 22 end-to-end scenarios
```

## Known limits

- The mock and the file source are local; "network" failures are simulated by
  closing connections or refusing reads, not by real partitions.
- The prepared directory is on local disk of each entry process; in a real
  deployment each entry would own durable staging with the same manifest.
- SQLite is used for coordination; it is appropriate for this local scale, not
  a substitute for a production message bus.
