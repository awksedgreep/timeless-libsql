# Telemetry over HTTP with sqld

The same `.so` that works in the `sqlite3` CLI loads into
[sqld](https://github.com/tursodatabase/libsql) (the self-hosted libSQL
server) — which turns a timeless database into a **networked telemetry
store speaking plain SQL over HTTP**. No client library, no wire protocol to
implement, no changes to the extension: sqld loads it into every pooled
connection, and the process-global engine registry makes all of them share
one engine per table.

The [SQLite extension API reference](SQL_API_REFERENCE.md) defines the SQL
modules and capability handshake used below. This guide covers only their
deployment through self-hosted `sqld`.

What you get:

- **Any language becomes a client.** If it can POST JSON, it can ingest and
  query compressed metrics, logs, and traces. See the
  [Livebook tour](tour.livemd) for a complete Elixir client in ~30 lines.
- **Fresh-connection semantics for free.** Each HTTP request may land on a
  different pooled connection; inserts and flushes from one are immediately
  visible to the others.
- **libSQL replication.** The database file is a normal libSQL database —
  embedded replicas can pull it, compressed telemetry included.

## 1. Get sqld

Build from source (the extension needs sqld's extension-loading support,
present on current main):

```sh
RUSTFLAGS="--cfg tokio_unstable" \
  cargo install --git https://github.com/tursodatabase/libsql libsql-server --locked
# installs the `sqld` binary
```

Both flags matter: the server's admin metrics use tokio's unstable runtime
API (`--cfg tokio_unstable`), and without `--locked` cargo resolves a newer
tokio where those methods no longer exist. Either omission fails the build
with `no method named ... found for struct RuntimeMetrics`.

(Container image: see [section 5](#5-running-sqld-in-a-container) — it works,
but read the glibc caveat first.)

## 2. Stage the extension

sqld only loads extensions listed in a `trusted.lst` file (sha256 + filename,
i.e. exactly what `sha256sum` prints):

```sh
cargo build --release -p timeless-ext

mkdir -p ext-dir
cp target/release/libtimeless_ext.so ext-dir/
(cd ext-dir && sha256sum libtimeless_ext.so > trusted.lst)
```

> Rebuild the extension → regenerate `trusted.lst`. A stale hash means the
> extension silently fails to load and you'll see
> `no such module: timeless_metrics` on CREATE.

## 3. Run

```sh
sqld --db-path telemetry.sqld \
     --extensions-path ./ext-dir \
     --http-listen-addr 127.0.0.1:8880
```

`--db-path` is a directory sqld manages; your data, shadow tables and all,
lives inside it as a standard libSQL database.

## 4. Speak SQL over HTTP

The HTTP API is the [Hrana protocol](https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md):
POST a pipeline of statements to `/v3/pipeline`. Create the tables, ingest,
flush — one request:

```sh
curl -s http://127.0.0.1:8880/v3/pipeline -d '{
  "requests": [
    {"type": "execute", "stmt": {"sql": "CREATE VIRTUAL TABLE IF NOT EXISTS metrics USING timeless_metrics"}},
    {"type": "execute", "stmt": {"sql": "INSERT INTO metrics(name, ts, value, labels) VALUES ('"'"'cpu_usage'"'"', 1753000000, 42.5, '"'"'{\"host\":\"web1\"}'"'"')"}},
    {"type": "execute", "stmt": {"sql": "INSERT INTO metrics(metrics) VALUES ('"'"'flush'"'"')"}},
    {"type": "close"}
  ]}'
```

Query it back — deliberately from a **second request**, i.e. a fresh pooled
connection, with bound parameters (integers are JSON strings to preserve
64-bit precision):

```sh
curl -s http://127.0.0.1:8880/v3/pipeline -d '{
  "requests": [
    {"type": "execute", "stmt": {
      "sql": "SELECT name, ts, value, labels FROM metrics WHERE name = ? AND ts >= ?",
      "args": [{"type": "text", "value": "cpu_usage"},
               {"type": "integer", "value": "1753000000"}]}},
    {"type": "close"}
  ]}'
```

```json
{"results": [{"type": "ok", "response": {"type": "execute", "result": {
  "cols": [{"name": "name"}, {"name": "ts"}, {"name": "value"}, {"name": "labels"}],
  "rows": [[{"type": "text", "value": "cpu_usage"},
            {"type": "integer", "value": "1753000000"},
            {"type": "float", "value": 42.5},
            {"type": "text", "value": "{\"host\":\"web1\"}"}]], ...}}}, ...]}
```

Everything from the [User's Guide](GUIDE.md) works verbatim through this
endpoint: logs with `index_keys` pushdown, trace reassembly by id,
`'optimize'` / `'prune:<ts>'` commands, shadow-table inspection. Only the
transport changed.

Notes for client authors:

- One pipeline = one connection = one transaction scope. Batch related
  statements (e.g. inserts + `'flush'`) into a single pipeline when you want
  them on one connection; end pipelines with `{"type": "close"}`.
- Values are typed JSON: `integer` values are *strings*, blobs are base64.
  Trace ids are easiest sent as 32-char hex TEXT and read back via
  `hex(trace_id)`.
- The interactive walkthrough of all three tables over this API — with
  charts — is the [Livebook tour](tour.livemd).

## 4½. dbhealth under sqld needs cron — here is the line

The dbhealth extension's built-in sampler deliberately stays OFF for
sqld-managed databases (out-of-band writes vs. the replication log —
see docs/DBHEALTH.md). Under sqld, collection is this one crontab
entry, tested verbatim against a real server:

```cron
# m h dom mon dow  command
* * * * * /usr/bin/curl -sS -m 10 -o /dev/null http://127.0.0.1:8880/v3/pipeline -d '{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO dbhealth(dbhealth) VALUES (?1)","args":[{"type":"text","value":"sample"}]}},{"type":"close"}]}'
```

`?1` binding avoids quote-escaping, there is no `%` for cron to eat,
`-m 10` bounds a hung request. Prerequisite: `libdbhealth_ext.so`
listed in `trusted.lst` and the dbhealth table created once.

## 5. Running sqld in a container

The official image is `ghcr.io/tursodatabase/libsql-server`. Two things to
know, both learned the hard way:

1. **The command must start with `/bin/sqld`** (exact string) — the image's
   entrypoint only injects its managed `--db-path` and listen address when it
   sees that argv[0], and the default workdir is not writable for other
   spellings:

   ```sh
   podman run -d --name sqld -p 8880:8080 \
     -v ./ext-dir:/ext:Z \
     ghcr.io/tursodatabase/libsql-server:latest \
     /bin/sqld --extensions-path /ext
   ```

2. **glibc must match.** The image is Debian-based (glibc 2.31 as of this
   writing); an extension built on a newer host fails to `dlopen` with
   `` version `GLIBC_2.34' not found `` — which SQLite masks as
   `…libtimeless_ext.so.so: cannot open shared object file`. If you see the
   double-`.so` error, it's an ABI mismatch, not a path problem. Fix: build
   the extension inside a matching container, e.g.

   ```sh
   podman run --rm -v "$PWD":/src:Z -w /src rust:bullseye \
     cargo build --release -p timeless-ext
   ```

   or run sqld natively (section 1) and skip the issue entirely.

## 6. Replication flourish

Because the shadow tables are ordinary tables in an ordinary libSQL database,
replication needs nothing from this extension. Run the container/binary as a
`primary`, point a replica at its gRPC port, and the compressed telemetry
replicates with everything else — the replica reads it with the same
extension loaded locally.

The bandwidth consequence deserves its own sentence: SQLite's WAL is
page-level, and points only touch pages *after* compression at flush, so
whatever replication transport you use — replica sync, S3-compatible
bottomless storage, a periodic file ship — carries 6–200x fewer bytes
than row-level telemetry would. Compress at the edge, pay for the
compressed bytes upstream; on cellular backhaul that's the difference
between gigabytes and megabytes a month (see the README's edge table).

---

*Everything on this page and every cell in the [Livebook tour](tour.livemd)
was verified 2026-07-27 against sqld 0.24.33 (built from libsql main) with
the extension loaded via `--extensions-path`. Measured behavior (ingest →
flush → cross-connection query with name pushdown in 0.19ms) is recorded in
[RESULTS.md](../RESULTS.md#sqld-self-hosted-libsql-server-over-http).*
