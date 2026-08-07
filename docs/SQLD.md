# Serving Timeless SQL through self-hosted sqld

Self-hosted `sqld` can load `libtimeless_ext` and expose its public SQLite
surface through Hrana over HTTP. This is a direct SQL deployment: it does not
run the Timeless signal APIs, PromQL/MetricsQL/LogsQL parsers, Timeless auth
policy, owner lease, maintenance scheduler, or graceful signal-aware drain.
Those remain separate products and must not concurrently own the same writable
database.

The canonical schemas, hidden inputs, formats, commands, and limits are in the
[SQLite extension API reference](SQL_API_REFERENCE.md). This guide uses only
those public surfaces. Private shadow tables are never an application or
inspection API.

## Compatibility scope

The checked direct-libSQL gate is pinned to Rust crate `libsql` 0.9.30 in
`tools/libsql-check/Cargo.lock`. The HTTP procedure below was rechecked on
2026-08-07 with the official `libsql-server-v0.24.32` release at commit
`40c272de85ee4e62d722c5ccae5da2e76b4253a1` on x86-64 GNU/Linux. That
upstream release documents both the `trusted.lst` extension directory and the
[Hrana 3 pipeline protocol](https://github.com/tursodatabase/libsql/blob/libsql-server-v0.24.32/docs/HRANA_3_SPEC.md).

Do not build from a floating branch or deploy an image named `latest` and
infer compatibility from this result. Pin the sqld artifact by release and
checksum, load the exact Timeless extension intended for the host ABI, run the
capability probe and the three-signal smoke below, and retain those identities
with deployment evidence.

## 1. Install a pinned sqld

Official release archives and checksums are attached to
[`libsql-server-v0.24.32`](https://github.com/tursodatabase/libsql/releases/tag/libsql-server-v0.24.32).
For x86-64 GNU/Linux:

```sh
curl --fail --location \
  --output libsql-server-x86_64-unknown-linux-gnu.tar.xz \
  https://github.com/tursodatabase/libsql/releases/download/libsql-server-v0.24.32/libsql-server-x86_64-unknown-linux-gnu.tar.xz
curl --fail --location \
  --output libsql-server-x86_64-unknown-linux-gnu.tar.xz.sha256 \
  https://github.com/tursodatabase/libsql/releases/download/libsql-server-v0.24.32/libsql-server-x86_64-unknown-linux-gnu.tar.xz.sha256
sha256sum --check libsql-server-x86_64-unknown-linux-gnu.tar.xz.sha256
tar -xJf libsql-server-x86_64-unknown-linux-gnu.tar.xz
```

Choose the matching upstream archive for another supported sqld platform.
The Timeless extension must also match the operating system, architecture,
and runtime libc. A successful checksum does not make an extension built on a
newer GNU/Linux host loadable in an older container.

## 2. Stage the trusted extension

Build the default loadable artifact and create the exact list sqld validates:

```sh
cargo build --release -p timeless-ext
mkdir -p ext-dir
cp target/release/libtimeless_ext.so ext-dir/
(cd ext-dir && sha256sum libtimeless_ext.so > trusted.lst)
```

Use `.dylib` and `shasum -a 256` on macOS. Regenerate `trusted.lst` whenever
the extension bytes change. sqld refuses a missing, malformed, or mismatched
hash before loading the file.

## 3. Start one local owner

```sh
./libsql-server-x86_64-unknown-linux-gnu/sqld \
  --db-path telemetry.sqld \
  --extensions-path ./ext-dir \
  --http-listen-addr 127.0.0.1:8880
```

Loopback is deliberate. The pinned sqld has its own JWT/basic-auth and network
configuration; Timeless server tokens and request limits do not wrap raw
Hrana SQL. Do not expose an unauthenticated arbitrary-SQL endpoint on a
non-loopback interface. A deployment that needs the signal APIs' language,
auth, tenancy, admission, response limits, and shutdown behavior should run
the signal-specific Rust server instead.

sqld's `--db-path` is a directory it owns. Do not point a Timeless Rust signal
server or another writer process at its files. Connections in this sqld
process load the trusted extension independently and share the table engine
through the extension's process-local registry.

## 4. Negotiate, create, write, and flush

Hrana integers are JSON strings so 64-bit values remain exact. The following
single pipeline checks the ABI, creates all three virtual tables, writes one
row per signal, and makes the rows durable:

```sh
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data-binary @- http://127.0.0.1:8880/v3/pipeline <<'JSON'
{
  "baton": null,
  "requests": [
    {"type":"execute","stmt":{"sql":"SELECT timeless_capabilities()"}},
    {"type":"execute","stmt":{"sql":"CREATE VIRTUAL TABLE metrics USING timeless_metrics"}},
    {"type":"execute","stmt":{"sql":"CREATE VIRTUAL TABLE logs USING timeless_logs(index_keys='service',timestamp_unit='us')"}},
    {"type":"execute","stmt":{"sql":"CREATE VIRTUAL TABLE traces USING timeless_traces"}},
    {"type":"execute","stmt":{"sql":"INSERT INTO metrics(name,ts,value,labels) VALUES(?1,?2,?3,?4)","args":[{"type":"text","value":"cpu"},{"type":"integer","value":"10"},{"type":"float","value":42.5},{"type":"text","value":"{\"host\":\"edge-1\"}"}]}},
    {"type":"execute","stmt":{"sql":"INSERT INTO logs(ts,level,message,metadata) VALUES(1000001,'alert','temperature limit','{\"service\":\"sensor\",\"nested\":{\"ok\":false}}')"}},
    {"type":"execute","stmt":{"sql":"INSERT INTO traces(trace_id,span_id,name,service,kind,status,start_ts,duration_ns,attributes,status_description,events,resource,instrumentation_scope) VALUES(x'11111111111111111111111111111111',x'2222222222222222','sensor.read','sensor','client','error',1000000001,25000,'{\"temperature\":91.5}','threshold exceeded','[{\"name\":\"alarm\",\"time_unix_nano\":1000000010,\"attributes\":{\"limit\":90}}]','{\"service.name\":\"sensor\"}','{\"name\":\"edge-sdk\"}')"}},
    {"type":"execute","stmt":{"sql":"INSERT INTO metrics(metrics) VALUES('flush')"}},
    {"type":"execute","stmt":{"sql":"INSERT INTO logs(logs) VALUES('flush')"}},
    {"type":"execute","stmt":{"sql":"INSERT INTO traces(traces) VALUES('flush')"}},
    {"type":"close"}
  ]
}
JSON
```

Require `data_abi=1`, `sql_surface_version=1`, all three expected storage
modules, the batch generations the client will send, and every query guard it
depends on. The complete handshake is documented in
[capability and version handshake](SQL_API_REFERENCE.md#capability-and-version-handshake).

A pipeline is one Hrana stream, not an implicit transaction. Statements above
autocommit individually. For an atomic group, execute `BEGIN IMMEDIATE`, the
writes/flushes, and `COMMIT` on one stream, handle every intermediate result,
and issue `ROLLBACK` on failure. Do not split a transaction across unrelated
requests unless the client correctly preserves and serializes the returned
baton.

## 5. Query from a closed and reopened stream

The previous pipeline closes its stream. A second request exercises public
query and rich-fidelity surfaces without relying on connection-local state:

```sh
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data-binary @- http://127.0.0.1:8880/v3/pipeline <<'JSON'
{
  "baton": null,
  "requests": [
    {"type":"execute","stmt":{"sql":"SELECT value,labels FROM timeless_latest('metrics','cpu',NULL,0,20)"}},
    {"type":"execute","stmt":{"sql":"SELECT level,message,metadata,service FROM logs WHERE service='sensor'"}},
    {"type":"execute","stmt":{"sql":"SELECT hex(trace_id),status_description,events,resource,instrumentation_scope FROM traces WHERE trace_id=x'11111111111111111111111111111111'"}},
    {"type":"close"}
  ]
}
JSON
```

Expect metric value `42.5`, severity `alert` with typed nested metadata, and
the exact rich-span description, events, resource, and instrumentation scope.
Use bound Hrana arguments for application values; the literals above keep the
deployment smoke copyable.

Every ordinary SQL recipe in [SQL equivalents](QUERY_SQL_EQUIVALENTS.md) can
run through Hrana when its setup and limits are preserved. PromQL, MetricsQL,
and LogsQL syntax itself is intentionally outside the extension and is not
accepted by this raw SQL endpoint.

## 6. Durability, maintenance, and shutdown

sqld does not know which virtual tables need a Timeless flush when the process
receives a shutdown signal. Automatic thresholds are 4,096 points per metric
series, 8,192 logs, and 8,192 spans, but a smaller accepted tail remains only
in the extension process until an explicit flush. Before planned shutdown:

1. stop producers;
2. wait for admitted requests to finish;
3. issue `flush` for every created Timeless table;
4. run required `compact`/`rollup` or bounded `optimize` commands;
5. ensure those statements succeed and close their streams;
6. use sqld/SQLite's supported checkpoint or snapshot procedure; and
7. terminate sqld gracefully and wait for exit.

The virtual tables have no embedded maintenance scheduler. The sqld operator
must schedule public maintenance/retention commands and enforce query work and
response limits. SIGKILL can lose the unflushed tail; persisted transactions
remain subject to the host database's recovery guarantees.

`dbhealth` is a separate extension and not part of the telemetry bundle. When
loaded explicitly under sqld, its background sampler stays disabled to avoid
out-of-band replication-log writes; schedule its public `sample` command as
described in [DBHEALTH.md](DBHEALTH.md).

## 7. Backup and replication boundary

All durable Timeless compressed blocks, catalogs, and indexes live in the
sqld-managed database. Host-supported libSQL replication or bottomless backup
can therefore carry committed Timeless state without an extension-specific
replication protocol. The in-memory buffer is not stored or replicated, and a
replica must load a compatible extension before querying the virtual tables.

This guide does not claim a particular replication topology, S3-compatible
provider, recovery-point objective, or hot-copy procedure. Configure and test
those through the pinned sqld/libSQL version, then validate counts, timestamp
ranges, float values, typed log metadata, and rich spans after restore. Never
copy only the main database file while WAL frames may be outstanding, and
never inspect or mutate private shadow tables as a replication shortcut.
