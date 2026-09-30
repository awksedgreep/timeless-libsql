# Showcase ideas

Linux and BEAM process accounting showcase compressed metric history. The next
demonstrations can give logs, traces, and SQL correlation equally concrete
roles: applications keeping compressed operational history in an ordinary
SQLite database, alongside their own tables.

These are proposed applications of the current storage primitives. Collectors,
instrumentation, and presentation would still need to be built. The
[storage overview](../README.md#storage-models), [SQL API](SQL_API_REFERENCE.md),
and [embedded Rust guide](EMBEDDED_RUST.md) describe the available foundations.

## Recommended starting points

Build the **CI and build recorder first**. Timeless's own release verification
provides a substantial real workload, exercises all three signals, and can
produce a downloadable database that makes the result tangible.

Build the **network flight recorder next** for a broader audience. A home
network provides understandable, real data and a familiar question to answer
when something goes wrong.

## Network flight recorder

**Question:** Why was the internet terrible at 9:14?

Collect DNS queries, connection summaries, latency, packet-loss measurements,
and interface counters. Demonstrate finding the affected devices, failed
lookups, and destinations during a slowdown.

This would showcase repetitive log compression, indexed metadata, time-window
queries, and joins between telemetry and ordinary device-inventory tables.
Start with DNS and connection summaries; packet payloads introduce a different
storage problem.

## CI and build recorder

**Question:** What made this commit take twice as long?

Record build and test steps as spans, compiler and test output as logs, and
resource usage as metrics. Produce a consistent `build.db` artifact that
someone can download and investigate locally with the extension loaded.

This would showcase all three signals, portable investigations, and comparisons
joined to commit and test metadata. Use Timeless's own release verification as
the first workload, then compare a normal run with one containing a known slow
step. OpenTelemetry defines
[pipeline and task spans](https://opentelemetry.io/docs/specs/semconv/cicd/cicd-spans/)
that can guide the instrumentation.

## AI agent execution history

**Question:** Where did this task spend its time and tokens?

Represent model calls and tool executions as spans, and record retries and
outcomes as structured events. Join token usage to a versioned pricing table
and task results.

This would showcase nested execution traces, typed attributes, and SQL
connecting operational behavior to cost and success. Demonstrate finding a
retry loop that made one task unusually expensive. Measure compression
separately for metadata and message content; their behavior will differ.

## Home energy and equipment history

**Question:** What changed when we adjusted the heating schedule?

Record electricity use, solar production, battery charge, temperatures, and
equipment state changes. Join readings to tariffs and device information.

This would showcase regular-series compression, raw-to-rollup retention, and
useful analysis on a small, offline machine. Move from a year of hourly history
to a recent detailed window, with the storage cost visible throughout. Make
the retained raw window and older rollup resolutions explicit.

## Embedded application flight recorder

**Question:** Can we diagnose the problem from the diagnostic database?

Add recording to a desktop app, CLI, or appliance: operation timings, queue
depths, errors, and configuration-change events. Export a consistent database
snapshot for investigation with the extension loaded.

This would showcase telemetry embedded directly in an application, local
operation, and portable support artifacts. For a crash demonstration, show
recovery through the last completed flush: buffered, unflushed rows are not
durable against process loss. The [flush contract](GUIDE.md#3-the-one-concept-you-must-know-flush)
explains that boundary.

## Demonstration evidence

Each demonstration should answer one recognizable question, then reveal the
cost of keeping enough history to answer it:

- Retained history, sampling cadence, and row or sample counts.
- Total database size including indexes, with any remaining WAL reported.
- Collection overhead, including CPU and memory use.
- Time to answer the question with the demonstrated query.
- Compression against a stated baseline containing the same data, with raw
  retention and rollups accounted for separately.

Use measured results from the actual workload. The existing process-accounting
results motivate the next showcase; they do not establish compression ratios
for different data. Follow the [query evidence protocol](QUERY_EVIDENCE.md)
when publishing performance claims.
