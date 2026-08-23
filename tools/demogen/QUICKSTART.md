# Demo quickstart

Prereqs: Rust toolchain, stock `sqlite3`.

```sh
git clone https://github.com/awksedgreep/timeless-libsql
cd timeless-libsql
cargo build --release -p timeless-ext
cd tools/demogen/ext && cargo build --release && cd ../../..
```

Run the whole demo inside one sqlite3 session:

```text
$ sqlite3 demo.db
.load ./target/release/libtimeless_ext.so
.load ./tools/demogen/ext/target/release/libtimeless_demogen.so
PRAGMA auto_vacuum=INCREMENTAL;    -- must come before any other write
PRAGMA journal_mode=WAL;
.timer on
SELECT timeless_demo('seed','large');   -- ~245k series, 5M logs, ~3M spans
```

Progress streams live; the query result is the ingest + compression
report (raw vs stored, per signal, indexes and file size separate).
A fresh database gives exact ratios immediately.

Afterwards:

```sql
SELECT timeless_demo('report');       -- re-run the storage report any time
SELECT timeless_demo('follow', 60);   -- live data for 60s (tail/dashboard demo)
SELECT timeless_demo('info');         -- built-in cheat sheet of queries
```

Profiles: `small` (~4k series, seconds), `medium` (~35k), `large`
(~245k). Bigger than large — use the CLI instead:

```sh
cd tools/demogen && cargo build --release
./target/release/timeless-demogen seed --db big.db --profile large \
  --pods 200 --logs 10000000 --traces 600000
```

Delete the `.db` file to start over; seeding is deterministic per seed
(`timeless_demo('seed','large', 7)` for a different one).
