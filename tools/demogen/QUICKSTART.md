# Demo quickstart

Prereqs: Rust toolchain, stock `sqlite3`.

On macOS, Apple's `/usr/bin/sqlite3` refuses to load extensions. Install a real
one and put it first on `PATH` for the whole session — `screencast.py` resolves
`sqlite3` through `PATH`:

```sh
brew install sqlite
export PATH="$(brew --prefix sqlite)/bin:$PATH"
```

The `.load` lines below carry no file suffix on purpose: SQLite appends the
platform-native one (`.so` on Linux, `.dylib` on macOS), so the same commands
work on both.

```sh
git clone https://github.com/awksedgreep/timeless-libsql
cd timeless-libsql
cargo build --release -p timeless-ext
cd tools/demogen/ext && cargo build --release && cd ../../..
```

Run the whole demo inside one sqlite3 session:

```text
$ sqlite3 demo.db
.load ./target/release/libtimeless_ext
.load ./tools/demogen/ext/target/release/libtimeless_demogen
PRAGMA auto_vacuum=INCREMENTAL;    -- must come before any other write
PRAGMA journal_mode=WAL;
.mode list --charlimit 0 --linelimit 0
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

## Recording the screencast

Don't type it on camera — `screencast.py` drives the whole tour through a
real sqlite3 session with simulated keystrokes (prompt-synced, paced for
reading), so every take is pristine and identical:

```sh
rm -f demo.db
python3 tools/demogen/screencast.py demo.db large          # rehearse it once
rm -f demo.db
asciinema rec --window-size 120x32 \
  -c 'python3 tools/demogen/screencast.py demo.db large' demo.cast
agg demo.cast demo.gif      # optional: gif/video for the blog post
```

Pass `--window-size` explicitly. The driver sets its inner pty to 120x32 so
sqlite3 formats for that width, but asciinema records at its own geometry —
and asciinema 3.x *silently ignores* `--cols`/`--rows`, so a mis-flagged take
looks fine until the 80-column playback wraps every table.

`agg` caps idle gaps at 5s by default, which is why the GIF runs shorter than
the recording.

The tour: seed, series count, the incident error-bucket ramp, error logs, the
log→trace pivot, and the storage report. Edit the `SCRIPT` list at the
top of screencast.py to change the shots.

For a take that includes the dashboards in a browser, use any screen
recorder and run `timeless_demo('follow', 120)` in the terminal while
the UI updates alongside.
