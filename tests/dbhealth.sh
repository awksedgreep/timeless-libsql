#!/usr/bin/env bash
# dbhealth standalone-extension test: build the health-only .so, then
# prove the headline contract — CREATE VIRTUAL TABLE begins collection,
# re-opening the database resumes it, the report renders, and the
# interactive 'sample' command still works. Driven from python3 sqlite3
# because the scheduler needs a process that stays alive between ticks.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== building dbhealth-ext (release) =="
cargo build -p dbhealth-ext --release --manifest-path "$ROOT/Cargo.toml"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

EXT="$ROOT/target/release/libdbhealth_ext" DB="$TMP/auto.db" python3 - <<'PYEOF'
import os, sqlite3, time

EXT, DB = os.environ["EXT"], os.environ["DB"]

def connect():
    c = sqlite3.connect(DB)
    c.enable_load_extension(True)
    c.load_extension(EXT)
    return c

# ── collection begins at CREATE ──────────────────────────────────────
c = connect()
c.execute("CREATE VIRTUAL TABLE dbhealth USING dbhealth(every=1)")
c.commit()
time.sleep(4.5)
n1 = c.execute("SELECT count(*) FROM dbhealth").fetchone()[0]
assert n1 > 0, "scheduler did not collect after create"
series = {r[0] for r in c.execute("SELECT DISTINCT name FROM dbhealth")}
assert "db_pages" in series and "db_file_bytes" in series, series

# interactive full sample still works and the report renders
c.execute("INSERT INTO dbhealth(dbhealth) VALUES ('sample')")
rep = dict(c.execute('SELECT "check", status FROM dbhealth_report').fetchall())
assert len(rep) == 7, rep
assert rep["sampling"] == "ok", rep

# unknown commands list 'sample'
try:
    c.execute("INSERT INTO dbhealth(dbhealth) VALUES ('bogus')")
    raise AssertionError("bogus command accepted")
except sqlite3.OperationalError as e:
    assert "sample" in str(e), e
c.close()

# ── collection RESUMES at reopen ─────────────────────────────────────
time.sleep(2.0)
c2 = connect()
n2 = c2.execute("SELECT count(*) FROM dbhealth").fetchone()[0]
time.sleep(3.5)
n3 = c2.execute("SELECT count(*) FROM dbhealth").fetchone()[0]
assert n3 > n2, f"scheduler did not resume on reopen ({n2} -> {n3})"

# every=0 opts out: a second table with no scheduler
c2.execute("CREATE VIRTUAL TABLE manual USING dbhealth(every=0)")
c2.commit()
time.sleep(2.6)
n4 = c2.execute("SELECT count(*) FROM manual").fetchone()[0]
assert n4 == 0, f"every=0 must not auto-collect (got {n4})"

# DROP stops cleanly and removes the views
c2.execute("DROP TABLE manual")
c2.execute("DROP TABLE dbhealth")
left = c2.execute(
    "SELECT count(*) FROM sqlite_master WHERE name LIKE 'dbhealth%'"
).fetchone()[0]
assert left == 0, f"{left} dbhealth objects left after DROP"
c2.close()
print("ALL DBHEALTH CHECKS PASSED")
PYEOF
