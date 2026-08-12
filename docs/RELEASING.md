# Releasing

The tag-triggered `Release artifacts` workflow is the *last* verification,
not the first. Everything below runs green **before** the tag exists. This
checklist exists because v0.6.3 was tagged on the root workspace's cargo
suite alone (226 tests) and every platform job failed in seconds on a stale
`Cargo.lock` — a failure any single post-bump `--locked` cargo command would
have caught locally.

## Pre-tag checklist

Run everything **after** the version bump — verification of anything other
than the exact tree being tagged verifies nothing.

1. **Version bump**: both workspaces move together (`Cargo.toml`,
   `servers/Cargo.toml`), per the pairing policy in
   [COMPATIBILITY.md](COMPATIBILITY.md). Line changes (0.x → 0.y) also move
   the capability document, both floors, and their guard tests.
2. **Lockfiles**: `cargo check --workspace --locked` in the root AND
   `servers/` (and `tools/query-harness/`). The release workflow builds with
   `--locked`; a bump without a lock refresh fails every platform job.
3. **Cargo suites**: `cargo test --workspace --locked` in the root and
   `servers/`.
4. **CLI suite**: `./tests/cli.sh` — all sections.
5. **Correctness suites**: `./tests/correctness.sh` for each section
   (`r1 r2 r3 r4 r8 logs-rich`), plus `./tests/dbhealth.sh` and
   `./tests/crash.sh`.
6. **Query contracts + oracles** (the `query-contracts` gate, runnable
   locally):
   `cargo test --manifest-path tools/query-harness/Cargo.toml --locked`,
   then `-- contracts` and `-- oracle validate`.
7. **Everything above runs locally, in one pass, without pausing between
   steps.** The dispatchable CI workflows re-run the same binaries on
   slower shared runners and have produced false failures from timing
   flakes; they earn their keep on changes that skipped the local stack,
   not as a ritual after ones that ran it. CI's unique job is the
   four-platform artifact build, and the tag triggers that automatically.
8. **Changelog**: a dated section for the release under the versioning
   policy at the top of `CHANGELOG.md`; update the `release-target` comment.

Only then: tag from `main`, push the tag, watch `Release artifacts` to
completion, and verify the GitHub release carries all four native archives
plus `SHA256SUMS` (see [ARTIFACTS.md](ARTIFACTS.md)).

## If a tagged release fails its artifact run

Follow the v0.4.0 precedent: the bad tag stays as a documented source-only
tag (record it in the changelog), the fix ships as the next patch version.
Do not move or delete published tags — consumers pin them by rev.
