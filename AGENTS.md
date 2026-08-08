# Repository Git Workflow

- Work only in this canonical checkout. Never use `git worktree`, create a duplicate clone or copy, or create a temporary checkout under `/tmp`, `.worktrees`, or anywhere else.
- Use `main` for routine work. Use a short-lived branch in this same checkout only when a feature genuinely benefits from isolation.
- Before starting, inspect the current branch and working tree. Do not hide, overwrite, relocate, or silently absorb existing changes.
- Finish every task by reconciling the canonical checkout: merge or fast-forward completed feature work into `main`, remove the temporary branch when safe, switch back to `main`, and leave the working tree clean. If existing user work makes that impossible, stop and explain it explicitly.
- Cut releases only from `main`. Push `main` before the tag, make the tag point at the released `main` commit, verify the release and its artifacts, and leave this checkout clean on `main`.
