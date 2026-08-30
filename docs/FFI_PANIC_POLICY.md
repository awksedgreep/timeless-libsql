# FFI panic policy

`libtimeless_ext` and `libdbhealth_ext` execute inside the SQLite or libSQL host
process. Their virtual-table implementations are called through rusqlite's
`extern "C"` callback adapters. That ABI does not permit Rust unwinding: if a
panic reaches one of those adapters, Rust aborts the process rather than
unwinding into SQLite.

The workspace does not set Cargo's global `panic = "abort"` profile option.
Keeping normal Rust unwind support allows recovery where a component defines a
sound boundary (rusqlite scalar functions do); it does not change the fail-stop
outcome when a panic reaches a non-unwinding virtual-table callback.

This project deliberately uses a **no-panic, fail-stop policy** at that
boundary:

- Public SQL values, virtual-table arguments, stored bytes, and database state
  must never cause a panic. Invalid or unsupported values return a
  `rusqlite::Error` (normally `ModuleError` or an appropriate SQLite code).
- Recoverable resource failures use checked arithmetic, bounded allocation or
  `try_reserve`, fallible lock/I/O/database operations, and error propagation.
- Poisoned process-global coordination locks recover the protected value when
  doing so preserves their invariant; they do not blindly `unwrap`.
- `assert!`, `expect`, indexing, infallible capacity growth, and unchecked
  integer arithmetic are not acceptable on data-derived production paths.
  Assertions that intentionally panic belong in tests.
- A residual panic is an invariant violation or implementation defect. The
  supported loadable extension may abort its host in that case. Hosts that
  require an availability boundary must isolate extension execution in a
  supervised process.

## Why callbacks do not use `catch_unwind`

An initialization-only guard would not cover query, update, transaction,
cursor, or destructor callbacks. Wrapping those callbacks mechanically is also
not a sound recovery guarantee: a panic can occur after a virtual table, cursor,
engine, shadow table, or transaction has been partially mutated. Returning an
ordinary SQLite error at that point would let the host continue with state the
callback did not establish or roll back according to its contract.

rusqlite already catches panics in scalar-function closures where it owns a
defined conversion to `Error::UnwindingPanic`; its virtual-table adapter does
not provide the equivalent recovery contract. Unless that contract changes,
fail-stop is safer than pretending an arbitrary vtab panic is recoverable.

## Review checklist

Changes on an extension callback path must answer all of the following:

1. Can any length, count, offset, timestamp, identifier, or decoded value come
   from SQL, a database page, or a stored block? Validate it before conversion,
   arithmetic, allocation, slicing, or indexing.
2. Can an allocation scale with that value? Enforce the format or operation
   limit first and use a fallible reservation where exhaustion is recoverable.
3. Can a mutex be poisoned or an I/O/database operation fail? Convert the
   condition to a stable SQL error or perform a documented invariant-preserving
   recovery.
4. Does an `unwrap`, `expect`, assertion, direct index, or arithmetic operation
   remain? Prove it is construction-time/static, replace it with a checked
   error path, or keep it test-only.
5. Does the regression exercise the release-relevant path and prove the host
   connection remains usable after the rejected statement?

This policy applies equally to the loadable-extension entry points and the Rust
embedding APIs: registration is fallible, and every later SQLite callback must
return errors for recoverable conditions. It is a coding and review contract,
not a claim that Rust or third-party code can never contain a bug.
