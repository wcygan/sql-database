# Repository Guidelines

## Project Structure & Module Organization
- Workspace root (`Cargo.toml`) tracks shared dependencies; source crates live under `crates/`.
- `crates/types/` defines core data structures and serialization logic; keep schema-centric code here.
- `crates/common/` hosts reusable helpers and error types that other crates can import.
- Generated artifacts land in `target/`; do not commit its contents.

## Dependency Management
- Pin every third-party or shared crate version under `[workspace.dependencies]` in the root `Cargo.toml`; child crates must never specify their own versions.
- Reference those shared dependencies from member crates using `{ workspace = true }`, even for path-only crates such as `common`, `expr`, or `types`.
- If a crate needs extra features, add them to the workspace definition so all consumers stay aligned.
- When introducing a new dependency, update the workspace table first, then wire it into the specific crate via `workspace = true`.

## Build, Test, and Development Commands
- `cargo check` — fast validation of code and dependency wiring without producing binaries.
- `cargo test` — executes all unit and property tests across workspace members.
- `cargo fmt -- --check` — verify formatting before submitting changes; omit `--check` to auto-format.
- `cargo clippy --all-targets --all-features` — run lints to enforce idiomatic Rust.
- `scripts/coverage.sh` — wraps `cargo llvm-cov` to run workspace-wide tests with coverage instrumentation, emitting both HTML (`target/llvm-cov/html/index.html`) and LCOV (`target/llvm-cov/lcov.info`) outputs; install the tool once via `cargo install cargo-llvm-cov`. Pass extra cargo filters (e.g., `-- --package parser`) to narrow the run.

## Coding Style & Naming Conventions
- Follow `rustfmt` defaults (4-space indentation, trailing commas for multi-line literals).
- Modules use snake_case (`mod storage_backend`); types and traits use UpperCamelCase (`SqlValue`).
- Prefer expressive enum/struct names tied to database concepts; avoid abbreviations unless ubiquitous (e.g., `sql`).
- Keep shared dependency versions pinned via `[workspace.dependencies]`; reference them with `workspace = true`.

## Builder Pattern with Bon
- Use the `bon` crate (version 3) for ergonomic compile-time-checked builders on constructors with 4+ parameters.
- Apply `#[bon::bon]` to impl blocks and `#[builder]` to methods that should have builder APIs.
- Prefer builders for: multi-parameter constructors, public configuration structs, complex operator initialization.
- Pattern: `#[bon::bon] impl MyStruct { #[builder] pub fn new(...) -> Self { ... } }`
- Usage: `MyStruct::builder().field1(val1).field2(val2).build()`
- Benefits: compile-time validation, named parameters, any-order initialization, zero runtime cost.
- Examples: `IndexScanExec::builder()` (crates/executor/src/scan.rs), `Config` struct (future: crates/common/src/lib.rs).

## Testing Guidelines
- Unit tests live alongside implementation files using the `mod tests` pattern.
- Property-based tests in `crates/types` leverage `proptest`; name them `prop_*` for clarity.
- Add targeted integration tests in `tests/` directories when behavior spans crates.
- Run `cargo test` locally before opening a PR; include failing-seed reproduction steps if a proptest fails.
- When you need executable documentation for test coverage, run `scripts/coverage.sh`; it runs the entire workspace with coverage instrumentation and leaves reports under `target/llvm-cov/`.

## Clippy Lint Standards
- All code must pass `cargo clippy --all-targets --all-features` with zero warnings before merging.
- Address clippy suggestions by fixing the underlying issue, not by suppressing warnings unless absolutely necessary.
- Common fixes:
  - Use `io::Error::other()` instead of `io::Error::new(ErrorKind::Other, _)`
  - Use `.first()` instead of `.get(0)`
  - Use array literals `[x; n]` instead of `vec![x; n]` for compile-time constant arrays
  - Use iterator methods (`.iter().enumerate()`) instead of indexing loops when appropriate
  - Add `.truncate(true/false)` when using `.create(true)` in OpenOptions to make intent explicit
  - Remove unused imports and variables, or prefix with `_` if intentionally unused
- For test helpers that will be used in future tests, use `#[allow(dead_code)]` with a comment explaining why.
- Run clippy after every significant change; don't batch up lint fixes—address them immediately.
- If clippy suggests a change that would reduce code clarity, discuss in PR review rather than suppressing.

## Commit & Pull Request Guidelines
- Write commits in the imperative mood, e.g., `Pin workspace dependencies` or `Add row serialization tests`.
- Squash trivial fixups; keep logical units separate for clarity.
- PRs should describe motivation, summarize code changes, list validation commands, and link relevant issues.
- Include screenshots or logs when UI/CLI behavior changes, and mention any follow-up work needed.
