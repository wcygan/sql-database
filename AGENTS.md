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

## Testing Guidelines
- Unit tests live alongside implementation files using the `mod tests` pattern.
- Property-based tests in `crates/types` leverage `proptest`; name them `prop_*` for clarity.
- Add targeted integration tests in `tests/` directories when behavior spans crates.
- Run `cargo test` locally before opening a PR; include failing-seed reproduction steps if a proptest fails.
- When you need executable documentation for test coverage, run `scripts/coverage.sh`; it runs the entire workspace with coverage instrumentation and leaves reports under `target/llvm-cov/`.

## Commit & Pull Request Guidelines
- Write commits in the imperative mood, e.g., `Pin workspace dependencies` or `Add row serialization tests`.
- Squash trivial fixups; keep logical units separate for clarity.
- PRs should describe motivation, summarize code changes, list validation commands, and link relevant issues.
- Include screenshots or logs when UI/CLI behavior changes, and mention any follow-up work needed.
