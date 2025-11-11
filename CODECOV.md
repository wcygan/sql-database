# Code Coverage Guide

This workspace standardizes on [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) for gathering Rust source-coverage. The helper script `scripts/coverage.sh` wraps the recommended invocation so contributors get identical reports locally and in CI.

## Prerequisites
- Rust toolchain via `rustup` (stable is fine). The script will automatically request the `llvm-tools-preview` component the first time it is missing.
- `cargo-llvm-cov` installed once per machine:
  ```bash
  cargo install cargo-llvm-cov
  ```
- Sufficient disk space under `target/` for the profdata artifacts and HTML bundle (expect tens of MB).
- Optional for CI uploads: a Codecov token exported as `CODECOV_TOKEN` when required by your organization’s Codecov plan.

## Quickstart
1. From the repository root run:
   ```bash
   ./scripts/coverage.sh
   ```
   The script runs `cargo llvm-cov --workspace --all-features --no-report`, then emits both `lcov.info` and an HTML snapshot using `cargo llvm-cov report`. Extra cargo filters are passed through, so `./scripts/coverage.sh -- --package parser` restricts coverage to the parser crate while retaining shared instrumentation.
2. Inspect results:
   - Human-friendly HTML dashboard at `target/llvm-cov/html/index.html`.
   - Machine-readable LCOV data at `target/llvm-cov/lcov.info`.
3. (Optional) Upload to Codecov after the script finishes:
   ```bash
   curl -Os https://uploader.codecov.io/latest/macos/codecov
   chmod +x codecov
   ./codecov -f target/llvm-cov/lcov.info
   ```
   Replace the binary URL with the appropriate OS/arch variant in CI. The uploader exits non-zero if coverage is missing, so run it after `./scripts/coverage.sh`.

### Inspecting Coverage Without HTML
`cargo llvm-cov report` can summarize results directly in the terminal, which is handy for headless environments or when you only need a quick status:
```bash
cargo llvm-cov report --workspace --show-missing-lines
```
- Add `--package expr` (or another crate) to focus on a single member.
- Append `--json --output-path target/llvm-cov/report.json` if you need machine-readable summaries.
- Use `--ignore-filename-regex` to omit generated or vendored files from the printed table.

These invocations read the instrumentation emitted by `./scripts/coverage.sh`, so run the script (or an equivalent `cargo llvm-cov --no-report`) first to ensure the data is fresh.

### Tips
- Use `./scripts/coverage.sh --no-clean` to reuse prior builds for faster iterations.
- Coverage builds are unoptimized and can be slower than `cargo test`; avoid running them on every commit and prefer targeted test runs while iterating.
- To exclude generated files, append `--ignore-filename-regex 'target/'` (or similar) to the script invocation.
