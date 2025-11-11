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

### Tips
- Use `./scripts/coverage.sh --no-clean` to reuse prior builds for faster iterations.
- Coverage builds are unoptimized and can be slower than `cargo test`; avoid running them on every commit and prefer targeted test runs while iterating.
- To exclude generated files, append `--ignore-filename-regex 'target/'` (or similar) to the script invocation.
