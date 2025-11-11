# Repository Guidelines

## Project Structure & Module Organization
The educational workspace keeps shared dependencies in the root `Cargo.toml`, while teaching-focused crates live under `crates/`. The `expr` crate illustrates how SQL expressions are parsed, type-checked, and evaluated before execution; it leans on schema primitives from `crates/types` and cross-cutting utilities in `crates/common`. Use `src/` for expression ASTs, planners, and evaluators; keep pedagogical examples or walkthroughs in `examples/` when needed. Generated artifacts stay in `target/`, and broader integration demos belong in workspace-level `tests/`.

## Build, Test, and Development Commands
- `cargo check` — fastest path to validate the crate when iterating on tutorial code or exercises.
- `cargo test expr` — runs the expression-specific unit tests that back each educational chapter; omit the suffix for the whole workspace.
- `cargo fmt -- --check` — enforces consistent formatting so readers see canonical Rust.
- `cargo clippy --all-targets --all-features` — flags non-idiomatic constructs, reinforcing best practices students should emulate.

## Coding Style & Naming Conventions
Favor clear, descriptive names that mirror SQL concepts (e.g., `BinaryOp`, `ScalarExpr`). Modules stay snake_case (`mod logical_builder`), while types and traits use UpperCamelCase. Follow `rustfmt` defaults with 4-space indentation and trailing commas to keep diffs clean for learners reviewing history. Keep shared dependency versions pinned under `[workspace.dependencies]` using `workspace = true` to demonstrate centralized versioning.

## Testing Guidelines
Each lesson ideally ships with adjacent unit tests under `mod tests` so students can inspect implementation and verification side by side. Use example-driven tests for parser coverage and property-based tests (via `proptest`) when modeling evaluator correctness or type inference edge cases. Always run `cargo test expr` prior to publishing a tutorial or exercise update, and document failing seeds in teaching notes to help others reproduce tricky scenarios.

## Commit & Pull Request Guidelines
Commit messages should teach by example: imperative, scoped, and contextual (e.g., `Explain constant folding safeguards`). Group related instructional changes and squash noisy fixups. PRs ought to state the learning objective, summarize how the `expr` crate now supports it, and list validation commands (`cargo fmt`, `cargo clippy`, `cargo test expr`). Link to curricula or issue trackers describing upcoming lessons, and include screenshots, REPL transcripts, or diagrams when they clarify how the crate fits into the educational SQL pipeline.

## Security & Configuration Tips
Even in an instructional setting, avoid hardcoded secrets or credentials when demonstrating expression evaluation; prefer environment-driven configuration as modeled in `crates/common`. Vet new dependencies for permissive licenses suitable for teaching, and add targeted regression tests whenever expression logic could influence query safety or resource usage examples.*** End Patch
