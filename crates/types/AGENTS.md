# Repository Guidelines

## Educational Context & Goals
- This `crates/types` package is the learning hub for the educational SQL database: it exposes concrete `SqlValue`, schema metadata, and encoding primitives that downstream crates model against.
- Each module should illustrate a clear concept (e.g., type coercion, nullable handling) so that readers can trace SQL semantics from type definition to execution.
- Keep doc comments focused on pedagogy—embed small SQL snippets that show when to use a type or trait.

## Project Structure & Module Organization
- Workspace root `Cargo.toml` pins shared dependencies; every learning module (parser, planner, runtime) resides under `crates/`.
- `crates/types/src/` houses domain enums, trait-based serialization, and feature flags for optional lessons (e.g., temporal types).
- `crates/common/` supplies reusable error/reporting helpers so examples stay uncluttered; integration exercises land in `tests/`.
- Generated artifacts belong in `target/` and should never be checked in.

## Build, Test, and Development Commands
- `cargo check` — fastest way to validate that tutorial snippets compile before updating docs or slides.
- `cargo fmt` / `cargo fmt -- --check` — enforce consistent examples; run before copy-pasting into educational material.
- `cargo clippy --all-targets --all-features` — surfaces patterns worth discussing during instruction (e.g., unnecessary clones).
- `cargo test` — executes all unit and property tests to prove the data model behaves as described in the lessons.
- `cargo test -p types schema::` — narrow to a single teaching unit while iterating on an exercise.

## Coding Style & Naming Conventions
- Stick to rustfmt defaults (4-space indent, trailing commas) so diffs mirror other crates in the course.
- Modules stay snake_case (`mod decimal_repr`); exported types/traits use UpperCamelCase (`SqlTimestamp`); constants stick to SCREAMING_SNAKE_CASE.
- Prefer explicit names over abbreviations (`SqlInterval` over `SqlIntv`) to reinforce terminology students must learn.
- Reference common dependencies with `workspace = true` inside `Cargo.toml` to keep the curriculum reproducible.

## Testing Guidelines
- Co-locate unit tests using `mod tests`; narrate the scenario in test names (`prop_nullable_roundtrip_preserves_none`).
- Property-based tests rely on `proptest` support in `crates/common`; capture seeds in failures so students can reproduce edge cases.
- Add integration-style samples under `crates/types/tests/` when demonstrating interoperability with other educational crates.
- Run `cargo test` (or the targeted command above) before publishing lesson updates or submitting PRs.

## Commit & Pull Request Guidelines
- Use imperative commits that describe the learning value (`Explain decimal coercion`, `Add enum-backed SqlValue`); squash minor fixups.
- PRs should include: motivation in the context of the curriculum, a concise change summary, validation commands, and links to any lesson markdown affected.
- Attach screenshots or CLI logs if output changes influence teaching materials, and note follow-up work students should expect.
