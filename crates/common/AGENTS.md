# Repository Guidelines

## Scope & Role in the Workspace
- `crates/common` powers the educational SQL database by centralizing shared errors, traits, and helpers so lesson crates can stay focused on query logic.
- Modules deliver stable contracts: error enums surface friendly messages, conversion helpers bridge examples to real SQL types, and feature flags mirror each learning objective.
- Changes here ripple through every tutorial—treat APIs as part of the curriculum.

## Module Organization
- `src/errors.rs`: canonical error taxonomy used by executors, planners, and storage layers; maintain exhaustive pattern matches to keep examples instructive.
- `src/convert` and other helper modules host reusable parsing/serialization routines; prefer adding adapters here before duplicating logic in learner modules.
- Tests live beside their modules to demonstrate usage patterns that tutorial steps can point to; keep examples approachable and well-commented.

## Build, Test, and Development Commands
- `cargo check -p common` — validate this crate quickly while iterating on educational examples.
- `cargo test -p common` — exercise helper and error semantics without running the entire workspace.
- `cargo fmt` / `cargo fmt -- --check` — ensure snippets copied into docs stay consistent.
- `cargo clippy -p common --all-targets` — catch surprising edge cases before students hit them.

## Coding Style & Naming Conventions
- Follow `rustfmt` defaults (4-space indent, trailing commas); most code doubles as reference material.
- Modules stay snake_case (`mod diagnostics`), public types use UpperCamelCase (`SqlCommonError`), and trait names describe intent (`DisplaysHint`).
- Keep dependency versions sourced from `[workspace.dependencies]` to mirror lessons on workspace management.

## Testing & Validation Guidelines
- Use `mod tests` blocks to show canonical usage of helpers; keep assertions descriptive so they can be cited in documentation.
- For logic that influences student exercises (e.g., type conversions), augment unit tests with property checks when feasible.
- Run `cargo test -p common` before publishing guides or merging; include failing seeds or reproduction steps if property tests uncover gaps.

## Commit & Pull Request Guidelines
- Commit messages follow the imperative mood (`Clarify parse error hints`) to document how the learning experience evolves.
- PRs should link to the lesson, exercise, or bug they unblock, outline changes to shared APIs, and list validation commands (`check`, `fmt`, `clippy`, `test`).
- Mention any downstream crates that need updates so reviewers can gauge educational impact; attach sample error output when messaging changes.
