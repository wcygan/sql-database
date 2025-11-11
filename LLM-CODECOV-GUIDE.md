# LLM Code Coverage Guide

This document explains how an automated agent (or human assisted by an LLM) can generate, parse, and act on workspace-wide coverage data in this repository. It builds atop the standard `scripts/coverage.sh` wrapper so tooling and humans follow the same process.

## 1. Generate Coverage Data
1. Ensure prerequisites:
   - Rust installed via `rustup` (stable toolchain).
   - `cargo-llvm-cov` present (`cargo install cargo-llvm-cov` once).
2. From the repo root run:
   ```bash
   ./scripts/coverage.sh
   ```
   The script runs `cargo llvm-cov --workspace --all-features --no-report`, then:
   - Emits LCOV text at `target/llvm-cov/lcov.info` (machine-friendly).
   - Emits HTML artifacts under `target/llvm-cov/html/` (human-friendly).
3. Optionally limit scope by forwarding cargo filters, e.g. `./scripts/coverage.sh -- --package storage`.

LLM note: when the agent must run the script, capture the command output (especially failure diagnostics) so the user understands why coverage might be missing.

## 2. Consume Machine-Readable Output
The LCOV file is a sequence of records:
```
SF:/abs/path/to/file.rs
DA:<line>,<hit_count>
DA:42,0
end_of_record
```
- `SF` marks the file; `DA` lines record per-line execution counts; `0` means uncovered.
- Use `rg` to target files of interest before sending data to an LLM:
  ```bash
  rg -n "SF:.*storage" -A5 target/llvm-cov/lcov.info
  ```
- For structured prompts, consider re-exporting as JSON: `cargo llvm-cov report --json --output-path target/llvm-cov/coverage.json`.

When handing data to an LLM:
1. Trim to the files that need attention (e.g., those below a threshold or containing `DA:line,0`).
2. Provide short excerpts of the corresponding source files so the model can reason about missing branches.
3. Ask targeted questions such as: “For the following uncovered lines, what scenarios are untested and how can we add tests in crate X?”

## 3. Interpreting Results and Suggesting Tests
- Align uncovered lines with module responsibilities (see `AGENTS.md` for crate scopes). This helps the LLM recommend the right test location (unit test vs. integration test).
- Encourage the LLM to:
  1. Summarize each affected file’s uncovered regions.
  2. Describe the behavior implied by those lines (e.g., error branches, edge-case validation).
  3. Propose test ideas referencing existing test modules (`mod tests` blocks or `tests/` folders).
  4. Highlight any prerequisite fixtures or data builders needed.
- Example analysis prompt:
  ```
  Using the LCOV data below for crates/storage/src/lib.rs and its current tests,
  list the uncovered line numbers, describe what logic they represent, and suggest
  concrete unit or property tests to cover them.
  ```

## 4. Automating Recommendations
For scripted agents:
1. Run coverage (`./scripts/coverage.sh`).
2. Parse LCOV to extract files with coverage below a threshold. A minimal approach is to scan for `LF:` (total lines) and `LH:` (hit lines) markers; more precise parsing can be done with a short Python/Rust helper.
3. For each file, open the source and gather contextual snippets (e.g., 10 lines around each `DA:line,0`).
4. Feed the snippets plus the uncovered line numbers to the LLM, asking for remediation guidance or new test cases.
5. Aggregate the recommendations into TODOs or PR descriptions.

## 5. Uploading to Coverage Services
If the goal is CI integration (e.g., Codecov):
```bash
./scripts/coverage.sh
./codecov -f target/llvm-cov/lcov.info
```
The LCOV artifact is what Codecov consumes; the same file can be shared with an LLM for deeper reasoning, so ensure it is preserved as a build artifact where automated agents can access it.

---

By following this workflow, an LLM can both regenerate ground-truth coverage metrics and reason about the concrete gaps, leading to actionable suggestions for new or improved tests.
