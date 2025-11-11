---
description: Analyze coverage gaps and generate tests to improve code coverage
---

Analyze code coverage for this Rust workspace and generate targeted tests to improve coverage.

## Workflow

**Phase 1: Generate Coverage Data**
1. Run `./scripts/coverage.sh` to generate workspace-wide coverage
2. Verify the script succeeds and produces `target/llvm-cov/lcov.info`
3. If the script fails, diagnose the issue and report to the user

**Phase 2: Analyze Coverage Gaps**
1. Parse `target/llvm-cov/lcov.info` to identify files with coverage below 90%
2. Use `cargo llvm-cov report --workspace --show-missing-lines` to get detailed missing line numbers
3. For each file with gaps:
   - Read the source file
   - Extract context around uncovered lines (10 lines before/after each `DA:line,0` entry)
   - Identify what logic is untested (error branches, edge cases, validation, etc.)

**Phase 3: Generate Test Recommendations**
For each file with coverage gaps:
1. **Summarize uncovered regions**: List line ranges and describe the untested behavior
2. **Categorize missing coverage**:
   - Error handling paths
   - Edge case validation
   - Boundary conditions
   - Integration scenarios
3. **Propose concrete tests**:
   - Reference existing test patterns in `mod tests` blocks or `tests/` directories
   - Suggest unit tests for isolated functions
   - Suggest property tests for data structures (using `proptest`)
   - Suggest integration tests for cross-crate behavior
4. **Provide test scaffolding**: Generate actual test code following project patterns

**Phase 4: Prioritization**
Order recommendations by:
1. Critical paths (error handling, data validation)
2. High-impact areas (public APIs, core logic)
3. Low-hanging fruit (simple branches, easy edge cases)

## Output Format

### Coverage Summary
```
Workspace Coverage: XX%
Files below 90% threshold: N

Top gaps:
1. crates/parser/src/lib.rs - 75% (25 uncovered lines)
2. crates/storage/src/backend.rs - 82% (15 uncovered lines)
...
```

### Per-File Analysis
For each file:
```markdown
## crates/parser/src/lib.rs (75% coverage)

### Uncovered Lines: 42-45, 67, 89-92

**Lines 42-45**: Error handling for invalid SQL syntax
**Line 67**: Edge case for empty input validation
**Lines 89-92**: Boundary condition for max query length

### Recommended Tests

#### Test 1: Invalid SQL Syntax Handling
**Type**: Unit test
**Location**: `crates/parser/src/lib.rs` in `mod tests`
**Code**:
\`\`\`rust
#[test]
fn test_parse_invalid_sql_syntax() {
    let result = parse("SELECT * FORM users");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ParseError::InvalidSyntax(_)));
}
\`\`\`

[Additional tests...]
```

## Constraints

- Follow workspace dependency patterns: use `{ workspace = true }` for all dependencies
- Respect existing test organization (`mod tests` for units, `tests/` for integration)
- Use `proptest` for property-based tests in `crates/types`
- Ensure tests follow `rustfmt` defaults and pass `cargo clippy`
- Generate tests that would pass immediately (with correct assertions)

## Implementation Approach

1. **Don't analyze files sequentially** - if multiple files need coverage analysis, use the Task tool to launch 2-3 exploration agents in parallel
2. **Focus on actionable gaps** - prioritize uncovered lines that represent meaningful untested behavior
3. **Provide complete test code** - not just descriptions, but copy-paste-ready test functions
4. **Align with module responsibilities** - reference crate purposes from project structure

## Success Criteria

- Identify all files below 90% coverage
- Provide specific line numbers and context for gaps
- Generate concrete, runnable test code
- Prioritize recommendations by impact
- Enable user to copy-paste tests and immediately run `cargo test`

## Example Usage

User types: `/coverage-improve`
1. Script runs coverage generation
2. Agent parses LCOV data
3. Agent analyzes top 3-5 files with biggest gaps (in parallel if needed)
4. Agent provides prioritized test recommendations with code
5. User implements suggested tests
6. User runs `./scripts/coverage.sh` again to verify improvement
