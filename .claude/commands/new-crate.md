---
description: Scaffold a new workspace crate following project patterns with comprehensive tests and documentation
---

Create a new crate in the `crates/` directory based on the design specification provided by the user.

**Design Input:**
The user will provide the overall design for the new crate, including:
- Crate name and purpose
- Core types and data structures
- Public API surface
- Dependencies on other workspace crates
- Integration points with existing components

**Implementation Process:**

1. **Analyze Existing Patterns**
   - Launch 2-3 parallel agents to study:
     - Task 1: Examine `crates/types/`, `crates/common/`, and other existing crates for structural patterns (module organization, error handling, public API design)
     - Task 2: Review dependency management patterns in root `Cargo.toml` and how crates reference workspace dependencies
     - Task 3: Analyze test patterns across workspace (unit test structure, property tests, test helpers)
   - Synthesize findings to ensure new crate matches established conventions

2. **Create Crate Structure**
   ```
   crates/new-crate/
   ├── Cargo.toml           # Workspace-aligned dependencies
   ├── src/
   │   ├── lib.rs           # Public API and module declarations
   │   ├── types.rs         # Core data structures
   │   ├── error.rs         # Error types (if needed)
   │   └── [feature].rs     # Feature-specific modules
   └── tests/
       └── integration.rs   # Integration tests (if applicable)
   ```

3. **Scaffold Core Files**

   **Cargo.toml:**
   - Add to workspace members in root `Cargo.toml` first
   - Reference all dependencies with `{ workspace = true }`
   - Pin any new dependencies in root `[workspace.dependencies]` section
   - Include `dev-dependencies` for testing (proptest, etc.)

   **src/lib.rs:**
   - Clear module organization with `pub mod` declarations
   - Comprehensive crate-level documentation explaining purpose, examples, and integration points
   - Re-export key types at crate root for ergonomic imports

   **src/types.rs:**
   - Core data structures with derived traits (Debug, Clone, PartialEq where appropriate)
   - Serde serialization if needed (following patterns in `crates/types/`)
   - Builder patterns or constructors for complex types

   **src/error.rs:**
   - Custom error enum using `thiserror` if multiple error cases exist
   - Implement `std::error::Error` and provide context
   - Follow error handling patterns from `crates/common/`

4. **Write Tests First (TDD Approach)**
   - Unit tests in `mod tests` within each source file
   - Test all public API functions with edge cases
   - Property-based tests using `proptest` for data structures (name them `prop_*`)
   - Integration tests in `tests/` if crate interacts with other components
   - Target 90%+ coverage from the start

5. **Implement Minimal Working Version**
   - Follow the design specification provided
   - Keep functions small and focused (single responsibility)
   - Use descriptive names tied to database concepts
   - Add inline documentation for non-obvious logic
   - Prefer expressive enum/struct names (UpperCamelCase)

6. **Documentation**
   - Add crate-level docs with examples in `src/lib.rs`
   - Document all public functions, types, and modules
   - Include usage examples in doc comments (these become doc tests)
   - Create a minimal README.md (< 50 lines) with purpose and quick example

7. **Validation**
   ```bash
   cargo check                                      # Fast validation
   cargo test --package new-crate                   # Run tests
   cargo fmt -- --check                             # Formatting
   cargo clippy --package new-crate --all-features  # Lints
   scripts/coverage.sh -- --package new-crate       # Coverage report
   ```

8. **Integration Checklist**
   - ✅ Crate added to workspace members in root `Cargo.toml`
   - ✅ All dependencies use `{ workspace = true }`
   - ✅ New dependencies added to `[workspace.dependencies]` first
   - ✅ Tests achieve 90%+ coverage
   - ✅ All clippy lints pass
   - ✅ Public API is documented with examples
   - ✅ Follows naming conventions (snake_case modules, UpperCamelCase types)
   - ✅ Error handling matches project patterns
   - ✅ README provides quick start

**Output:**
- Create all necessary files with complete implementations
- Run validation commands and report results
- Show coverage report summary
- Provide next steps for integration with other components

**Key Principles:**
- **Modularity**: Clear separation of concerns, minimal coupling
- **Testability**: Write tests first, aim for high coverage
- **Clarity**: Expressive names, comprehensive docs, simple interfaces
- **Consistency**: Match patterns from existing crates exactly
- **Incrementality**: Start minimal, ensure green builds, iterate

**Example Design Specification Format:**
```
Crate: query-optimizer
Purpose: Transform logical query plans into optimized execution plans
Core Types: LogicalPlan, PhysicalPlan, OptimizationRule
Dependencies: types (workspace), common (workspace)
Public API: fn optimize(plan: LogicalPlan) -> Result<PhysicalPlan>
Integration: Called by query executor after parsing
```

After scaffolding, commit the new crate with message: `feat: add [crate-name] with initial implementation and tests`
