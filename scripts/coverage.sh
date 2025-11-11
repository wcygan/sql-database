#!/usr/bin/env bash
set -euo pipefail

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is not installed. Install it with 'cargo install cargo-llvm-cov'." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LCOV_DIR="${REPO_ROOT}/target/llvm-cov"
HTML_DIR="${LCOV_DIR}/html"
LCOV_FILE="${LCOV_DIR}/lcov.info"

pushd "${REPO_ROOT}" >/dev/null

# Ensure the output directories exist before generating reports.
mkdir -p "${LCOV_DIR}"

# Run the workspace tests with coverage instrumentation but skip immediate report generation.
cargo llvm-cov --workspace --all-features --no-report "$@"

# Produce machine-consumable LCOV output for CI or coverage services.
cargo llvm-cov report --lcov --output-path "${LCOV_FILE}"

# Emit an HTML report for local inspection.
cargo llvm-cov report --html --output-dir "${LCOV_DIR}"

popd >/dev/null

cat <<EOF
Coverage artifacts generated:
  HTML report: ${HTML_DIR}/index.html
  LCOV file: ${LCOV_FILE}
EOF
