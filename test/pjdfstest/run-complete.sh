#!/usr/bin/env bash
#
# Full POSIX validation: pjdfstest (pjd) + fuse_integration extras
# (flock/fcntl, rename concurrency) that pjdfstest does not cover.
#
# Usage:
#   test/pjdfstest/run-complete.sh
#   WEED_BIN=/path/to/weed test/pjdfstest/run-complete.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEED_BIN="${WEED_BIN:-weed}"
FUSE_TEST_DIR="${SCRIPT_DIR}/../fuse_integration"

echo "==> Phase 1: pjdfstest (pjd POSIX suite)"
WEED_BIN="${WEED_BIN}" "${SCRIPT_DIR}/run.sh"

echo ""
echo "==> Phase 2: fuse_integration POSIX extras (locks + rename races)"
if ! command -v go >/dev/null 2>&1; then
  echo "go not found; skipping fuse_integration extras" >&2
  exit 1
fi

if ! command -v fusermount3 >/dev/null 2>&1 && ! command -v fusermount >/dev/null 2>&1; then
  echo "fusermount not found; skipping fuse_integration extras" >&2
  exit 1
fi

export PATH="$(dirname "$(command -v "${WEED_BIN}" 2>/dev/null || echo "${WEED_BIN}")"):${PATH}"
if ! command -v weed >/dev/null 2>&1; then
  for candidate in \
    "${SCRIPT_DIR}/../../weed/weed" \
    "${SCRIPT_DIR}/../../weed" \
    "${SCRIPT_DIR}/../weed/weed"; do
    if [ -x "${candidate}" ]; then
      export PATH="$(dirname "${candidate}"):${PATH}"
      break
    fi
  done
fi
(
  cd "${FUSE_TEST_DIR}"
  if [ ! -f go.mod ]; then
    go mod init seaweedfs-fuse-tests
    go mod tidy
  fi
  go test -v -timeout 30m -run 'TestPosixFileLocking|TestRenameAtomicity' .
)

echo ""
echo "==> Phase 3: fuse_integration stress tests (with -dlm=true)"
(
  cd "${FUSE_TEST_DIR}"
  go test -v -timeout 45m -run 'TestStressOperations|TestConcurrentFileOperations' .
)

echo ""
echo "==> All POSIX validation phases passed"
