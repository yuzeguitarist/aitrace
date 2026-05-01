#!/usr/bin/env bash
set -euo pipefail

APP_NAME="aitrace"
DEFAULT_GIT_URL="https://github.com/<owner>/aitrace.git"
GIT_URL="${AITRACE_GIT_URL:-$DEFAULT_GIT_URL}"
INSTALL_ROOT="${AITRACE_INSTALL_ROOT:-${HOME}/.aitrace-src}"
SRC_DIR="${INSTALL_ROOT}/${APP_NAME}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" >/dev/null 2>&1 && pwd || pwd)"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required. Install Rust first: https://rustup.rs/" >&2
  exit 1
fi

if [[ -f "${SCRIPT_DIR}/Cargo.toml" && -z "${AITRACE_GIT_URL:-}" ]]; then
  SRC_DIR="${SCRIPT_DIR}"
  echo "Installing from local source ${SRC_DIR}..."
elif [[ -d "${SRC_DIR}/.git" ]]; then
  echo "Updating ${SRC_DIR}..."
  git -C "${SRC_DIR}" pull --ff-only
else
  mkdir -p "${INSTALL_ROOT}"
  if [[ "${GIT_URL}" == *"<owner>"* ]]; then
    echo "error: set AITRACE_GIT_URL to your GitHub repository URL, for example:" >&2
    echo "  AITRACE_GIT_URL=https://github.com/you/aitrace.git bash install.sh" >&2
    exit 1
  fi
  echo "Cloning ${GIT_URL} into ${SRC_DIR}..."
  git clone "${GIT_URL}" "${SRC_DIR}"
fi

echo "Installing ${APP_NAME}..."
cargo install --path "${SRC_DIR}" --force

echo
echo "Installed:"
command -v "${APP_NAME}" || true
echo
"${APP_NAME}" doctor || true
