#!/usr/bin/env bash
set -euo pipefail

APP_NAME="aitrace"
DEFAULT_GIT_URL="https://github.com/yuzeguitarist/aitrace.git"
GIT_URL="${AITRACE_GIT_URL:-$DEFAULT_GIT_URL}"
INSTALL_ROOT="${AITRACE_INSTALL_ROOT:-${HOME}/.aitrace-src}"
SRC_DIR="${INSTALL_ROOT}/${APP_NAME}"
SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR=""

if [[ -n "${SCRIPT_PATH}" && -f "${SCRIPT_PATH}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")" >/dev/null 2>&1 && pwd)"
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required. Install Rust first: https://rustup.rs/" >&2
  exit 1
fi

if [[ -n "${SCRIPT_DIR}" && -f "${SCRIPT_DIR}/Cargo.toml" && -z "${AITRACE_GIT_URL:-}" ]]; then
  SRC_DIR="${SCRIPT_DIR}"
  echo "Installing from local source ${SRC_DIR}..."
elif [[ -d "${SRC_DIR}/.git" ]]; then
  if ! command -v git >/dev/null 2>&1; then
    echo "error: git is required to update ${SRC_DIR}." >&2
    exit 1
  fi
  echo "Updating ${SRC_DIR}..."
  git -C "${SRC_DIR}" pull --ff-only
else
  if ! command -v git >/dev/null 2>&1; then
    echo "error: git is required to clone ${GIT_URL}." >&2
    exit 1
  fi
  mkdir -p "${INSTALL_ROOT}"
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
