#!/usr/bin/env bash
set -euo pipefail

APP_NAME="aitrace"
REPO="yuzeguitarist/aitrace"
BASE_URL="https://github.com/${REPO}/releases/latest/download"

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required to download ${APP_NAME}." >&2
  exit 1
fi

case "$(uname -s)" in
Darwin) ;;
*)
  echo "error: ${APP_NAME} currently supports macOS only." >&2
  exit 1
  ;;
esac

case "$(uname -m)" in
arm64 | aarch64)
  TARGET="aarch64-apple-darwin"
  ;;
x86_64)
  TARGET="x86_64-apple-darwin"
  ;;
*)
  echo "error: unsupported CPU architecture: $(uname -m)" >&2
  exit 1
  ;;
esac

choose_bin_dir() {
  if [[ -n "${AITRACE_BIN_DIR:-}" ]]; then
    printf '%s\n' "${AITRACE_BIN_DIR}"
    return
  fi

  local dir
  for dir in "${HOME}/.local/bin" "${HOME}/bin" "/opt/homebrew/bin" "/usr/local/bin"; do
    case ":${PATH}:" in
    *":${dir}:"*)
      if [[ -d "${dir}" && -w "${dir}" ]]; then
        printf '%s\n' "${dir}"
        return
      fi
      ;;
    esac
  done

  printf '%s\n' "${HOME}/.local/bin"
}

BIN_DIR="$(choose_bin_dir)"
ASSET="${APP_NAME}-${TARGET}.tar.gz"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading ${APP_NAME} for ${TARGET}..."
if ! curl -fsSL "${BASE_URL}/${ASSET}" -o "${TMP_DIR}/${ASSET}"; then
  echo "error: failed to download ${BASE_URL}/${ASSET}" >&2
  echo "The latest GitHub release may not have a prebuilt binary for ${TARGET} yet." >&2
  exit 1
fi

if curl -fsSL "${BASE_URL}/${ASSET}.sha256" -o "${TMP_DIR}/${ASSET}.sha256"; then
  if command -v shasum >/dev/null 2>&1; then
    (cd "${TMP_DIR}" && shasum -a 256 -c "${ASSET}.sha256")
  else
    echo "warning: shasum not found; skipping checksum verification." >&2
  fi
else
  echo "warning: checksum file not found; skipping checksum verification." >&2
fi

tar -xzf "${TMP_DIR}/${ASSET}" -C "${TMP_DIR}"
if [[ ! -x "${TMP_DIR}/${APP_NAME}" ]]; then
  echo "error: archive did not contain an executable ${APP_NAME} binary." >&2
  exit 1
fi

mkdir -p "${BIN_DIR}"
install -m 0755 "${TMP_DIR}/${APP_NAME}" "${BIN_DIR}/${APP_NAME}"

echo
echo "Installed ${APP_NAME} to ${BIN_DIR}/${APP_NAME}"
if ! command -v "${APP_NAME}" >/dev/null 2>&1; then
  echo
  echo "Add this to your shell profile if ${APP_NAME} is not found:"
  echo "  export PATH=\"${BIN_DIR}:\$PATH\""
fi
echo
"${BIN_DIR}/${APP_NAME}" doctor || true
