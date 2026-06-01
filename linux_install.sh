#!/usr/bin/env bash
set -euo pipefail

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required but not installed."
  exit 1
fi

usage() {
  cat <<'EOUSAGE'
Usage:
  ./install.sh [--install] [--system|--user]
  ./install.sh --uninstall [--system|--user]

Options:
  --install     Install oppo-multiplexer (default action)
  --uninstall   Uninstall oppo-multiplexer
  --system      Force system scope (/etc, /usr/local/bin, systemctl)
  --user        Force user scope (~/.config, ~/.local/bin, systemctl --user)
  -h, --help    Show this help

Notes:
  - On install without --system/--user, scope is picked from listen port.
  - Privileged listen ports (<1024) require --system and root.
EOUSAGE
}

ACTION="install"
SCOPE_OVERRIDE=""
for arg in "$@"; do
  case "$arg" in
    --install)
      ACTION="install"
      ;;
    --uninstall)
      ACTION="uninstall"
      ;;
    --system)
      SCOPE_OVERRIDE="system"
      ;;
    --user)
      SCOPE_OVERRIDE="user"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg"
      usage
      exit 1
      ;;
  esac
done

if [[ "${ACTION}" == "uninstall" ]]; then
  INSTALL_SCOPE="${SCOPE_OVERRIDE}"
  if [[ -z "${INSTALL_SCOPE}" ]]; then
    if [[ "${EUID}" -eq 0 ]]; then
      INSTALL_SCOPE="system"
    else
      INSTALL_SCOPE="user"
    fi
  fi

  if [[ "${INSTALL_SCOPE}" == "system" ]]; then
    if [[ "${EUID}" -ne 0 ]]; then
      echo "System uninstall requires sudo/root."
      exit 1
    fi
    BIN_PATH="/usr/local/bin/oppo-multiplexer"
    ENV_FILE="/etc/default/oppo-multiplexer"
    UNIT_FILE="/etc/systemd/system/oppo-multiplexer.service"
    SYSTEMCTL_CMD=(systemctl)
  else
    if [[ "${EUID}" -eq 0 ]]; then
      echo "User uninstall must run as the target user (no sudo), or pass --system."
      exit 1
    fi
    BIN_PATH="${HOME}/.local/bin/oppo-multiplexer"
    ENV_FILE="${HOME}/.config/oppo-multiplexer/env"
    UNIT_FILE="${HOME}/.config/systemd/user/oppo-multiplexer.service"
    SYSTEMCTL_CMD=(systemctl --user)
  fi

  "${SYSTEMCTL_CMD[@]}" disable --now oppo-multiplexer >/dev/null 2>&1 || true
  rm -f "${UNIT_FILE}" "${ENV_FILE}" "${BIN_PATH}"
  "${SYSTEMCTL_CMD[@]}" daemon-reload
  "${SYSTEMCTL_CMD[@]}" reset-failed >/dev/null 2>&1 || true

  echo "Uninstall complete (${INSTALL_SCOPE})."
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required but not installed."
  exit 1
fi
if ! command -v tar >/dev/null 2>&1; then
  echo "tar is required but not installed."
  exit 1
fi

# Matches .github/workflows/release.yml naming:
# oppo-multiplexer-linux-amd64.tar.gz
# oppo-multiplexer-linux-arm64.tar.gz
case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: $(uname -m)"
    exit 1
    ;;
esac

read -r -p "Release tag to install (default latest): " RELEASE_TAG
RELEASE_TAG="${RELEASE_TAG:-latest}"

read -r -p "Oppo IP/host (example 192.168.1.50): " OPPO_HOST
if [[ -z "${OPPO_HOST}" ]]; then
  echo "Oppo IP/host cannot be empty."
  exit 1
fi

read -r -p "Oppo port (default 23): " OPPO_PORT
OPPO_PORT="${OPPO_PORT:-23}"

read -r -p "Listen port for oppo-multiplexer: " LISTEN_PORT

read -r -p "Timeout seconds (default 10): " TIMEOUT_SECONDS
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-10}"

if ! [[ "${OPPO_PORT}" =~ ^[0-9]+$ ]] || (( OPPO_PORT < 1 || OPPO_PORT > 65535 )); then
  echo "Invalid Oppo port: ${OPPO_PORT}"
  exit 1
fi
if ! [[ "${LISTEN_PORT}" =~ ^[0-9]+$ ]] || (( LISTEN_PORT < 1 || LISTEN_PORT > 65535 )); then
  echo "Invalid listen port: ${LISTEN_PORT}"
  exit 1
fi
if ! [[ "${TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "Invalid timeout seconds: ${TIMEOUT_SECONDS}"
  exit 1
fi

# Requirement:
# - privileged listen port (<1024): run as root
# - non-privileged listen port: install/run as current user
INSTALL_SCOPE=""
BIN_PATH=""
ENV_FILE=""
UNIT_FILE=""
WANTED_BY=""
SYSTEMCTL_CMD=()
if [[ "${SCOPE_OVERRIDE}" == "system" ]]; then
  INSTALL_SCOPE="system"
elif [[ "${SCOPE_OVERRIDE}" == "user" ]]; then
  INSTALL_SCOPE="user"
elif (( LISTEN_PORT < 1024 )); then
  INSTALL_SCOPE="system"
else
  INSTALL_SCOPE="user"
fi

if [[ "${INSTALL_SCOPE}" == "system" ]]; then
  if [[ "${EUID}" -ne 0 ]]; then
    echo "System install requires sudo/root."
    exit 1
  fi
  if (( LISTEN_PORT >= 1024 )) && [[ "${SCOPE_OVERRIDE}" == "system" ]]; then
    echo "Using system scope by explicit --system override."
  fi
  BIN_PATH="/usr/local/bin/oppo-multiplexer"
  ENV_FILE="/etc/default/oppo-multiplexer"
  UNIT_FILE="/etc/systemd/system/oppo-multiplexer.service"
  WANTED_BY="multi-user.target"
  SYSTEMCTL_CMD=(systemctl)
  echo "Using system service as root."
else
  if [[ "${EUID}" -eq 0 ]]; then
    echo "User install must run as your normal user (no sudo)."
    exit 1
  fi
  if (( LISTEN_PORT < 1024 )); then
    echo "Listen port ${LISTEN_PORT} is privileged; use --system with sudo/root instead."
    exit 1
  fi
  BIN_PATH="${HOME}/.local/bin/oppo-multiplexer"
  ENV_FILE="${HOME}/.config/oppo-multiplexer/env"
  UNIT_FILE="${HOME}/.config/systemd/user/oppo-multiplexer.service"
  WANTED_BY="default.target"
  SYSTEMCTL_CMD=(systemctl --user)
  mkdir -p "${HOME}/.local/bin" "${HOME}/.config/oppo-multiplexer" "${HOME}/.config/systemd/user"
  echo "Using user service."
fi

ASSET="oppo-multiplexer-linux-${ARCH}.tar.gz"
if [[ "${RELEASE_TAG}" == "latest" ]]; then
  URL="https://github.com/henrikwidlund/oppo-multiplexer/releases/latest/download/${ASSET}"
else
  URL="https://github.com/henrikwidlund/oppo-multiplexer/releases/download/${RELEASE_TAG}/${ASSET}"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading ${URL}"
curl -fL "${URL}" -o "${TMP_DIR}/${ASSET}"
tar -xzf "${TMP_DIR}/${ASSET}" -C "${TMP_DIR}"

if [[ ! -f "${TMP_DIR}/oppo-multiplexer" ]]; then
  echo "Archive did not contain expected binary: oppo-multiplexer"
  exit 1
fi

install -m 755 "${TMP_DIR}/oppo-multiplexer" "${BIN_PATH}"

cat > "${ENV_FILE}" <<EOCFG
OPPO_HOST=${OPPO_HOST}
OPPO_PORT=${OPPO_PORT}
LISTEN_PORT=${LISTEN_PORT}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS}
RUST_LOG=info
EOCFG

cat > "${UNIT_FILE}" <<EOUNIT
[Unit]
Description=OPPO Multiplexer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_PATH} \${LISTEN_PORT} \${OPPO_HOST}:\${OPPO_PORT} \${TIMEOUT_SECONDS}
Restart=always
RestartSec=2
TimeoutStopSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
EOUNIT

cat >> "${UNIT_FILE}" <<EOUNIT

[Install]
WantedBy=${WANTED_BY}
EOUNIT

"${SYSTEMCTL_CMD[@]}" daemon-reload
"${SYSTEMCTL_CMD[@]}" enable --now oppo-multiplexer

echo
echo "Install complete."
if [[ "${INSTALL_SCOPE}" == "system" ]]; then
  systemctl status oppo-multiplexer --no-pager || true
  echo
  echo "Config: ${ENV_FILE}"
  echo "Logs:   journalctl -u oppo-multiplexer -f"
else
  systemctl --user status oppo-multiplexer --no-pager || true
  echo
  echo "Config: ${ENV_FILE}"
  echo "Logs:   journalctl --user -u oppo-multiplexer -f"
fi
