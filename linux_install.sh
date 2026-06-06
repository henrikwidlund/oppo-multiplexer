#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOUSAGE'
Usage:
  ./linux_install.sh [--install]
  ./linux_install.sh --uninstall

Options:
  --install     Install oppo-multiplexer (default action)
  --uninstall   Uninstall oppo-multiplexer
  -h, --help    Show this help

Notes:
  - sudo/root is required to install and uninstall.
  - Binary is installed to /opt/oppo-multiplexer/.
  - Config is installed to /etc/oppo-multiplexer/.
  - For privileged listen ports (<1024) the service runs as root.
  - For non-privileged listen ports the service runs as the invoking user.
EOUSAGE
}

ACTION="install"
for arg in "$@"; do
  case "$arg" in
    --install)
      ACTION="install"
      ;;
    --uninstall)
      ACTION="uninstall"
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

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required but not installed."
  exit 1
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must be run with sudo/root."
  exit 1
fi

BIN_DIR="/opt/oppo-multiplexer"
BIN_PATH="${BIN_DIR}/oppo-multiplexer"
ENV_DIR="/etc/oppo-multiplexer"
ENV_FILE="${ENV_DIR}/env"
UNIT_FILE="/etc/systemd/system/oppo-multiplexer.service"

if [[ "${ACTION}" == "uninstall" ]]; then
  systemctl disable --now oppo-multiplexer >/dev/null 2>&1 || true
  rm -f "${UNIT_FILE}" "${ENV_FILE}" "${BIN_PATH}"
  rmdir "${ENV_DIR}" 2>/dev/null || true
  rmdir "${BIN_DIR}" 2>/dev/null || true
  systemctl daemon-reload
  systemctl reset-failed >/dev/null 2>&1 || true
  echo "Uninstall complete."
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

read -r -p "Max consecutive timed-out requests before reconnect (default 3): " MAX_CONSECUTIVE_TIMEOUTS
MAX_CONSECUTIVE_TIMEOUTS="${MAX_CONSECUTIVE_TIMEOUTS:-3}"

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
if ! [[ "${MAX_CONSECUTIVE_TIMEOUTS}" =~ ^[0-9]+$ ]] || (( MAX_CONSECUTIVE_TIMEOUTS < 1 || MAX_CONSECUTIVE_TIMEOUTS > 100 )); then
  echo "Invalid max consecutive timeouts: ${MAX_CONSECUTIVE_TIMEOUTS} (must be in the range 1-100)"
  exit 1
fi

# Privileged ports (<1024): service runs as root.
# Non-privileged ports: service runs as the invoking user.
SERVICE_USER=""
SERVICE_GROUP=""
if (( LISTEN_PORT < 1024 )); then
  echo "Privileged listen port ${LISTEN_PORT}: service will run as root."
else
  if [[ -z "${SUDO_USER:-}" || "${SUDO_USER}" == "root" ]]; then
    echo "For non-privileged listen ports, re-run as: sudo ./linux_install.sh (from your normal user account)."
    exit 1
  fi
  SERVICE_USER="${SUDO_USER}"
  SERVICE_GROUP="$(id -gn "${SUDO_USER}")"
  echo "Non-privileged listen port ${LISTEN_PORT}: service will run as ${SERVICE_USER}:${SERVICE_GROUP}."
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
curl --proto "=https" --tlsv1.2 -sSfL "${URL}" -o "${TMP_DIR}/${ASSET}"
tar -xzf "${TMP_DIR}/${ASSET}" -C "${TMP_DIR}"

if [[ ! -f "${TMP_DIR}/oppo-multiplexer" ]]; then
  echo "Archive did not contain expected binary: oppo-multiplexer"
  exit 1
fi

mkdir -p "${BIN_DIR}" "${ENV_DIR}"
install -m 755 "${TMP_DIR}/oppo-multiplexer" "${BIN_PATH}"

# Config readable by root and the service user only.
if [[ -n "${SERVICE_USER}" ]]; then
  ENV_GROUP="${SERVICE_GROUP}"
else
  ENV_GROUP="root"
fi

cat > "${ENV_FILE}" <<EOCFG
OPPO_HOST=${OPPO_HOST}
OPPO_PORT=${OPPO_PORT}
LISTEN_PORT=${LISTEN_PORT}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS}
MAX_CONSECUTIVE_TIMEOUTS=${MAX_CONSECUTIVE_TIMEOUTS}
RUST_LOG=info
EOCFG
chown "root:${ENV_GROUP}" "${ENV_FILE}"
chmod 640 "${ENV_FILE}"

# Build the unit, inserting User/Group only for non-privileged installs.
cat > "${UNIT_FILE}" <<EOUNIT
[Unit]
Description=OPPO Multiplexer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_PATH} \${LISTEN_PORT} \${OPPO_HOST}:\${OPPO_PORT} \${TIMEOUT_SECONDS} \${MAX_CONSECUTIVE_TIMEOUTS}
Restart=always
RestartSec=2
TimeoutStopSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
EOUNIT

if [[ -n "${SERVICE_USER}" ]]; then
  {
    echo "User=${SERVICE_USER}"
    echo "Group=${SERVICE_GROUP}"
  } >> "${UNIT_FILE}"
fi

cat >> "${UNIT_FILE}" <<'EOUNIT'

[Install]
WantedBy=multi-user.target
EOUNIT

systemctl daemon-reload
systemctl enable oppo-multiplexer
systemctl restart oppo-multiplexer

echo
echo "Install complete."
systemctl status oppo-multiplexer --no-pager || true
echo
echo "Binary: ${BIN_PATH}"
echo "Config: ${ENV_FILE}"
echo "Logs:   journalctl -u oppo-multiplexer -f"
echo "        journalctl -b -t oppo-multiplexer"
