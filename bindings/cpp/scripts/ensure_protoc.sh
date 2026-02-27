#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

PROTOBUF_BASELINE_VERSION="${PROTOBUF_BASELINE_VERSION:-3.25.5}"
PROTOC_INSTALL_ROOT="${PROTOC_INSTALL_ROOT:-/tmp/fluss-cpp-tools}"
PROTOC_OS="${PROTOC_OS:-linux}"
PROTOC_ARCH="${PROTOC_ARCH:-x86_64}"
PROTOC_FORCE_INSTALL="${PROTOC_FORCE_INSTALL:-0}"
PROTOC_PRINT_PATH_ONLY="${PROTOC_PRINT_PATH_ONLY:-0}"

usage() {
  cat <<'EOF'
Usage: bindings/cpp/scripts/ensure_protoc.sh [--print-path]

Ensures a protoc binary matching the configured protobuf baseline is available.
Installs into a local cache directory (default: /tmp/fluss-cpp-tools) and prints
the protoc path on stdout.

Env vars:
  PROTOBUF_BASELINE_VERSION  Baseline protobuf version (default: 3.25.5)
  PROTOC_INSTALL_ROOT        Local cache root (default: /tmp/fluss-cpp-tools)
  PROTOC_OS                 protoc package OS (default: linux)
  PROTOC_ARCH               protoc package arch (default: x86_64)
  PROTOC_FORCE_INSTALL      1 to force re-download
  BAZEL_PROXY_URL           Optional proxy (sets curl/wget proxy envs if present)
EOF
}

for arg in "$@"; do
  case "$arg" in
    --print-path)
      PROTOC_PRINT_PATH_ONLY=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      usage >&2
      exit 1
      ;;
  esac
done

setup_proxy_env() {
  if [[ -n "${BAZEL_PROXY_URL:-}" ]]; then
    export http_proxy="${http_proxy:-$BAZEL_PROXY_URL}"
    export https_proxy="${https_proxy:-$BAZEL_PROXY_URL}"
    export HTTP_PROXY="${HTTP_PROXY:-$http_proxy}"
    export HTTPS_PROXY="${HTTPS_PROXY:-$https_proxy}"
  fi
}

normalize_version_for_protoc_release() {
  local v="$1"
  # Protobuf release packaging switched from v3.x.y to vX.Y for newer versions.
  # For our current agreed baseline (3.25.5), the protoc archive/tag is 25.5.
  if [[ "$v" =~ ^3\.([0-9]+\.[0-9]+)$ ]]; then
    local stripped="${BASH_REMATCH[1]}"
    local major="${stripped%%.*}"
    if [[ "$major" -ge 21 ]]; then
      echo "$stripped"
      return 0
    fi
  fi
  echo "$v"
}

version_matches_baseline() {
  local actual="$1"
  local baseline="$2"
  local actual_norm baseline_norm
  actual_norm="$(normalize_version_for_protoc_release "$actual")"
  baseline_norm="$(normalize_version_for_protoc_release "$baseline")"
  [[ "$actual" == "$baseline" || "$actual_norm" == "$baseline_norm" ]]
}

download_file() {
  local url="$1"
  local out="$2"

  if command -v curl >/dev/null 2>&1; then
    if [[ -n "${https_proxy:-}" || -n "${http_proxy:-}" ]]; then
      curl -fLk "$url" -o "$out"
    else
      curl -fL "$url" -o "$out"
    fi
    return 0
  fi

  if command -v wget >/dev/null 2>&1; then
    local wget_args=()
    if [[ -n "${https_proxy:-}" || -n "${http_proxy:-}" ]]; then
      wget_args+=(--no-check-certificate -e use_proxy=yes)
      if [[ -n "${https_proxy:-}" ]]; then
        wget_args+=(-e "https_proxy=${https_proxy}")
      fi
      if [[ -n "${http_proxy:-}" ]]; then
        wget_args+=(-e "http_proxy=${http_proxy}")
      fi
    fi
    wget "${wget_args[@]}" -O "$out" "$url"
    return 0
  fi

  echo "ERROR: neither curl nor wget is available for downloading protoc." >&2
  return 1
}

ensure_zip_tools() {
  command -v unzip >/dev/null 2>&1 || {
    echo "ERROR: unzip not found." >&2
    exit 1
  }
}

setup_proxy_env
ensure_zip_tools

if command -v protoc >/dev/null 2>&1; then
  existing_out="$(protoc --version 2>/dev/null || true)"
  if [[ "$existing_out" =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]; then
    existing_ver="${BASH_REMATCH[1]}"
    if version_matches_baseline "$existing_ver" "$PROTOBUF_BASELINE_VERSION"; then
      command -v protoc
      exit 0
    fi
  fi
fi

PROTOC_RELEASE_VERSION="$(normalize_version_for_protoc_release "$PROTOBUF_BASELINE_VERSION")"
PROTOC_ARCHIVE="protoc-${PROTOC_RELEASE_VERSION}-${PROTOC_OS}-${PROTOC_ARCH}.zip"
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_RELEASE_VERSION}/${PROTOC_ARCHIVE}"
PROTOC_PREFIX="${PROTOC_INSTALL_ROOT}/protoc-${PROTOC_RELEASE_VERSION}-${PROTOC_OS}-${PROTOC_ARCH}"
PROTOC_BIN="${PROTOC_PREFIX}/bin/protoc"

if [[ "${PROTOC_FORCE_INSTALL}" != "1" && -x "${PROTOC_BIN}" ]]; then
  if [[ "${PROTOC_PRINT_PATH_ONLY}" == "1" ]]; then
    echo "${PROTOC_BIN}"
  else
    echo "${PROTOC_BIN}"
  fi
  exit 0
fi

mkdir -p "${PROTOC_INSTALL_ROOT}"
tmpdir="$(mktemp -d "${PROTOC_INSTALL_ROOT}/.protoc-download.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

archive_path="${tmpdir}/${PROTOC_ARCHIVE}"
download_file "${PROTOC_URL}" "${archive_path}"

extract_dir="${tmpdir}/extract"
mkdir -p "${extract_dir}"
unzip -q "${archive_path}" -d "${extract_dir}"

rm -rf "${PROTOC_PREFIX}"
mkdir -p "${PROTOC_PREFIX}"
cp -a "${extract_dir}/." "${PROTOC_PREFIX}/"
chmod +x "${PROTOC_BIN}"

if [[ "${PROTOC_PRINT_PATH_ONLY}" == "1" ]]; then
  echo "${PROTOC_BIN}"
else
  echo "${PROTOC_BIN}"
fi
