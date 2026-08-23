#!/usr/bin/env bash

set -Eeuo pipefail

readonly REPOSITORY_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly DOCS_PORT="${MINTLIFY_PORT:-3777}"

if ! command -v mint >/dev/null 2>&1; then
	echo "error: Mintlify CLI not found; install it with 'npm install -g mint'" >&2
	exit 127
fi

if [[ ! "${DOCS_PORT}" =~ ^[0-9]+$ ]] ||
	((DOCS_PORT < 1 || DOCS_PORT > 65535)); then
	echo "error: MINTLIFY_PORT must be an integer between 1 and 65535" >&2
	exit 2
fi

cd "${REPOSITORY_ROOT}"

exec mint dev --port "${DOCS_PORT}" "$@"
