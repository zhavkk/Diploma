#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/bin"
mkdir -p "$BIN"

echo "==> Building orchestrator..."
go build -o "$BIN/orchestrator" "$ROOT/services/orchestrator/cmd"

echo "==> Building node-agent..."
go build -o "$BIN/node-agent" "$ROOT/services/node-agent/cmd"

echo "==> Building ha-ctl (CLI)..."
go build -o "$BIN/ha-ctl" "$ROOT/services/cli/cmd"

echo "==> Done. Binaries in $BIN/"
ls -lh "$BIN/"
