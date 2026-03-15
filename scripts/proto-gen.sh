#!/usr/bin/env bash
# Генерация Go кода из proto-файлов.
# Требует: protoc, protoc-gen-go, protoc-gen-go-grpc
# Установка: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#            go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/api/proto/gen"
mkdir -p "$OUT"

protoc \
  --proto_path="$ROOT/api/proto" \
  --go_out="$OUT" --go_opt=paths=source_relative \
  --go-grpc_out="$OUT" --go-grpc_opt=paths=source_relative \
  orchestrator.proto nodeagent.proto

echo "==> Proto generated in $OUT/"
