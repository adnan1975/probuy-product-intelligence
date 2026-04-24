#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-10000}"

exec uvicorn api.main:app --host "$HOST" --port "$PORT"
