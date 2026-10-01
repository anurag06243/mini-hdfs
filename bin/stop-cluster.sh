#!/usr/bin/env bash
root="$(cd "$(dirname "$0")/.." && pwd)"
for f in "$root"/data/logs/*.pid; do
  [ -e "$f" ] || continue
  kill "$(cat "$f")" 2>/dev/null || true
  rm -f "$f"
done
echo "Stopped."
