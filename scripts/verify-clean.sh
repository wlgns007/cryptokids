#!/usr/bin/env bash
set -euo pipefail
echo "Scanning for legacy endpoints..."

search_paths=()
for dir in client server; do
  if [ -d "$dir" ]; then
    search_paths+=("$dir")
  fi
done

if [ ${#search_paths[@]} -eq 0 ]; then
  echo "No client or server directories found to scan."
  exit 0
fi

if grep -R "/api/families" "${search_paths[@]}" | grep -v "/api/admin" >/dev/null; then
  echo "Found legacy /api/families call(s). Fix before merge."
  exit 1
fi

if grep -R "/api/whoami" "${search_paths[@]}" >/dev/null; then
  echo "Found legacy /api/whoami call(s). Use /api/admin/whoami."
  exit 1
fi

echo "OK"
