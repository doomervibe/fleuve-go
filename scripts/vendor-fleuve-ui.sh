#!/usr/bin/env bash
# Sync the Python Fleuve UI static build into this module for go:embed.
# Source (sibling checkout): ../les/fleuve/ui/frontend_dist
# Target: pkg/uiembed/dist/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${FLEUVE_UI_DIST:-$ROOT/../les/fleuve/ui/frontend_dist}"
DST="$ROOT/pkg/uiembed/dist"
if [[ ! -d "$SRC" ]]; then
  echo "error: UI dist not found at $SRC (set FLEUVE_UI_DIST or clone les next to fleuve-go)" >&2
  exit 1
fi
mkdir -p "$DST"
rsync -a --delete "$SRC/" "$DST/"
echo "Vendored UI from $SRC -> $DST"
