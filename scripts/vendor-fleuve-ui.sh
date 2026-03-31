#!/usr/bin/env bash
# Copy Python Fleuve Vite build output into pkg/uiembed/dist for go:embed.
# Usage:
#   ./scripts/vendor-fleuve-ui.sh [/path/to/fleuve/ui/frontend_dist]
# Or set FLEUVE_PYTHON_UI_DIST to that directory.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-${FLEUVE_PYTHON_UI_DIST:-}}"
if [[ -z "$SRC" || ! -d "$SRC" ]]; then
  echo "usage: FLEUVE_PYTHON_UI_DIST=/path/to/frontend_dist $0" >&2
  echo "   or: $0 /path/to/fleuve/ui/frontend_dist" >&2
  exit 1
fi
DEST="$ROOT/pkg/uiembed/dist"
rm -rf "$DEST"
mkdir -p "$DEST"
rsync -a --delete --exclude='.gitkeep' "$SRC"/ "$DEST"/
echo "Vendored UI -> $DEST ($(find "$DEST" -type f | wc -l | tr -d ' ') files)"
echo "Rebuild: go build -o /dev/null ./cmd/ui"
