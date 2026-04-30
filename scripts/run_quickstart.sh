#!/bin/bash
set -e

TEMPLATE="${1:?Usage: $0 <template>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_DIR="$PROJECT_ROOT/scratchpad/quickstarts/$TEMPLATE"

pwd
rm -rf "$STACK_DIR"
mkdir -p "$STACK_DIR"
cd "$STACK_DIR"

laktory quickstart -t "$TEMPLATE"

python "$PROJECT_ROOT/.github/scripts/update_quickstart_stack.py" main "$TEMPLATE" --stack_root "$STACK_DIR"

ENV_FILE="secrets/.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
fi

laktory init --options -reconfigure

