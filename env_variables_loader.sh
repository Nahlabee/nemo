#!/usr/bin/env bash
# env_variables_loader.sh — usage: source env_variables_loader.sh /path/to/project
PROJ_DIR="${1:-$(pwd)}"
python3 - <<PY
import sys
sys.path.insert(0, "$PROJ_DIR")
import config
config.print_paths()
PY
