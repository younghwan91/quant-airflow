#!/usr/bin/env bash
set -e

# Collectors are self-contained (./collectors, image deps only) — no editable
# install of swing-it(구 kr-quant) needed anymore. swing-it is still mounted read-only at
# /opt/swing-it for the 1 DAG that intentionally runs its analysis code
# in-place (weekly_price_adjust.py's swing_it.price_adjust, via
# PYTHONPATH/sys.path, not a package install).

exec /entrypoint "$@"
