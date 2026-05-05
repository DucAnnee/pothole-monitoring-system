#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

rm -f "$SCRIPT_DIR"/images/* "$SCRIPT_DIR"/metadata/*
