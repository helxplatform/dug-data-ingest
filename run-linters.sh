#!/usr/bin/env bash
#
# Run all the linters that are installed via PIP.
#
# SYNOPSIS
#		bash linters/run-linters.sh
#
#	Set the FIX environmental variable if you want to fix as much code as possible.
#	Set the BASEDIR environmental variable to use a different directory as the repository base.
#		Defaults to '.'
#

# Python linting: Ruff (https://github.com/astral-sh/ruff)
PYTHON_DIR="${BASEDIR:-.}"
ruff check --select I --fix "${PYTHON_DIR}"
[ -z "${FIX}" ] && ruff format "${PYTHON_DIR}"

# Shell scripts: Shellcheck (https://github.com/koalaman/shellcheck)
# You can fix these files by asking shellcheck to generate a diff (`-f diff`) and then piping that to `patch`.
find "${BASEDIR:-.}" -path "./venv" -prune -o -name '*.sh' -print0 | xargs -0 shellcheck
