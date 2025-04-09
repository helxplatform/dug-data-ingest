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

# Set DONT_FIX=" " if FIX
[ -z "${FIX}" ] && DONT_FIX=" "

# Python linting: Ruff (https://github.com/astral-sh/ruff)
PYTHON_DIR="${BASEDIR:-.}"
ruff check --select I --fix ${PYTHON_DIR}
[ -z "${FIX}" ] && ruff format ${PYTHON_DIR} 
