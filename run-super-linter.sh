#!/usr/bin/env bash

# --platform linux/amd64
#   Only needed on Apple Silicon
# -e FIX_*=true
#   Only needed if you want SuperLinter to fix code locally.

docker run \
	--platform linux/amd64 \
	-e RUN_LOCAL=true \
	-e DEFAULT_BRANCH=main \
	-e VALIDATE_ALL_CODEBASE=true \
	-e VALIDATE_GIT_COMMITLINT=false \
	-v .:/tmp/lint \
	--rm \
	ghcr.io/super-linter/super-linter:latest
