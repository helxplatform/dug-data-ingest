#
# Dockerfile for dug-data-ingest
#
# For simplicity, we create a single Docker image for this entire repository. This has three advantages:
# 1. We don't need to set up individual GitHub Actions to publish each ingest script.
# 2. We can write common code that is shared between ingest scripts.
# 3. We can upgrade common requirements across all the tools at once.
#
# The main downside is that this Docker image will be larger than it needs to be for just a single ingest tool,
# but since I expect these scripts to be pretty small, I'm not too concerned yet.
#
# Another option would be to extend/incorporate Dug's own Docker image
# (https://github.com/helxplatform/dug/blob/develop/Dockerfile), but I don't think we want to tie these two components
# that closely together unless it is unavoidable.
#

FROM python:3.12-alpine

# uv, for fast dependency installs (also used to build/manage this image's venv).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Update packages
RUN apk update

RUN apk add git
RUN apk add bash

# Needed to sync with LakeFS.
RUN apk add rclone

# Needed to send commands to LakeFS.
RUN apk add curl

# Needed to install tee to save logs to a file.
RUN apk add coreutils

# Create a non-root user.
ENV USER=dug-ingest
ENV HOME=/home/$USER
ENV UID=1000

RUN adduser -D --home $HOME --uid $UID $USER

USER $USER
WORKDIR $HOME

# Create a venv and put it first on PATH, so `python3`/scripts pick it up with no
# activation step needed. uv detects and installs into this venv via $VIRTUAL_ENV.
ENV VIRTUAL_ENV=$HOME/venv
ENV PATH=$VIRTUAL_ENV/bin:$PATH
RUN uv venv $VIRTUAL_ENV

# Copy over the requirements file and install it as the local user.
COPY --chown=$USER requirements.txt .
RUN uv pip install -r requirements.txt

# Copy over the scripts, then install each script's own requirements.txt (if any) on
# top of the shared ones above.
RUN mkdir scripts
COPY --chown=$USER scripts/ scripts/
RUN find scripts -mindepth 2 -maxdepth 2 -name requirements.txt -print0 \
    | xargs -0 -n1 uv pip install -r
WORKDIR $HOME/scripts

# Note that data should be kept at /data
VOLUME /data
