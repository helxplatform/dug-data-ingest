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

FROM python:3-alpine

# Update packages
RUN apk update

RUN apk add git
RUN apk add bash

# Needed to sync with LakeFS.
RUN apk add rclone

# Update Python
RUN pip install --upgrade pip

# Create a non-root user.
ENV USER dug-ingest
ENV HOME /home/$USER
ENV UID 1000

RUN adduser -D --home $HOME --uid $UID $USER

USER $USER
WORKDIR $HOME

ENV PATH=$HOME/.local/bin:$PATH

# Copy over the requirements file and install it as the local user.
COPY --chown=$USER requirements.txt .
RUN pip install --user -r requirements.txt

# Copy over the scripts.
RUN mkdir scripts
COPY --chown=$USER scripts/ scripts/
WORKDIR $HOME/scripts

# Note that data should be kept at /data
VOLUME /data
