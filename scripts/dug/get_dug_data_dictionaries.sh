#!/usr/bin/bash
#
# Download a list of data dictionaries currently ingested in a Dug instance. Downloads the
# [search program information](https://heal.renci.org/search-api/docs#/default/search_program_search_program_get)
# to ${OUTPUT_DIR}/list.json, and then converts it into TSV file at ${OUTPUT_DIR}/list.tsv.
#
# Environmental variables:
# - DUG_INSTANCE="https://heal.renci.org" -- the Dug instance to query, without a final `/`.
# - OUTPUT_DIR="reports/" -- the output directory to write list.json and list.tsv to.
#
# Requires [jq](https://jqlang.github.io/jq/) to convert the downloaded JSON file into a TSV file.

DUG_INSTANCE=${DUG_INSTANCE:-"https://heal.renci.org"}
OUTPUT_DIR=${OUTPUT_DIR:-"$(dirname "$0")/reports"}

mkdir -p "${OUTPUT_DIR}"
wget "$DUG_INSTANCE/search-api/search_program" -O "${OUTPUT_DIR}/list.json"
jq -r '(["collection_id", "collection_name", "collection_action"], (.result | sort_by(.collection_id) | .[] | [.collection_id, .collection_name, .collection_action]) | @tsv)' "${OUTPUT_DIR}/list.json" | grep -v '^\tCDE\t$' > "${OUTPUT_DIR}/list.tsv"
