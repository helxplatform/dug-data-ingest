#!/usr/bin/bash
#
# Download a list of data dictionaries currently ingested in a Dug instance. Downloads the
# [search program information](https://heal.renci.org/search-api/docs#/default/search_program_search_program_get)
# to ${DIR}/list.json, and then converts it into TSV file at ${DIR}/list.tsv.
#
# Requires [jq](https://jqlang.github.io/jq/) to convert the downloaded JSON file into a TSV file.

DUG_INSTANCE=${DUG_INSTANCE:-"https://heal.renci.org"}
DIR=reports/data-dictionaries-from-dug

mkdir -p ${DIR}
wget "$DUG_INSTANCE/search-api/search_program" -O ${DIR}/list.json
jq -r '(["collection_id", "collection_name", "collection_action"], (.result | sort_by(.collection_id) | .[] | [.collection_id, .collection_name, .collection_action]) | @tsv)' ${DIR}/list.json | grep -v '^\tCDE\t$' > ${DIR}/list.tsv
