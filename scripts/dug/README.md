# Dug scripts

## reports/

Files in the `reports/` directory will be excluded from Git (via `.gitignore`) unless explicitly added.

## get_dug_data_dictionaries.sh

A script for downloading a list of studies in a Dug instance. This requires [jq] in order to generate
a TSV file from the downloaded JSON file. If no `OUTPUT_DIR` is specified, this script will create a
`reports/` subdirectory in this directory containing two files: `list.json`, the list downloaded from
Dug, and `list.tsv`, a TSV file listing all the studies.

```shell
$ OUTPUT_DIR=reports/hss/ bash get_dug_data_dictionaries.sh
$ OUTPUT_DIR=reports/hss-dev DUG_INSTANCE=https://heal-dev.apps.renci.org bash get_dug_data_dictionaries.sh
```

[jq]: https://jqlang.org/