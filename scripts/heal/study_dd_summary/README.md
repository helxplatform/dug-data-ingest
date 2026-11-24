# HEAL study data-dictionary summary

`summarize_study_data_dictionaries.py` reports how rich each HEAL study's data dictionary is,
and which files the study lists, as a CSV with one row per study. It reads a directory of `<HDPID>.dug.json` files in the
`dug_data_model` v2 format, which is what the HEAL MDS study ingest writes to
`lakefs://heal-mds-studies/`. Sync that repository locally first (e.g. into `data/heal-mds-studies/`).

This directory is self-contained. The script uses only the Python standard library, reads only
local files and imports nothing else from this repository, so it can be moved to another
repository as is.

## Usage

```bash
# Specific studies
python3 summarize_study_data_dictionaries.py data/heal-mds-studies \
    --study HDP00937 --study HDP00951 --output summary.csv

# Every study in a research program or network (case-insensitive substring match)
python3 summarize_study_data_dictionaries.py data/heal-mds-studies --program RE-JOIN

# Every study, to stdout
python3 summarize_study_data_dictionaries.py data/heal-mds-studies

# Also list every file in the selected studies, one row per file
python3 summarize_study_data_dictionaries.py data/heal-mds-studies --program RE-JOIN \
    --output studies.csv --files-output files.csv
```

`--study` and `--program` can be combined: the result is the union of the two. A study passed to
`--study` that has no file in the directory still gets a row, with `file_present=false`. A full run
over about 1,200 studies takes well under a second.

## Columns

| column | meaning |
|---|---|
| `hdp_id`, `study_name` | from the study element |
| `research_programs` | the study's `programs` plus its Research Program/Network tags, `; `-joined |
| `file_present`, `file_mtime` | whether the file exists, and when it was last written (UTC), which shows how stale a local sync is |
| `data_availability`, `data_package_links` | from the study's `metadata` |
| `repositories` | from the study's `Repositories` tag, de-duplicated |
| `num_variables` | number of `variable` elements in the file |
| `num_variable_sections` | distinct data-dictionary sections (the variable's second `parents` entry) |
| `vars_with_description` | variables with a non-blank description |
| `vars_with_typed_data` | variables whose `data_type` isn't `string` |
| `vars_with_permissible_values` | variables with `metadata.permissible_values` or `metadata.enum` |
| `vars_with_constraints` | variables with any other `metadata` key (`minimum`, `maximum`, `pattern`, …) |
| `vars_with_cde_mapping` | variables mapped to a CDE (`metadata.cde_mapping`) |
| `num_cde_sections`, `cde_sections` | the study's `section_list`: CDE sections it is registered against |
| `file_manifest_present` | whether `file_manifests/<HDPID>.json` exists next to the study files |
| `num_files`, `total_file_size_bytes`, `file_types`, `documentation_files` | the file summary the ingest puts in the study's `metadata` (`File Count`, `Total File Size (bytes)`, `File Types`, `Documentation Files`) |

The `--files-output` CSV has one row per file, from `file_manifests/<HDPID>.json`: `hdp_id`,
`file_name`, `file_size`, `dataset_name`, `repository_name`, `repository_study_link`,
`object_id` and `sha256`. These are copied from the HEAL Platform MDS (`gen3_discovery.__manifest`),
and not every repository fills in all of them.

The file columns are blank for output written before the ingest started producing file
manifests.

## Reading the output

- `num_variables` = 0 with `num_cde_sections` > 0: the study registered CDEs on the HEAL Platform
  MDS but submitted no data dictionary of its own. Its MDS record
  (`https://healdata.org/mds/metadata/<HDPID>`) has an empty
  `variable_level_metadata.data_dictionaries`.
- Both 0: the MDS has neither a data dictionary nor CDE registrations for the study.
- Before blaming the ingest for a missing dictionary, check the MDS record. As of November 2026,
  the five RE-JOIN studies above had no data dictionaries on the MDS either.

## Tests

```bash
python3 -m unittest test_summarize_study_data_dictionaries.py
```
