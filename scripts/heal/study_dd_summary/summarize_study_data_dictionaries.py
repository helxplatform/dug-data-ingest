#!/usr/bin/env python3
"""
Summarize how rich each study's data dictionary is in a directory of HEAL study files.

The input is a directory of `<HDPID>.dug.json` files in the `dug_data_model` v2 format, which is
what the HEAL MDS study ingest writes (and publishes to lakeFS, e.g. `lakefs://heal-mds-studies/`).
Each file is a JSON list holding one `study` element plus zero or more `variable` elements.

The output is a CSV with one row per study: how many variables it has, how many of those have a
description, a non-string data type, permissible values, other constraints or a CDE mapping, and
which CDE sections the study is registered against. See COLUMNS below for the full list.

This script only reads local files and uses nothing beyond the Python standard library, so it can
be copied out of this repository and run on its own. It doesn't import anything from the ingest
code; the field names it reads are listed in the functions below.

Reading the output: a study with `num_variables` = 0 but a non-zero `num_cde_sections` registered
CDEs on the HEAL Platform MDS but submitted no data dictionary of its own (its MDS record has an
empty `variable_level_metadata.data_dictionaries`). A study with zero of both has neither.

Examples:

    # Five specific studies, written to a CSV file.
    python3 summarize_study_data_dictionaries.py data/heal-mds-studies \\
        --study HDP00937 --study HDP00951 --output summary.csv

    # Every study in a research program/network (case-insensitive substring match).
    python3 summarize_study_data_dictionaries.py data/heal-mds-studies --program RE-JOIN

    # Every study in the directory, to stdout.
    python3 summarize_study_data_dictionaries.py data/heal-mds-studies
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

FILE_SUFFIX = '.dug.json'

# Tag categories whose values are matched by --program, alongside the element's `programs` list.
PROGRAM_TAG_CATEGORIES = ('Research Program', 'Research Network')

# Metadata keys that are counted by their own column, so they don't count as "other constraints".
PERMISSIBLE_VALUE_KEYS = ('permissible_values', 'enum')
CDE_MAPPING_KEY = 'cde_mapping'

# Separator used when a list is written into a single CSV cell.
LIST_SEPARATOR = '; '

COLUMNS = [
    'hdp_id',
    'study_name',
    'research_programs',
    'file_present',
    'file_mtime',
    'data_availability',
    'repositories',
    'data_package_links',
    'num_variables',
    'num_variable_sections',
    'vars_with_description',
    'vars_with_typed_data',
    'vars_with_permissible_values',
    'vars_with_constraints',
    'vars_with_cde_mapping',
    'num_cde_sections',
    'cde_sections',
]


def _unique(values):
    """Return the non-empty values in their original order, without duplicates."""
    seen = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
    return seen


def _tag_values(element, category):
    """Return the values of all tags in a category, splitting comma-joined tag values."""
    values = []
    for tag in element.get('tags') or []:
        if tag.get('category') == category and tag.get('value'):
            values.extend(v.strip() for v in str(tag['value']).split(','))
    return _unique(values)


def study_programs(study):
    """Return the research programs and networks a study element belongs to."""
    # Program names can contain commas, so tag values here aren't split the way _tag_values does.
    tag_values = [
        tag.get('value') for tag in study.get('tags') or []
        if tag.get('category') in PROGRAM_TAG_CATEGORIES
    ]
    return _unique(list(study.get('programs') or []) + tag_values)


def summarize_variables(variables):
    """Count how many variable elements carry each kind of data-dictionary information."""
    counts = {
        'num_variables': len(variables),
        'vars_with_description': 0,
        'vars_with_typed_data': 0,
        'vars_with_permissible_values': 0,
        'vars_with_constraints': 0,
        'vars_with_cde_mapping': 0,
    }
    sections = set()
    for variable in variables:
        metadata = variable.get('metadata') or {}
        # parents is [study_id, section] when the data dictionary names a section.
        parents = variable.get('parents') or []
        if len(parents) > 1 and parents[1]:
            sections.add(parents[1])
        if (variable.get('description') or '').strip():
            counts['vars_with_description'] += 1
        if (variable.get('data_type') or 'string') != 'string':
            counts['vars_with_typed_data'] += 1
        if any(metadata.get(key) for key in PERMISSIBLE_VALUE_KEYS):
            counts['vars_with_permissible_values'] += 1
        other_keys = set(metadata) - set(PERMISSIBLE_VALUE_KEYS) - {CDE_MAPPING_KEY}
        if any(metadata[key] not in (None, '', [], {}) for key in other_keys):
            counts['vars_with_constraints'] += 1
        if metadata.get(CDE_MAPPING_KEY):
            counts['vars_with_cde_mapping'] += 1
    counts['num_variable_sections'] = len(sections)
    return counts


def missing_study_row(hdp_id):
    """Return the row for a requested study that has no file in the input directory."""
    row = {column: '' for column in COLUMNS}
    row['hdp_id'] = hdp_id
    row['file_present'] = 'false'
    return row


def summarize_study_file(path):
    """
    Read one `<HDPID>.dug.json` file and return its summary row.

    :param path: Path to the file.
    :return: A dict keyed by the names in COLUMNS.
    """
    path = Path(path)
    with path.open() as f:
        elements = json.load(f)

    studies = [e for e in elements if e.get('type') == 'study']
    variables = [e for e in elements if e.get('type') == 'variable']
    study = studies[0] if studies else {}
    if len(studies) > 1:
        print(f'Warning: {path} has {len(studies)} study elements; summarizing the first.',
              file=sys.stderr)
    metadata = study.get('metadata') or {}

    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    cde_sections = list(study.get('section_list') or [])
    row = {
        'hdp_id': study.get('id') or path.name.removesuffix(FILE_SUFFIX),
        'study_name': study.get('name', ''),
        'research_programs': LIST_SEPARATOR.join(study_programs(study)),
        'file_present': 'true',
        'file_mtime': mtime.isoformat(timespec='seconds'),
        'data_availability': metadata.get('Data Availability', ''),
        'repositories': LIST_SEPARATOR.join(_tag_values(study, 'Repositories')),
        'data_package_links': LIST_SEPARATOR.join(metadata.get('Data Package Links') or []),
        'num_cde_sections': len(cde_sections),
        'cde_sections': LIST_SEPARATOR.join(cde_sections),
    }
    row.update(summarize_variables(variables))
    return row


def summarize_directory(input_dir, study_ids=(), program=None):
    """
    Summarize the studies in a directory of `.dug.json` files.

    With neither `study_ids` nor `program`, every file is summarized. Otherwise the result is
    the union of the listed studies and the studies whose programs contain `program`
    (case-insensitive). A listed study with no file still gets a row, with file_present=false.

    :param input_dir: Directory holding `<HDPID>.dug.json` files.
    :param study_ids: HDP IDs to include.
    :param program: Substring to match against each study's research programs/networks.
    :return: A list of row dicts, sorted by hdp_id.
    """
    input_dir = Path(input_dir)
    if not input_dir.is_dir():
        raise NotADirectoryError(f'{input_dir} is not a directory')

    study_ids = _unique(study_ids)
    rows = {}
    if program or not study_ids:
        # Every file has to be read to match on program.
        for path in sorted(input_dir.glob('*' + FILE_SUFFIX)):
            row = summarize_study_file(path)
            in_program = program and program.lower() in row['research_programs'].lower()
            if not (study_ids or program) or in_program or row['hdp_id'] in study_ids:
                rows[row['hdp_id']] = row
    for hdp_id in study_ids:
        if hdp_id in rows:
            continue
        path = input_dir / (hdp_id + FILE_SUFFIX)
        rows[hdp_id] = summarize_study_file(path) if path.exists() else missing_study_row(hdp_id)
    return [rows[key] for key in sorted(rows)]


def write_csv(rows, output):
    """Write summary rows as CSV to an open text file."""
    writer = csv.DictWriter(output, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Summarize how rich each study\'s data dictionary is in a directory of '
                    'HEAL <HDPID>.dug.json files, as a CSV with one row per study.')
    parser.add_argument('input_dir', type=Path,
                        help='Directory of <HDPID>.dug.json files (dug_data_model v2 format).')
    parser.add_argument('--study', '-s', action='append', default=[], metavar='HDPID',
                        help='Include this study (repeatable).')
    parser.add_argument('--program', '-p', metavar='SUBSTRING',
                        help='Include every study whose research program or network contains '
                             'this text, case-insensitively (e.g. RE-JOIN).')
    parser.add_argument('--output', '-o', type=Path,
                        help='CSV file to write (parent directories are created). '
                             'Defaults to stdout.')
    args = parser.parse_args(argv)

    rows = summarize_directory(args.input_dir, args.study, args.program)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('w', newline='') as f:
            write_csv(rows, f)
        print(f'Wrote {len(rows)} studies to {args.output}', file=sys.stderr)
    else:
        write_csv(rows, sys.stdout)
    return 0


if __name__ == '__main__':
    sys.exit(main())
