"""
Tests for summarize_study_data_dictionaries.py. Standard library only:

    python3 -m unittest test_summarize_study_data_dictionaries.py
"""

import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import summarize_study_data_dictionaries as sdd  # noqa: E402


def _study(hdp_id, programs=(), sections=(), variable_ids=(), repositories=None, file_summary=None):
    tags = [{'category': 'Research Program', 'value': p} for p in programs]
    if repositories:
        tags.append({'category': 'Repositories', 'value': repositories})
    metadata = {'Data Availability': 'available', 'Data Package Links': ['https://doi.org/x']}
    metadata.update(file_summary or {})
    return {
        'id': hdp_id, 'name': f'Study {hdp_id}', 'type': 'study',
        'programs': list(programs), 'tags': tags,
        'metadata': metadata,
        'variable_list': list(variable_ids), 'section_list': list(sections),
    }


def _variable(hdp_id, name, section='', description='', data_type='string', metadata=None):
    return {
        'id': f'{hdp_id}:{name}', 'name': name, 'type': 'variable', 'description': description,
        'parents': [p for p in (hdp_id, section) if p], 'data_type': data_type,
        'metadata': metadata or {},
    }


class SummarizeTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.write('HDP00001', [
            _study('HDP00001', programs=['(RE-JOIN): Restoring Joint Health'],
                   sections=['gad2', 'peg'], repositories='SPARC,SPARC',
                   file_summary={'File Count': 2, 'Total File Size (bytes)': 30,
                                 'File Types': ['ims', 'xlsx'],
                                 'Documentation Files': ['files/dataset_description.xlsx']}),
        ])
        manifest_dir = self.dir / sdd.FILE_MANIFEST_DIR
        manifest_dir.mkdir()
        (manifest_dir / 'HDP00001.json').write_text(json.dumps({'id': 'HDP00001', 'files': [
            {'file_name': 'files/dataset_description.xlsx', 'file_size': 10, 'object_id': 'o1',
             'repository_name': 'SPARC'},
            {'file_name': 'files/primary/a.ims', 'file_size': 20, 'object_id': 'o2'},
        ]}))
        self.write('HDP00002', [
            _study('HDP00002', programs=['(ACT NOW): Neonatal'], variable_ids=['a', 'b', 'c']),
            _variable('HDP00002', 'a', section='form1', description='A',
                      metadata={'enum': ['1', '2'], 'permissible_values': ['Yes', 'No']}),
            _variable('HDP00002', 'b', section='form2', description='  ', data_type='number',
                      metadata={'minimum': 0, 'maximum': 10}),
            _variable('HDP00002', 'c', description='C',
                      metadata={'cde_mapping': {'section': 'peg'}}),
        ])

    def tearDown(self):
        self._tmp.cleanup()

    def write(self, hdp_id, elements):
        (self.dir / f'{hdp_id}.dug.json').write_text(json.dumps(elements))

    def test_study_without_variables(self):
        row = sdd.summarize_study_file(self.dir / 'HDP00001.dug.json')
        self.assertEqual(row['num_variables'], 0)
        self.assertEqual(row['num_cde_sections'], 2)
        self.assertEqual(row['cde_sections'], 'gad2; peg')
        self.assertEqual(row['repositories'], 'SPARC')
        self.assertEqual(row['research_programs'], '(RE-JOIN): Restoring Joint Health')

    def test_variable_counts(self):
        row = sdd.summarize_study_file(self.dir / 'HDP00002.dug.json')
        self.assertEqual(row['num_variables'], 3)
        self.assertEqual(row['num_variable_sections'], 2)
        self.assertEqual(row['vars_with_description'], 2)
        self.assertEqual(row['vars_with_typed_data'], 1)
        self.assertEqual(row['vars_with_permissible_values'], 1)
        self.assertEqual(row['vars_with_constraints'], 1)
        self.assertEqual(row['vars_with_cde_mapping'], 1)

    def test_file_summary_columns(self):
        row = sdd.summarize_study_file(self.dir / 'HDP00001.dug.json')
        self.assertEqual(row['file_manifest_present'], 'true')
        self.assertEqual(row['num_files'], 2)
        self.assertEqual(row['total_file_size_bytes'], 30)
        self.assertEqual(row['file_types'], 'ims; xlsx')
        self.assertEqual(row['documentation_files'], 'files/dataset_description.xlsx')

    def test_study_without_file_manifest(self):
        row = sdd.summarize_study_file(self.dir / 'HDP00002.dug.json')
        self.assertEqual(row['file_manifest_present'], 'false')
        self.assertEqual(row['num_files'], '')

    def test_file_rows(self):
        rows = sdd.file_rows(self.dir, ['HDP00001', 'HDP00002', 'HDP99999'])
        self.assertEqual([r['file_name'] for r in rows],
                         ['files/dataset_description.xlsx', 'files/primary/a.ims'])
        self.assertEqual(rows[0]['hdp_id'], 'HDP00001')
        self.assertEqual(rows[0]['repository_name'], 'SPARC')
        self.assertEqual(rows[1]['repository_name'], '')

    def test_main_writes_files_output(self):
        files_output = self.dir / 'files.csv'
        sdd.main([str(self.dir), '-p', 'RE-JOIN', '-o', str(self.dir / 's.csv'),
                  '--files-output', str(files_output)])
        rows = list(csv.DictReader(files_output.open()))
        self.assertEqual(len(rows), 2)
        self.assertEqual(list(rows[0]), sdd.FILE_COLUMNS)

    def test_no_filter_returns_all(self):
        ids = [r['hdp_id'] for r in sdd.summarize_directory(self.dir)]
        self.assertEqual(ids, ['HDP00001', 'HDP00002'])

    def test_program_filter_is_case_insensitive(self):
        ids = [r['hdp_id'] for r in sdd.summarize_directory(self.dir, program='re-join')]
        self.assertEqual(ids, ['HDP00001'])

    def test_study_and_program_filters_are_unioned(self):
        rows = sdd.summarize_directory(self.dir, study_ids=['HDP00002'], program='RE-JOIN')
        self.assertEqual([r['hdp_id'] for r in rows], ['HDP00001', 'HDP00002'])

    def test_missing_study_gets_a_row(self):
        rows = sdd.summarize_directory(self.dir, study_ids=['HDP99999', 'HDP00001'])
        self.assertEqual([r['hdp_id'] for r in rows], ['HDP00001', 'HDP99999'])
        self.assertEqual(rows[1]['file_present'], 'false')
        self.assertEqual(rows[1]['num_variables'], '')

    def test_csv_has_all_columns(self):
        out = io.StringIO()
        sdd.write_csv(sdd.summarize_directory(self.dir), out)
        reader = csv.DictReader(io.StringIO(out.getvalue()))
        self.assertEqual(reader.fieldnames, sdd.COLUMNS)
        self.assertEqual(len(list(reader)), 2)

    def test_main_writes_output_file(self):
        output = self.dir / 'nested' / 'out.csv'
        self.assertEqual(sdd.main([str(self.dir), '-s', 'HDP00001', '-o', str(output)]), 0)
        self.assertIn('HDP00001', output.read_text())


if __name__ == '__main__':
    unittest.main()
