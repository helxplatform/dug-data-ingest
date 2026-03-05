"""Excel (.xlsx) format handler for heal-non-dd ingest."""
from pathlib import Path

import openpyxl

from formats import ExtractResult


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    wb = openpyxl.load_workbook(str(asset_path), data_only=True)
    sections = []
    variables = []

    for sheet in wb.worksheets:
        sheet_section_id = f"{section_id}/{sheet.title}"
        sheet_variable_ids = []

        for row in sheet.iter_rows():
            for cell in row:
                if cell.value is None:
                    continue
                if isinstance(cell.value, (int, float)):
                    continue
                text = str(cell.value).strip()
                if not text:
                    continue
                var_id = f"{sheet_section_id}/{cell.column_letter}{cell.row}"
                variables.append({
                    "id": var_id,
                    "name": text,
                    "description": text,
                    "type": "variable",
                    "data_type": "text",
                    "parents": [sheet_section_id],
                    "parent_type": "section",
                })
                sheet_variable_ids.append(var_id)

        sections.append({
            "id": sheet_section_id,
            "name": sheet.title,
            "description": sheet.title,
            "type": "section",
            "parents": [study_id],
            "parent_type": "study",
            "variable_list": sheet_variable_ids,
        })

    return ExtractResult(sections=sections, variables=variables, replace_file_section=True)
