"""Excel (.xlsx) format handler for heal-non-dd ingest."""
from pathlib import Path

import openpyxl

from formats import ExtractResult, rows_to_variables


def _sheet_rows(sheet) -> list[list[str]]:
    """Return non-empty rows from a sheet as string lists."""
    rows = []
    for row in sheet.iter_rows():
        cells = [str(c.value).strip() if c.value is not None else "" for c in row]
        if any(cells):
            rows.append(cells)
    return rows


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    wb = openpyxl.load_workbook(str(asset_path), data_only=True)
    sections = []
    variables = []

    for sheet in wb.worksheets:
        sheet_section_id = f"{section_id}/{sheet.title}"
        all_rows = _sheet_rows(sheet)
        if all_rows:
            sheet_vars = rows_to_variables(all_rows[0], all_rows[1:], sheet_section_id)
        else:
            sheet_vars = []
        variables.extend(sheet_vars)
        sections.append({
            "id": sheet_section_id,
            "name": sheet.title,
            "description": sheet.title,
            "type": "section",
            "parents": [study_id],
            "parent_type": "study",
            "variable_list": [v["id"] for v in sheet_vars],
        })

    return ExtractResult(sections=sections, variables=variables, replace_file_section=True)
