"""CSV (.csv) format handler for heal-non-dd ingest."""
import csv
from pathlib import Path

from formats import ExtractResult, rows_to_variables


def _read_rows(asset_path: Path) -> list[list[str]]:
    rows = []
    with asset_path.open(newline="", encoding="utf-8-sig", errors="replace") as f:
        for row in csv.reader(f):
            stripped = [cell.strip() for cell in row]
            if any(stripped):
                rows.append(stripped)
    return rows


def extract(asset_path: Path, section_id: str, study_id: str) -> ExtractResult:
    rows = _read_rows(asset_path)
    if not rows:
        return ExtractResult()
    headers = rows[0]
    variables = rows_to_variables(headers, rows[1:], section_id)
    return ExtractResult(variables=variables)
