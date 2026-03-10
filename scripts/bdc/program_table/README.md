# BDC Program Table Ingestion Pipeline

A modular data ingestion pipeline for BioData Catalyst (BDC) program table used in the program search/filter feature on the BDC portal.

## Overview

This pipeline fetches study data from BDC and Gen3 portals, merges them, updates program names using NHLBI Jira data, and processes the final program table for upload. The output is used to populate the program search table on the BDC portal.

## Pipeline Steps

| Step | Script | Description |
|------|--------|-------------|
| 1 | `fetch_studies.py` | Fetch studies from BDC and Gen3 portals |
| 2 | `update_programs.py` | Update program names from Jira data |
| 3 | `process_program_table.py` | Post-process: set defaults and expand by community |

## Directory Structure

```
program_table/
├── bdc_utils.py                # Shared utilities (logging, JSON I/O, validation)
├── bdc_data_manager.py         # Core data management (BDC/Gen3 API interactions)
├── fetch_studies.py            # Step 1: Fetch and merge studies
├── update_programs.py          # Step 2: Update program names from Jira
├── process_program_table.py    # Step 3: Post-process program table
├── run_pipeline.sh             # Bash orchestration script
└── README.md                   # This file
```

## Quick Start

### Run Full Pipeline (Steps 1-2)

```bash
./run_pipeline.sh \
  --output-dir /path/to/output \
  --jira-file /path/to/jira.csv
```

### Run Post-Processing (Step 3)

```bash
python3 process_program_table.py \
  --input-file /path/to/program_table_updated.json \
  --output-dir /path/to/output
```

## Data Flow

```
┌─────────────────┐          ┌──────────────┐
│   BDC Portal    │          │ Gen3 Portal  │
│   (API Fetch)   │          │  (API Fetch) │
└────────┬────────┘          └──────┬───────┘
         │                          │
         └────────────┬─────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Step 1: Merge Studies │
         │  (fetch_studies.py)    │
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────┐     ┌──────────────┐
         │  Step 2: Update        │◄────│  Jira Data   │
         │  Programs              │     │  (CSV/Excel) │
         │  (update_programs.py)  │     └──────────────┘
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────────────────────────┐
         │  Step 3: Post-Process Program Table        │
         │  (process_program_table.py)                │
         │                                            │
         │  1. Set "Extramural Research" for studies  │
         │     with missing descriptions              │
         │  2. Replace with Community values          │
         │  3. Create duplicates for multi-community  │
         │     studies                                │
         └───────────┬────────────────────────────────┘
                     │
                     ▼
         ┌────────────────────────┐
         │  Final Program Table   │
         │  (Ready for Upload)    │
         └────────────────────────┘
```

---

## Step 1: Fetch Studies

Fetches studies from BDC portal and Gen3 portal, merges them, and prepares the program table.

```bash
python3 fetch_studies.py \
  --output-dir /path/to/output \
  [--bdc-url URL] \
  [--gen3-url URL] \
  [--gen3-limit N] \
  [--show-details]
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--output-dir` | Output directory | Required |
| `--bdc-url` | BDC API URL | Production |
| `--gen3-url` | Gen3 API URL | Production |
| `--gen3-limit` | Pagination limit | 50 |
| `--show-details` | Show detailed logs | False |

**Output:** `program_table/program_table.json`

---

## Step 2: Update Program Names

Updates program names based on NHLBI Jira data.

```bash
python3 update_programs.py \
  --jira-file /path/to/jira.csv \
  --input-file /path/to/program_table.json \
  --output-dir /path/to/output
```

**Options:**
| Option | Description |
|--------|-------------|
| `--jira-file` | Path to Jira data file (CSV or Excel) |
| `--input-file` | Path to input program table JSON |
| `--output-dir` | Output directory |

**Output:**
- `program_table_updated_TIMESTAMP.json` (readable)
- `program_table_updated_TIMESTAMP_minified.json` (for upload)

---

## Step 3: Post-Process Program Table

Processes the program table to set default programs for studies with missing descriptions and expands the table by community mapping.

```bash
python3 process_program_table.py \
  --input-file /path/to/program_table_updated.json \
  --output-dir /path/to/output
```

**Options:**
| Option | Description | Default |
|--------|-------------|---------|
| `--input-file` | Input program table JSON | Required |
| `--output-dir` | Output directory | Script directory |

### What It Does

**Phase 1: Set Default Program**
- Studies with empty descriptions get:
  - Program = "Extramural Research"
  - Description = "Various HLBS"
- Excludes: `TOPMed_Common_Exchange_Area` studies, studies with no valid name

**Phase 2: Expand by Community**
- Replaces "Extramural Research" with Community value (when available)
- Creates duplicate records for studies belonging to multiple communities
- Updates Description based on Program-to-Description mapping

### Program to Description Mapping

| Program | Description |
|---------|-------------|
| Bench to Bassinet | Pediatric Cardiovascular |
| BioLINCC | Various HLBS |
| C4R | COVID-19 & Various HLBS |
| CONNECTS | COVID-19 |
| Extramural Research | Various HLBS |
| Heartshare | Cardiovascular |
| Longitudinal Epidemiology Observational Study (LEOS) | Longitudinal Observational |
| LungMAP | Pulmonary |
| NHLBI Intramural Research | Various HLBS |
| National Sleep Research Resource (NSRR) | Sleep and Circadian Rhythms |
| PETAL Network | Pulmonary & COVID-19 |
| Pediatric Heart Network (PHN) | Pediatric Cardiovascular |
| RECOVER | Long COVID & PASC |
| Sickle Cell Disease | Sickle Cell Disease |
| TOPMed | Precision Medicine |
| Training | Open-Access; Training |
| *(other)* | Various HLBS *(default)* |

### Output Files

| File | Description |
|------|-------------|
| `program_table_final_TIMESTAMP.json` | Formatted JSON (readable) |
| `program_table_final_TIMESTAMP.min.json` | Minified JSON (for upload) |
| `program_table_processing_TIMESTAMP.log` | Detailed processing log |
| `program_table_report_TIMESTAMP.txt` | Comprehensive report |

### Input Format

JSON array of study records:

```json
[
  {
    "Accession": "phs000123.v1.p1",
    "Study Name": "Example Study",
    "Program": "",
    "Description": "",
    "Community": "TOPMed",
    "Community1": "CONNECTS",
    "Community2": ""
  }
]
```

---

## Output Directory Structure

After running the full pipeline:

```
output_directory/
├── program_table/
│   ├── program_table.json                    # Step 1 output
│   ├── program_table_updated_*.json          # Step 2 output (readable)
│   ├── program_table_updated_*_minified.json # Step 2 output (minified)
│   ├── program_table_final_*.json            # Step 3 output (readable)
│   └── program_table_final_*.min.json        # Step 3 output (FINAL for upload)
│
├── studies_on_bdc_portal/
│   └── bdc_studies.json
│
└── studies_on_gen3_portal/
    ├── raw_studies_on_gen3.json
    ├── missing_studies_comparing_gen3_and_bdc.log
    ├── studies_updated_*.json
    ├── program_names_updated_report_*.json
    ├── studies_missing_description_*.json
    └── jira_studies_not_in_gen3_report_*.log
```

---

## Module Documentation

### bdc_utils.py

Shared utilities for logging, JSON I/O, and validation.

| Function | Description |
|----------|-------------|
| `setup_logging()` | Configure logging with file and console handlers |
| `load_json()` / `save_json()` | JSON file I/O with minify option |
| `extract_base_accession()` | Extract base accession from full ID |
| `validate_study()` | Validate study has required fields |
| `generate_timestamp()` | Generate timestamp for file naming |

### bdc_data_manager.py

Core data management for API interactions.

| Class | Description |
|-------|-------------|
| `BDCDataManager` | Manages BDC API interactions |
| `Gen3DataManager` | Manages Gen3 API interactions |
| `StudyMerger` | Merges and compares study data |

---

## Examples

### Full Pipeline Run

```bash
# Step 1-2: Fetch and update
./run_pipeline.sh \
  --output-dir /data/bdc_$(date +%Y%m) \
  --jira-file /data/jira/latest.csv

# Step 3: Post-process
python3 process_program_table.py \
  --input-file /data/bdc_$(date +%Y%m)/program_table/program_table_updated_*.json \
  --output-dir /data/bdc_$(date +%Y%m)/program_table
```

### Re-run Post-Processing Only

```bash
python3 process_program_table.py \
  --input-file ./program_table_updated.json \
  --output-dir ./output
```

---

## Requirements

- Python 3.7+
- pandas
- requests

```bash
pip install pandas requests
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Import errors | Run from the correct directory |
| Missing Jira file | Verify path and file format (CSV/Excel) |
| Permission denied | Run `chmod +x run_pipeline.sh` |
| API timeout | Re-run the pipeline; Gen3 API occasionally times out |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| v3.0 | 2026-01 | Added `process_program_table.py` combining post-processing steps |
| v2.0 | 2025-10 | Clean directory structure with program_table directory |
| v1.0 | - | Initial modular refactoring with shared utilities |
