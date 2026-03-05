# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Does

This repo contains scripts and a Docker image for downloading biomedical data from external sources (BDC, HEAL Platform) and uploading it to a LakeFS instance. A Helm chart deploys these scripts as Kubernetes CronJobs on a schedule.

There is a single Dockerfile that bundles all ingest scripts. The image is published to `containers.renci.org/helxplatform/dug-data-ingest` via GitHub Actions on push/release.

## Building the Docker Image

```bash
docker build -t dug-data-ingest .
```

## Running Scripts Locally

Set required environment variables, then run the ingest script directly:

```bash
export LAKEFS_HOST=http://your-lakefs-host
export LAKEFS_USERNAME=your-username
export LAKEFS_PASSWORD=your-password
export LAKEFS_REPOSITORY=your-repo
export PICSURE_TOKEN=your-token  # BDC only

# BDC ingest
bash scripts/bdc/ingest.sh [--output-dir /tmp/bdc-output]

# HEAL ingest
bash scripts/heal/ingest.sh
```

Install Python dependencies locally:
```bash
pip install -r requirements.txt
```

Run individual Python scripts:
```bash
python scripts/bdc/xml_generator.py --picsure-csv path/to/picsure.csv --gen3-csv path/to/gen3.csv --output-dir /tmp/output
python scripts/lakefs/generate_lakefs_dbgap_xml_index.py -r heal-mds-import -r heal-mds-studies
```

## Helm Chart Deployment

```bash
# Install/upgrade a specific ingest
helm upgrade --install dug-data-ingest-bdc charts/dug-data-ingest \
  -f charts/dug-data-ingest/values.yaml \
  -f charts/dug-data-ingest/values-secret.yaml \
  -f charts/dug-data-ingest/values/bdc-ingest.yaml
```

`values-secret.yaml` is gitignored — copy from `values-secret.yaml.txt` and fill in credentials.

## Architecture

### Ingest Pipeline Pattern

Each ingest follows this pattern:
1. Download data from source API (Gen3, PicSure, HEAL MDS, etc.) into `/data/`
2. Generate dbGaP-formatted XML files if needed
3. Upload to LakeFS using `rclone sync` (configured via env vars)
4. Commit the branch via LakeFS REST API (`curl -X POST .../commits`)

### Scripts Directory Structure

- `scripts/bdc/` — BioData Catalyst ingest
  - `ingest.sh` — main pipeline: PicSure → Gen3 → XML generation → LakeFS upload
  - `get_bdc_studies_md_from_picsure.py` — extracts variable-level metadata from PicSure
  - `get_bdc_studies_md_from_gen3.py` — extracts study-level metadata from Gen3
  - `xml_generator.py` — generates dbGaP-format XML from combined PicSure + Gen3 data
  - `run_dbgap_xml_gen_fallback.py` — orchestrates dbGaP download with XML generation fallback
- `scripts/heal/` — HEAL Platform ingest
  - `ingest.sh` — downloads from HEAL MDS, uploads to multiple LakeFS repos
  - `get_heal_platform_mds_data_dicts.py` — fetches data dictionaries from HEAL Platform MDS API
- `scripts/lakefs/` — Utility scripts for LakeFS
  - `generate_lakefs_dbgap_xml_index.py` — indexes dbGaP XML files across LakeFS repositories
- `scripts/heal-cdes/` — HEAL Common Data Elements (static JSON data)
- `scripts/dug/` — Shell scripts for Dug data dictionary operations

### Helm Chart

The chart (`charts/dug-data-ingest/`) creates a `CronJob` and a `PersistentVolumeClaim`. The script to execute is specified via `jobExecutor.script` in a per-ingest values file. Secrets (`LAKEFS_HOST`, `LAKEFS_USERNAME`, `LAKEFS_PASSWORD`, `PICSURE_TOKEN`) are read from a Kubernetes Secret named `dug-ingest-secrets`.

### Adding a New Ingest

1. Create `scripts/<name>/` with an `ingest.sh`
2. Add Python dependencies to `requirements.txt`
3. Add Alpine packages to `Dockerfile` if needed
4. Create `charts/dug-data-ingest/values/<name>-ingest.yaml` with at minimum `jobExecutor.script` and `nameOverride`

### LakeFS Authentication for Utility Scripts

The `generate_lakefs_dbgap_xml_index.py` script reads credentials from `~/.lakectl.yaml` or from env vars `LAKECTL_SERVER_ENDPOINT_URL`, `LAKECTL_CREDENTIALS_ACCESS_KEY_ID`, and `LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY`.
