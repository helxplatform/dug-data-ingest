# HEAL scripts

`get_heal_studies.py` generates Dug-format (`dug_data_model`) study and variable
JSON files from the HEAL Platform MDS, and (optionally) publishes them to lakeFS.

`get_heal_platform_mds_data_dicts.py` is deprecated

## Data flow

For each study returned by the HEAL Platform MDS (`--mds-metadata-endpoint`):

1. **CDE mapping.** All `.dug.json` CDE files (from `--cde-location` or
   `--cde-lakefs-location`) are read once up front to build two mappings:
   - study &rarr; which CDE sections it uses
   - study variable &rarr; which specific CDE measure/section it maps to

   A single measure can be reused across more than one CDE section. Resolving _which_
   section applies to a given study's variable requires knowing every CDE section the
   study is registered against, so this is a two-pass process: collect all candidate
   `{measure, section}` pairs per study/variable first, then once every CDE file has been
   read, pick whichever candidate matches a section the study is actually mapped to
   (`_resolve_variable_cde_mappings`).

2. **Research program/network normalization.** Each study's HDPID is looked up against
   the Research Program/Network API (`--research-program-network-endpoint`), then
   normalized through `--research-normalization-csv` (see below) into a canonical name
   and, where applicable, an acronym. The two are combined into one string, e.g.
   `"(JCOIN): Justice Community Opioid Innovation Network"` (see `_research_tag_value`) —
   this combined form is what's used both in the element's `programs` list (so the
   website's exact-match program filter/facet resolves on the acronym as well as the full
   name) and as the tag value. Research network has no equivalent `programs` entry — it's
   tag-only.

3. **Element generation.** Study metadata, variable-level metadata dictionaries, CDE
   mappings, and research program/network tags are assembled into `dug_data_model.v2`
   `DugStudy`/`DugVariable` objects and written to `<OUTPUT>/<HDPID>.dug.json`.

4. **lakeFS publish** (only if `--lakefs-output` is given). Files aren't written straight
   to the target branch. Instead:
   - a short-lived branch (`ingest-<unix-timestamp>`) is created off the target branch
   - every upload/delete for this run happens on that branch
   - once the run finishes, the branch is **committed** (commit message = the same
     summary text sent to Slack) and **merged** into the target branch, then deleted

   This means the target branch only ever sees one commit per run, so any lakeFS Action
   (webhook) configured on that branch fires once per ingest run, not once per file.

5. **Slack notification** (only if `SLACK_WEBHOOK_URL` is set). A summary is sent after
   the run: CDE files read, studies/variables mapped, files written/uploaded/deleted, and
   whether the lakeFS commit/merge succeeded, failed, or had nothing to merge. Sent on
   success, on "no changes," and on merge failure — not just on a clean run.

## Local setup

```bash
cd dug-data-ingest/scripts
uv venv --python 3.12 venv        # only needed once; reuse the existing scripts/venv otherwise
source venv/bin/activate
uv pip install -r heal/requirements.txt
```

`dug-data-model` (in `heal/requirements.txt`) requires Python >= 3.12 and isn't on PyPI
yet, so it's pinned to a GitHub commit in that requirements file.

### Running locally

Either run the full pipeline via `ingest.sh` (matches what the CronJob runs):

```bash
export LAKEFS_HOST=...        # from charts/dug-data-ingest/values.yaml (lakeFS.host)
export LAKEFS_USERNAME=...    # from charts/dug-data-ingest/values-secret.yaml
export LAKEFS_PASSWORD=...    # from charts/dug-data-ingest/values-secret.yaml
export SLACK_WEBHOOK_URL=...  # optional

cd scripts
./heal/ingest.sh
```

By default this reads CDEs from `lakefs://heal-cdes/main/` and publishes to
`lakefs://heal-mds-studies/test/` (override with `HEAL_CDE_LAKEFS_LOCATION` /
`HEAL_LAKEFS_OUTPUT` env vars — this is exactly what the Helm chart's `extraEnv` sets for
the deployed CronJob, see below).

Or call `get_heal_studies.py` directly for more control:

```bash
python3 get_heal_studies.py OUTPUT_DIR \
    --cde-location /path/to/local/cde/dug/jsons \
    # or: --cde-lakefs-location lakefs://heal-cdes/main/
    [--lakefs-output lakefs://heal-mds-studies/test/] \
    [--mds-metadata-endpoint https://healdata.org/mds/metadata] \
    [--research-program-network-endpoint <url>] \
    [--research-normalization-csv data/research_program_network_normalization.csv] \
    [--debug]
```

`OUTPUT_DIR` and `--lakefs-output` aren't mutually exclusive — local files are always
written to `OUTPUT_DIR` regardless of whether `--lakefs-output` is also given; at least
one of the two is required. Similarly at least one of `--cde-location` /
`--cde-lakefs-location` is required (the lakeFS one takes precedence if both are given).

### The normalization CSV

`data/research_program_network_normalization.csv` maps raw research program/network
strings from the API into canonical names. Columns: `raw_value` (as returned by the API,
matched case-insensitively), `code` (acronym, blank if none), `canonical_name`. One row
per known raw string variant — including a self-mapping row for the correct spelling
itself, so typo'd variants collapse to the same canonical entry. If the same `raw_value`
appears twice with conflicting `canonical_name`/`code`, the first row wins and a warning
is logged. Values the API returns that aren't in this table pass through unchanged (also
logged, as a single summary warning listing all distinct unmapped values for that run).

## Deploying to the cluster

The chart is `charts/dug-data-ingest` (a generic CronJob + PVC template shared with BDC's
ingest; `values/heal-ingest.yaml` is the HEAL-specific override layer).

1. **Secrets.** Copy `charts/dug-data-ingest/values-secret.yaml.txt` to
   `values-secret.yaml` (gitignored) in the same directory and fill in the real lakeFS
   username/password (and Slack webhook URL, optional).

2. **Image tag.** `containers.renci.org` prunes any tag that doesn't start with `v`, so
   images are only published from GitHub Releases (see
   `.github/workflows/release-docker-to-renci-containers.yaml`) — not from plain branch
   pushes. To deploy code that isn't in a released image yet:
   - publish a new GitHub Release tagged `vX.Y.Z` (or `vX.Y.Zalpha` etc.) from the branch
     you want to deploy — this triggers the build automatically
   - once it succeeds, bump `jobExecutor.image.tag` in
     `charts/dug-data-ingest/values.yaml` to match

3. **Deploy:**

   ```bash
   cd charts/dug-data-ingest
   helm upgrade --install dug-data-ingest-heal . \
     -f values/heal-ingest.yaml \
     -f values-secret.yaml \
     -n heal-dev
   ```

4. **Verify:**

   ```bash
   kubectl get cronjob dug-data-ingest-heal -n heal-dev -o wide
   ```

   Confirm `SCHEDULE`, `TIMEZONE`, and `IMAGES` match what you expect (schedule is
   currently Mondays 9am `America/New_York`, set in `values/heal-ingest.yaml`).

5. **Trigger a one-off run** (useful for testing without waiting for the schedule):

   ```bash
   kubectl create job dug-data-ingest-heal-manual-$(date +%s) \
     --from=cronjob/dug-data-ingest-heal -n heal-dev
   kubectl get pods -n heal-dev -l job-name=<job-name-from-above>
   kubectl logs -f <pod-name> -n heal-dev
   ```

## Troubleshooting

**`lakefs.exceptions.BadRequestException: ... 'uncommitted changes (dirty branch)'`** on
merge — the _target_ branch (not the ephemeral ingest branch) has uncommitted changes
sitting on it, usually from an old run that predates the branch/commit/merge feature
(anything that wrote directly to a branch via `lakefs_spec` without a commit step). Check
and clear it:

```python
import lakefs
client = ...  # see get_heal_studies._get_lakefs_client()
branch = lakefs.Branch("heal-mds-studies", "test", client=client)
list(branch.uncommitted())   # inspect what's pending
branch.commit(message="...")  # to keep it, or:
branch.reset_changes()        # to discard it
```

Be careful doing this against `main` (or whatever the CronJob's real target branch is) —
prefer committing over discarding there, since it's not a disposable branch.

**`OSError: [Errno 28] No space left on device`** — the PVC (`dataStorage` in
`values/heal-ingest.yaml`) is too small for the current study count/metadata size. It's
expandable in place (`helm upgrade` after bumping the value) as long as the storage class
has `allowVolumeExpansion: true` — no need to delete/recreate the PVC.
