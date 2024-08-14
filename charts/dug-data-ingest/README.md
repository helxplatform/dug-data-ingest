# dug-data-ingest

A single Helm chart for all Dug Data Ingest tasks. The idea is that you can include different value files to
determine which script should be executed, which should be the only difference between the different tasks.

When installing Helm charts from this directory, make sure you include the following values files:
- `values.yaml`: Basic shared values.
- `values-secret.yaml`: Secret login values. 
  - You can copy `values-secret.yaml.txt` to `values-secret.yaml` and then fill in the TODOs.
- One of the files from `values/` for the specific ingest you want.

You should install multiple CronJobs using this template for each data ingest you want to support.