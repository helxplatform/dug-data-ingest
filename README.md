# dug-data-ingest - Jobs and scripts for ingesting data into Dug via LakeFS

This repository contains a [Docker image](./Dockerfile) that contains all the scripts and programs needed to download data from a
source and upload them into a LakeFS instance, and [a Helm chart](./charts/dug-data-ingest) for setting up a CronJob to execute this Docker image
at a regular schedule on Sterling. For simplicity's sake, these are

## Instructions

1. Copy [values-secret.yaml.txt](./charts/dug-data-ingest/values-secret.yaml.txt) to
   `charts/dug-data-ingest/values-secret.yaml` and add the authentication details for
   the LakeFS server.
2. Helm install the chart in `charts/dug-data-ingest` with three values files:
   1. `values.yaml`, with the default settings.
   2. `values-secret.yaml`, with the authentication details.
   3. `values/*.yaml` corresponding to the Dug Data Ingest you want to set up, such as
      `values/bdc-ingest.yaml` for the BDC ingest.
