# dug-data-ingest - Jobs and scripts for ingesting data into Dug via LakeFS

This repository contains a [Docker image](./Dockerfile) that contains all the scripts and programs needed to download
data from a source and upload them into a LakeFS instance, and [a Helm chart](./charts/dug-data-ingest) for setting up
a CronJob to execute this Docker image at a regular schedule on Sterling. For simplicity's sake, there is only a single
Dockerfile (that contains all the scripts and all the programs needed) and only a single Helm chart (with different
values.yaml files to choose which ingest you want to set up), but in the future this may need to be reorganized.

## Instructions for installing Helm chart

1. Copy [values-secret.yaml.txt](./charts/dug-data-ingest/values-secret.yaml.txt) to
   `charts/dug-data-ingest/values-secret.yaml` and add the authentication details for
   the LakeFS server.
2. Helm install the chart in `charts/dug-data-ingest` with three values files:
   1. `values.yaml`, with the default settings.
   2. `values-secret.yaml`, with the authentication details.
   3. `values/*.yaml` corresponding to the Dug Data Ingest you want to set up, such as
      `values/bdc-ingest.yaml` for the BDC ingest.

## Instructions for creating a new Dug ingest.
1. Create a new directory for your ingest in the scripts directory, e.g. `scripts/babel`.
2. Add the scripts needed to run an ingest in this directory. We usually have an `ingest.sh` script to run an
   ingest, but you can call it whatever you like.
3. Add any Python requirements to [requirements.txt](./requirements.txt).
4. Add any Alpine requirements to [Dockerfile](./Dockerfile).
5. Add a values file in the `charts/dug-data-ingest/values` directory, e.g. `babel-ingest.yaml`. At a minimum
   this should provide a name for the ingest job and provide the name of the script to be executed by `bash`.
   - You may need to modify the `charts/dug-data-ingest/templates` files; if so, please make sure you don't break the
     other ingests!
6. Add an `on:push` trigger to `.github/workflows/release-docker-to-renci-containers.yaml` to generate a container
   named after your branch, then use this tag to test your new CronJob, then remove them once you're done.
