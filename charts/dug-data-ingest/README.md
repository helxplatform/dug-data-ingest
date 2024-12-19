# dug-data-ingest

A single Helm chart for all Dug Data Ingest tasks. The idea is that you can include different value files to
determine which script should be executed, which should be the only difference between the different tasks.

When installing Helm charts from this directory, make sure you include the following values files:
- `values.yaml`: Basic shared values.
- `values-secret.yaml`: Secret login values. 
  - You can copy `values-secret.yaml.txt` to `values-secret.yaml` and then fill in the TODOs.
- One of the files from `values/` for the specific ingest you want.

You should install multiple CronJobs using this template for each data ingest you want to support.

## Using this Helm chart

This Helm chart creates a [CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/)
in a Kubernetes cluster set up for either BDC or HEAL ingest. To install this Helm chart, set up Helm and
then run:

```shell
$ helm install -n bdc-dev -f values/bdc-ingest.yaml -f values-secret.yaml dug-data-ingest-bdc .
```

(If upgrading to a new version of this chart, replace `install` with `upgrade`).

This will create a CronJob named `dug-data-ingest-bdc` that creates pods named
`dug-data-ingest-bdc-[alphanumeric code]` on the specified schedule.

You can uninstall the Helm chart with:

```shell
$ helm uninstall -n bdc-dev dug-data-ingest-bdc
```

