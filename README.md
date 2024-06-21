# dug-data-ingest
Jobs and scripts for ingesting data into Dug

Use **kubefiles/dug-data-ingest-app.yaml** to run ingestion job.

Do not forget to set these values before run:

```
LAKEFS_USERNAME
LAKEFS_PASSWORD
```

and update this value when needed
`image: containers.renci.org/vgl/bdc-ingest`
