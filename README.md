# dug-data-ingest - Jobs and scripts for ingesting data into Dug via LakeFS

This repository contains a number of Kubernetes jobs and scripts for ingesting data
into LakeFS for eventual ingestion into Dug.

To simplify how this works, we are initially building this as a single Docker image
created by the root [Dockerfile](./Dockerfile), which contains all the needed scripts
(in the `scripts/` directory) and installed programs. The Helm charts in the
`./charts` directory can then be used to run the appropriate scripts to download the
data (always to the `/data` directory in the Docker image) and then upload it to our
LakeFS ingest using the [avalon](https://github.com/helxplatform/avalon) library built
as part of the Helx Platform project.



Do not forget to set these values before run:

```
LAKEFS_USERNAME
LAKEFS_PASSWORD
```

and update this value when needed
`image: containers.renci.org/vgl/bdc-ingest`
