# Troubleshooting

## `tar: executable file not found` in `nextflow-driver`

The pinned Nextflow driver image does not contain `tar`. Use
`scripts/upload-workflow.sh` and `scripts/upload-input.sh`; both extract
through the Alpine `geoflow-uploader` pod, which mounts the same PVC.

## `tar: short read`

This means the archive stream was interrupted or a source-side `tar` failed.
Run the upload again and then run `scripts/verify-cluster-input.sh`. A transfer
is complete only when every checksum reports `OK`.

## `build_vrt-stack.py: command not found`

Run `scripts/prepare-workflow.sh` and re-upload the workflow. The included
patch invokes:

```text
python3 /workspace/geoflow/scripts-python/build_vrt-stack.py
```

After that fix, resume with:

```bash
RESUME=1 ./scripts/run-workflow.sh NEW_RUN_ID
```

Do not run `build_vrt-stack.py` manually; it needs the task work directory and
arguments assembled by Nextflow.

## Prometheus returned an unexpected response

Make sure the port-forward uses `19090`, then test:

```bash
curl -fsS http://127.0.0.1:19090/-/ready
```

An HTML page or response from a different service means the wrong local port
was used.

## Missing energy or memory

Confirm Prometheus still retains the task interval and exposes:

```text
container_cpu_usage_seconds_total
container_memory_working_set_bytes
kepler_container_joules_total
```

For resumed runs, use `INCLUDE_CACHED_ORIGIN_METRICS=1`. Cached stages will
remain zero if their original pod series are no longer retained.

## CO2Map or Electricity Maps errors

The default wrapper uses CO2Map preliminary data and needs no Electricity Maps
subscription. HTTP 401 from Electricity Maps means the account cannot access
the historical range endpoint; HTTP 403/1010 indicates access was blocked.
