# Resource and carbon metrics

The collector joins the Nextflow trace with Prometheus series for each task
pod and emits both VIVO Turtle and an audit JSON file.

## Measurements

| Value | Source and calculation |
| --- | --- |
| CPU time | Sum of positive increases in `container_cpu_usage_seconds_total` |
| Average memory | Time-weighted average of container working-set bytes |
| Peak memory | Maximum container working-set bytes in the accounting window |
| Energy | Sum of positive per-pod Kepler joule-counter increases |
| Carbon intensity | Time-matched CO2Map.de German consumption intensity |
| Carbon | `energy_kWh * carbon_intensity_kg_CO2e_per_kWh` |
| Duration | Nextflow task and workflow timestamps |

The tested Kepler metric is `kepler_container_joules_total`. The audit JSON
records every PromQL selector, returned sample, pod attribution, accounting
window, and carbon source value so the calculation can be reviewed.

## Prometheus access

Start the port-forward and leave it running:

```bash
./scripts/port-forward-prometheus.sh
```

Port `19090` is used to avoid collisions with a local service on `9090`.
Opening `http://127.0.0.1:19090/-/ready` should return a successful response.

## Resumed runs

Nextflow `-resume` marks prior tasks as `CACHED`; those rows reference the pods
that originally produced the cached outputs. Use:

```bash
INCLUDE_CACHED_ORIGIN_METRICS=1 \
  ./scripts/collect-metadata.sh RUN_ID
```

This creates one composite view of the logical Nextflow session. Without that
flag, stages containing only cached tasks correctly show no resource use in
the resume attempt.

Prometheus retention must still cover the original pod timestamps. If those
series have expired, the collector cannot reconstruct measured resource use.

## Carbon-data status

The wrapper uses CO2Map preliminary data because paid Electricity Maps
historical access was not available. Preliminary data is near-real-time and
may later be revised. For archival publication, rerun the collector with
`--co2map-data-status historical` after finalized data becomes available.
