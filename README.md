# Geoflow on Kubernetes with VIVO metadata

This repository reproduces the Kubernetes execution of
[CRC-FONDA/geoflow](https://github.com/CRC-FONDA/geoflow) and collects CPU,
memory, energy, carbon, timing, container, node, and provenance metadata as
VIVO-ready Turtle.

It provides everything the upstream repository does not:

- Kubernetes storage, RBAC, driver, and uploader manifests;
- the Geoflow compatibility patch and Kubernetes Nextflow config;
- scripts for deployment, upload, execution, monitoring, and metadata collection;
- the metadata collector and an audited example output;
- checksums for the exact FORCE input tile of the tested run;
- VIVO relationship and import guidance.

`scripts/prepare-workflow.sh` clones the upstream workflow from the public
CRC-FONDA/geoflow repository at the pinned commit into `runtime/geoflow` and
patches it locally. The FORCE input tile is not distributed in this
repository; [docs/INPUT_DATA.md](docs/INPUT_DATA.md) explains how to obtain
and verify it.

## Tested setup

| Component | Tested value |
| --- | --- |
| Kubernetes | v1.27.7 FONDA cluster |
| Namespace | `yagmur` |
| Storage | `cephfs`, ReadWriteMany |
| Nextflow | `25.04.8` |
| Workflow revision | `CRC-FONDA/geoflow@acd0d618d0451c65ecb12466a412abb65dda1a87` |
| Workflow image | `floriankaterndahl/geoflow@sha256:96b6c613ed5685beb86746d9f4b764d461ac3bd2553376111f8e0c9a9a3db35d` |
| Input tile | `X0103_Y0103`, 15 FORCE scenes |
| Prometheus | kube-prometheus |
| Energy metric | `kepler_container_joules_total` |
| Carbon source | CO2Map.de preliminary German consumption intensity |

## Repository layout

```text
collector/        Metadata collector
data/             Checksum manifest for the tested FORCE input tile
docs/             Input, metrics, VIVO, and troubleshooting notes
k8s/              Kubernetes manifests
metadata/         Example trace, log, audit JSON, and VIVO Turtle
patches/          Changes required by the tested Geoflow container
scripts/          End-to-end commands
workflow/         Upstream pin and Nextflow Kubernetes config
```

## Prerequisites

- Bash, Git, Python 3, `kubectl`, and `tar`
- access to a Kubernetes namespace
- a ReadWriteMany storage class
- Prometheus with kubelet/cAdvisor metrics
- Kepler metrics for measured energy
- the FORCE input tile under `data/X0103_Y0103`
  ([docs/INPUT_DATA.md](docs/INPUT_DATA.md))

## Run the workflow

Clone the repository and choose the namespace and storage class:

```bash
git clone https://github.com/YagmurKati/geoflow-kubernetes-vivo-metadata.git
cd geoflow-kubernetes-vivo-metadata

export NS=<your-namespace>            # tested: yagmur
export STORAGE_CLASS=<your-class>     # tested: cephfs
```

Check access, fetch and patch the pinned workflow, and deploy the support pods:

```bash
./scripts/check-prerequisites.sh
./scripts/prepare-workflow.sh
./scripts/deploy-k8s.sh
```

Place the input tile under `data/X0103_Y0103`, then upload the patched
workflow and the input. The upload verifies the tile against
`data/X0103_Y0103/SHA256SUMS` first:

```bash
./scripts/upload-workflow.sh
./scripts/upload-input.sh
./scripts/verify-cluster-input.sh
```

Start a run. The run ID becomes part of every trace and log filename:

```bash
./scripts/run-workflow.sh geoflow-run-01
```

The run command remains attached until Nextflow exits. In another terminal:

```bash
./scripts/status.sh geoflow-run-01
```

Follow the console log:

```bash
kubectl -n "$NS" exec -it nextflow-driver -- \
  tail -f /workspace/results/nextflow-geoflow-run-01.log
```

The run is complete when the log ends with `Succeeded`.

### Resume after a failure

After fixing the cause, reuse the same Nextflow work directory:

```bash
RESUME=1 ./scripts/run-workflow.sh geoflow-run-01-resume
```

For a resumed logical session, collect origin-pod metrics so cached stages
retain the CPU and energy that produced their outputs:

```bash
INCLUDE_CACHED_ORIGIN_METRICS=1 \
  ./scripts/collect-metadata.sh geoflow-run-01-resume
```

## Collect metadata

In a separate terminal, forward Prometheus to local port `19090`:

```bash
./scripts/port-forward-prometheus.sh
```

Then collect metadata:

```bash
./scripts/collect-metadata.sh geoflow-run-01
```

The collector writes:

- a VIVO-ready `.ttl` file;
- an audit `.metrics.json` file containing the metric queries and source values.

See [docs/METRICS.md](docs/METRICS.md) for formulas and limitations.

## Import into VIVO

Use VIVO's **Add/Remove RDF data** page and add the generated Turtle file.
The output links the workflow to FONDA B5 and to Felix Kummer's existing VIVO
individual, and links the run to the existing Kubernetes Backend individual.

See [docs/VIVO_IMPORT.md](docs/VIVO_IMPORT.md) for the exact import and
cleanup procedure.

## Input data

The workflow reads paired FORCE `BOA.tif` and `QAI.tif` products. The tested
run used the 15-scene `X0103_Y0103` tile; `data/X0103_Y0103/SHA256SUMS` pins
every file of that tile, so any obtained copy can be verified as
bit-identical to the tested input. [docs/INPUT_DATA.md](docs/INPUT_DATA.md)
explains how to obtain the tile and verify it locally and on the cluster.

## Attribution

- Workflow: [CRC-FONDA/geoflow](https://github.com/CRC-FONDA/geoflow)
  by Florian Katerndahl and Dirk Pflugmacher
- FONDA B5 and trace support: Felix Kummer
- Kubernetes execution and metadata collection: Yagmur Kati

See [NOTICE.md](NOTICE.md) for pinned revisions.

## License

The scripts, manifests, collector, patch, and documentation in this
repository are licensed under the [MIT License](LICENSE). The upstream
Geoflow workflow and the FORCE input data are not part of this repository and
keep their own terms.
