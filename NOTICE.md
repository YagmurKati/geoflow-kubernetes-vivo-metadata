# Attribution

Reproducibility companion for
[CRC-FONDA/geoflow](https://github.com/CRC-FONDA/geoflow).

- Upstream repository: https://github.com/CRC-FONDA/geoflow
- Pinned revision: `acd0d618d0451c65ecb12466a412abb65dda1a87`
- Container: `floriankaterndahl/geoflow` pinned by digest in the config
- Workflow authors: Florian Katerndahl and Dirk Pflugmacher
- FONDA B5 and trace support: Felix Kummer
- Kubernetes execution and metadata collection: Yagmur Kati

The upstream source is cloned from the public repository at run time by
`scripts/prepare-workflow.sh` and patched locally. It is not redistributed
here, and the patch does not relicense it.

The FORCE input tile is not distributed here. `data/X0103_Y0103/SHA256SUMS`
pins the exact files of the tested run; see `docs/INPUT_DATA.md`.
