# Input data

## The tested tile

The documented successful run used FORCE tile `X0103_Y0103`:

- 15 FORCE Level 2 scenes;
- one `BOA.tif` and one `QAI.tif` GeoTIFF per scene;
- one `OVV.jpg` quicklook per scene;
- 45 files in total.

The workflow reads the 30 `BOA.tif` and `QAI.tif` files. The quicklooks are
part of the tile but are not workflow inputs.

The tile itself is not distributed in this repository. The repository tracks
only `data/X0103_Y0103/SHA256SUMS`, which pins every file of the tested run,
so any obtained copy can be verified as bit-identical to the tested input.
`.gitignore` keeps the TIFF and JPEG payloads out of Git.

## Obtaining the tile

FONDA cluster users: the tile is available on the shared workspace at
`/workspace/input/X0103_Y0103` in the `geoflow-uploader` pod. Copy it into
`data/X0103_Y0103` with:

```bash
./scripts/import-tested-input-from-cluster.sh
```

The script reads the tile one file at a time, writes each download through a
temporary file, and validates the finished directory against the manifest.
It never writes to the source pod or PVC.

Everyone else: request the tile from the maintainer or from FONDA B5 and
place the 45 files under `data/X0103_Y0103`. Any FORCE Level 2 output for
tile `X0103_Y0103` covering the same 15 scenes works identically if it
matches the checksums.

## Verification

Verify the local copy:

```bash
./scripts/verify-input.sh
```

After upload, verify the PVC copy:

```bash
./scripts/verify-cluster-input.sh
```

Both checks must report all 45 files as valid. The checksum for
`20040703_LEVEL2_LND05_BOA.tif` is:

```text
cd9060ad0bca14c4e59dc69facf816adfd630b95ffcea2dfdaa7fc854c124ff2
```
