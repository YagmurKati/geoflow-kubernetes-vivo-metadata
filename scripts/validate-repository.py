#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "data" / "X0103_Y0103"
CHECKSUM_FILE = INPUT_DIR / "SHA256SUMS"
FORBIDDEN_NAMES = {
    "config.yml",
    "kubeconfig",
    "wg0.conf",
}
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"client-key-data:\s*[A-Za-z0-9+/=]{40,}"),
)
IGNORED_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "runtime",
    "venv",
}


def fail(message: str) -> None:
    raise RuntimeError(message)


def validate_input(require_files: bool) -> None:
    if not CHECKSUM_FILE.is_file():
        fail(f"Missing checksum manifest: {CHECKSUM_FILE}")

    expected: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        CHECKSUM_FILE.read_text(encoding="utf-8").splitlines(), start=1
    ):
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", raw_line)
        if not match:
            fail(f"Malformed SHA256SUMS line {line_number}")
        digest, name = match.groups()
        if "/" in name or name in expected:
            fail(f"Unsafe or duplicate checksum filename: {name}")
        expected[name] = digest

    boa_count = sum(name.endswith("_BOA.tif") for name in expected)
    qai_count = sum(name.endswith("_QAI.tif") for name in expected)
    ovv_count = sum(name.endswith("_OVV.jpg") for name in expected)
    if (boa_count, qai_count, ovv_count) != (15, 15, 15):
        fail(
            "Expected 15 BOA, 15 QAI, and 15 OVV files; "
            f"found {boa_count}, {qai_count}, and {ovv_count}"
        )

    actual_files = {
        path.name
        for path in INPUT_DIR.iterdir()
        if path.is_file() and path.name != CHECKSUM_FILE.name
    }
    if not actual_files:
        if require_files:
            fail(
                f"Input tile not found under {INPUT_DIR}; "
                "obtain it as described in docs/INPUT_DATA.md"
            )
        print(f"Manifest valid: {len(expected)} files (tile not present)")
        return

    if actual_files != set(expected):
        missing = sorted(set(expected) - actual_files)
        extra = sorted(actual_files - set(expected))
        fail(f"Input manifest mismatch; missing={missing}, extra={extra}")

    for name, expected_digest in expected.items():
        digest = hashlib.sha256()
        with (INPUT_DIR / name).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_digest:
            fail(f"Checksum mismatch: {name}")
    print(f"Input checksums valid: {len(expected)} files")


def validate_metadata() -> None:
    try:
        from rdflib import Graph
    except ImportError as exc:
        raise RuntimeError(
            "Install requirements-dev.txt before full validation"
        ) from exc

    ttl_files = sorted((ROOT / "metadata").rglob("*.ttl"))
    if not ttl_files:
        fail("No example VIVO Turtle files found")
    for path in ttl_files:
        Graph().parse(path, format="turtle")
        print(f"RDF valid: {path.relative_to(ROOT)}")

    json_files = sorted((ROOT / "metadata" / "examples").glob("*.json"))
    if not json_files:
        fail("No example audit JSON found")
    for path in json_files:
        with path.open(encoding="utf-8") as handle:
            json.load(handle)
        print(f"JSON valid: {path.relative_to(ROOT)}")


def validate_repository_safety() -> None:
    for path in ROOT.rglob("*"):
        relative = path.relative_to(ROOT)
        if not path.is_file() or any(
            part in IGNORED_PARTS for part in relative.parts
        ):
            continue
        if relative.parts[:2] == ("metadata", "generated"):
            continue
        if path.name in FORBIDDEN_NAMES or path.suffix in {".pem", ".key"}:
            fail(f"Sensitive filename must not be committed: {path}")
        if path.stat().st_size > 20 * 1024 * 1024:
            continue
        payload = path.read_bytes()
        for pattern in SECRET_PATTERNS:
            if pattern.search(payload):
                fail(f"Possible secret found in {path}")
    print("Repository safety checks passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-only", action="store_true")
    args = parser.parse_args()

    try:
        validate_input(require_files=args.input_only)
        if not args.input_only:
            validate_metadata()
            validate_repository_safety()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
