#!/usr/bin/env python3
"""Deliberately regenerate tree release answers after the runner source freezes.

Run once with each installed node interpreter; only observed runtime profiles
are recorded, and no runtime or calibration facts are mocked::

  python3 tools/generate-tree-release-kat.py \
    --python /path/to/pytorch/bin/python --python /path/to/native-tree/bin/python

The public fixed test key and synthetic vector are defined in test support.
The native-container fixture has its separate generator; this numeric KAT
binds the actual runtime, including the complete current runner source hash.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "inst/python/tests/fixtures/tree-release-kat-v3.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", action="append", dest="interpreters",
                        help="verified node Python; repeat for every installed runtime")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--capture", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.capture:
        sys.path.insert(0, str(ROOT / "inst/flower_app"))
        sys.path.insert(0, str(ROOT / "inst/python/tests"))
        from tree_release_kat_support import capture_record
        print(json.dumps(capture_record(), sort_keys=True, allow_nan=False))
        return
    if not args.interpreters:
        parser.error("supply --python for each node runtime to verify")
    entries = {}
    for interpreter in args.interpreters:
        completed = subprocess.run(
            [interpreter, str(Path(__file__).resolve()), "--capture"],
            check=True, capture_output=True, text=True)
        record = json.loads(completed.stdout)
        # Keep selection independent of runner hash; changes in source must
        # cause stale-answer failure on an otherwise verified environment.
        key = json.dumps({"runtime": {name: value for name, value in record["runtime"].items()
                                      if name != "runner_sha256"},
                          "numeric_profile": record["numeric_profile"]},
                         sort_keys=True, separators=(",", ":"), allow_nan=False)
        if key in entries and entries[key] != record:
            raise RuntimeError("the same runtime profile produced conflicting tree answers")
        entries[key] = record
    output = {"contract": "dsflower-tree-release-kat-v3",
              "profiles": [entries[key] for key in sorted(entries)]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, sort_keys=True, indent=2, allow_nan=False) + "\n")
    print("Recorded %d observed runtime profile(s) in %s" % (len(entries), args.output))


if __name__ == "__main__":
    main()
