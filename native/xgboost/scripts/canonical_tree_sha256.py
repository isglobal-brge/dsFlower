#!/usr/bin/env python3
"""Hash a Git tree's paths, modes, types, and raw contents canonically."""

from __future__ import annotations

import hashlib
import pathlib
import subprocess
import sys


DOMAIN = b"dsflower-git-source-sha256-v1"


def git(source: pathlib.Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(source), *args],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def frame(digest, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def canonical_tree_sha256(source: pathlib.Path, treeish: str) -> str:
    records = git(source, "ls-tree", "-r", "-z", "--full-tree", treeish)
    entries: list[tuple[bytes, bytes, bytes, bytes]] = []
    for record in records.split(b"\0"):
        if not record:
            continue
        metadata, path = record.split(b"\t", 1)
        mode, object_type, object_id = metadata.split(b" ", 2)
        if object_type not in (b"blob", b"commit"):
            raise ValueError(f"unsupported Git object type: {object_type!r}")
        entries.append((path, mode, object_type, object_id))
    entries.sort(key=lambda entry: entry[0])
    if len({entry[0] for entry in entries}) != len(entries):
        raise ValueError("duplicate path in Git tree")

    process = subprocess.Popen(
        ["git", "-C", str(source), "cat-file", "--batch"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stdout is not None
    digest = hashlib.sha256()
    frame(digest, DOMAIN)
    try:
        for path, mode, object_type, object_id in entries:
            if object_type == b"blob":
                process.stdin.write(object_id + b"\n")
                process.stdin.flush()
                header = process.stdout.readline().rstrip(b"\n").split(b" ")
                if len(header) != 3 or header[0] != object_id or header[1] != b"blob":
                    raise ValueError("unexpected response from git cat-file --batch")
                size = int(header[2])
                content = process.stdout.read(size)
                if len(content) != size or process.stdout.read(1) != b"\n":
                    raise ValueError("truncated response from git cat-file --batch")
            else:
                # A gitlink has no local blob. Bind its pinned commit ID; the
                # submodule worktree is verified independently by the caller.
                content = object_id
            frame(digest, mode)
            frame(digest, object_type)
            frame(digest, path)
            frame(digest, content)
    finally:
        process.stdin.close()
        if process.wait() != 0:
            raise RuntimeError("git cat-file --batch failed")
    return digest.hexdigest()


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} GIT_SOURCE TREEISH", file=sys.stderr)
        return 2
    source = pathlib.Path(sys.argv[1]).resolve()
    print(canonical_tree_sha256(source, sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
