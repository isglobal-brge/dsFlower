#!/usr/bin/env python3
"""Validate and extract one uploaded Flower FAB without trusting archive paths."""

import argparse
import json
import os
import re
import shutil
import stat
import sys
import zipfile


_DRIVE = re.compile(r"^[A-Za-z]:")
_MAX_FILES = 10000


def _result(ok, error=""):
    print(json.dumps({"ok": bool(ok), "error": str(error)}))


def _entry_parts(info):
    name = info.filename
    if not name or "\x00" in name or "\\" in name:
        raise ValueError("invalid archive member name")
    if name.startswith("/") or name.startswith("//") or _DRIVE.match(name):
        raise ValueError("absolute archive member path")

    is_dir = info.is_dir() or name.endswith("/")
    parts = name.split("/")
    if is_dir and parts and parts[-1] == "":
        parts.pop()
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise ValueError("traversing archive member path")

    mode = (info.external_attr >> 16) & 0xFFFF
    kind = stat.S_IFMT(mode)
    if stat.S_ISLNK(mode):
        raise ValueError("symbolic link archive member")
    if kind and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
        raise ValueError("non-regular archive member")
    if info.flag_bits & 0x1:
        raise ValueError("encrypted archive member")
    return parts, is_dir


def _extract(archive, destination, max_bytes):
    if os.path.lexists(destination):
        raise ValueError("destination already exists")
    os.makedirs(destination, mode=0o700)
    root = os.path.realpath(destination)

    with zipfile.ZipFile(archive, "r") as zf:
        infos = zf.infolist()
        if not infos or len(infos) > _MAX_FILES:
            raise ValueError("invalid archive member count")

        entries = []
        seen = set()
        declared_total = 0
        for info in infos:
            parts, is_dir = _entry_parts(info)
            relative = "/".join(parts)
            if relative in seen:
                raise ValueError("duplicate archive member path")
            seen.add(relative)
            if info.file_size < 0:
                raise ValueError("invalid archive member size")
            declared_total += int(info.file_size)
            if declared_total > max_bytes:
                raise ValueError("unpacked archive exceeds size limit")
            entries.append((info, parts, is_dir))

        actual_total = 0
        for info, parts, is_dir in entries:
            target = os.path.join(root, *parts)
            parent = target if is_dir else os.path.dirname(target)
            os.makedirs(parent, mode=0o700, exist_ok=True)
            if os.path.commonpath((root, os.path.realpath(parent))) != root:
                raise ValueError("archive member escapes destination")
            if is_dir:
                continue

            written = 0
            with zf.open(info, "r") as source, open(target, "xb") as output:
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    actual_total += len(chunk)
                    if written > info.file_size or actual_total > max_bytes:
                        raise ValueError("unpacked archive exceeds size limit")
                    output.write(chunk)
            if written != info.file_size:
                raise ValueError("archive member size mismatch")
            os.chmod(target, 0o600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--max-bytes", required=True, type=int)
    args = parser.parse_args()

    try:
        if args.max_bytes < 1:
            raise ValueError("invalid size limit")
        _extract(args.archive, args.destination, args.max_bytes)
    except Exception as exc:
        shutil.rmtree(args.destination, ignore_errors=True)
        _result(False, exc)
        return 1
    _result(True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
