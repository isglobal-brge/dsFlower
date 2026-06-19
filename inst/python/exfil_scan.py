"""dsFlower Tier-2 validation: static exfiltration/escape scan (node-side, trusted).

Per ARCHITECTURE.md §8, a Tier-2 (arbitrary uploaded app) is validated ONLY for
containment — NOT for DP-SGD internals. The app trains itself and the egress gate
carries DP via output perturbation, so it may call `.backward`, read the staged
data (`open` on the data mount), etc. This scan rejects only clear
exfiltration / process-escape / dynamic-code constructs.

This is HYGIENE / defence-in-depth, not the enforcement boundary (a static scan
cannot certify arbitrary code — Rice's theorem). The real controls are the sandbox
(no net, restricted FS) and the egress gate. The scan closes trivial bypasses and
gives a clear rejection reason before any data is touched.

Usage:
    python exfil_scan.py <package_dir>     # JSON {"ok": bool, "violations": [...]}
or import scan_tree(dir) -> list[dict(file, line, rule, detail)].
"""

import ast
import json
import os
import sys


# Imports an uploaded app may not use: network egress, process/FS escape, and
# deserialization that executes code. The sandbox also blocks the effects; this
# rejects them early with a clear reason. NOTE: numeric/ML libs (torch, numpy,
# pandas, sklearn, math, json, typing, etc.) and `open` are intentionally allowed
# — a Tier-2 app legitimately trains and reads the staged data.
_FORBIDDEN_IMPORTS = {
    "subprocess", "socket", "ctypes", "cffi", "requests", "urllib", "urllib2",
    "urllib3", "http", "httplib", "ftplib", "smtplib", "telnetlib", "paramiko",
    "pickle", "cPickle", "marshal", "dill", "pty", "fcntl", "mmap",
    "multiprocessing", "subprocess32", "asyncssh", "pexpect",
}
# Dynamic-code builtins (the trivial "exec a string" escape).
_FORBIDDEN_CALLS = {"eval", "exec", "compile", "__import__"}


def _import_roots(node):
    if isinstance(node, ast.Import):
        return [a.name.split(".")[0] for a in node.names]
    if isinstance(node, ast.ImportFrom):
        if node.level and node.level > 0:
            return []  # relative import within the app package: allowed
        return [(node.module or "").split(".")[0]]
    return []


# os is allowed for path/env reads but os.system/os.exec*/os.popen/os.fork are escapes.
_FORBIDDEN_OS_ATTRS = {
    "system", "popen", "fork", "forkpty", "spawn", "spawnl", "spawnv",
    "execl", "execv", "execve", "execvp", "exec", "putenv", "remove",
    "unlink", "rmdir", "removedirs",
}


def scan_source(src, filename):
    violations = []

    def add(line, rule, detail):
        violations.append({"file": filename, "line": int(line or 0),
                           "rule": rule, "detail": detail})

    try:
        tree = ast.parse(src, filename=filename)
    except SyntaxError as e:
        add(e.lineno, "syntax-error", str(e))
        return violations

    for node in ast.walk(tree):
        for root in _import_roots(node):
            if root in _FORBIDDEN_IMPORTS:
                add(node.lineno, "forbidden-import", root)

        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in _FORBIDDEN_CALLS:
            add(node.lineno, "forbidden-call", node.func.id)

        # os.<escape>(...) — os itself is allowed, these attributes are not.
        if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_OS_ATTRS \
                and isinstance(node.value, ast.Name) and node.value.id == "os":
            add(node.lineno, "os-escape", "os." + node.attr)

        # Dunder reflection used to break out of the sandbox
        # (e.g. ().__class__.__bases__[0].__subclasses__()).
        if isinstance(node, ast.Attribute) and node.attr in (
                "__subclasses__", "__bases__", "__mro__", "__globals__",
                "__builtins__", "__code__", "__reduce__", "__reduce_ex__"):
            add(node.lineno, "dunder-reflection", node.attr)

    return violations


def scan_tree(pkg_dir):
    violations = []
    for root, dirs, files in os.walk(pkg_dir):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in sorted(files):
            if not f.endswith(".py"):
                continue
            full = os.path.join(root, f)
            rel = os.path.relpath(full, pkg_dir)
            try:
                with open(full, "r", encoding="utf-8") as fh:
                    src = fh.read()
            except OSError as e:
                violations.append({"file": rel, "line": 0,
                                   "rule": "unreadable", "detail": str(e)})
                continue
            violations.extend(scan_source(src, rel))
    return violations


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "."
    v = scan_tree(target)
    print(json.dumps({"ok": len(v) == 0, "violations": v}))
