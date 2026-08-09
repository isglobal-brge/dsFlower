"""Trusted Tier-2 runner library (node-resident).

The researcher's uploaded app provides a narrow, reviewable interface (exfiltration-scanned
+ hash-verified at install, framework-agnostic):

    initial_arrays(cfg: dict, input_dim: int) -> list[np.ndarray]
        # starting global model parameters; the ServerApp (researcher's own machine) uses it
    local_update(global_arrays, X, y, cfg) -> list[np.ndarray]
        # train however you like on the node's PRIVATE data, return new parameters

PROCESS-ISOLATED TRUST BOUNDARY. On the data node the untrusted `local_update` is run in a
FRESH, separate interpreter (`egress_child.py`); the trusted PARENT never imports or executes
the upload. The child can only ever hand back plain numeric arrays (loaded with
allow_pickle=False), which the parent validates and then privatises itself -- clip the delta
to the C-ball and add RDP-calibrated Gaussian noise. So the upload cannot monkeypatch the DP
harness / NumPy / the RNG, cannot leak via a crash/traceback, and (with a sufficient
sandbox) cannot carry state across sample-and-aggregate blocks. DP parameters always come
from the server-written manifest, never from the app.

Mechanism selection is the NODE's automatic, server-authoritative decision -- never the
researcher's: the plain 2C output-perturbation floor universally, and the sample-and-aggregate
min(2C,4C/k) floor when (a) the platform provides a sandbox strong enough to GUARANTEE per-block
independence and (b) the custodian enables a fixed public block count.
"""

import base64
import json
import hashlib
import hmac
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))


def _trusted_import(name):
    """Load a co-located trusted node module by EXPLICIT path, bypassing sys.path order so
    a malicious entry earlier on sys.path cannot shadow it and execute code in the trusted
    parent. (Plain `import dp_harness` would honour sys.path; the upload's dir may be on it.)"""
    import importlib.util
    path = os.path.join(_HERE, name + ".py")
    private_name = "_dsftrusted_" + name
    existing = sys.modules.get(private_name)
    if existing is not None:
        if os.path.realpath(getattr(existing, "__file__", "")) != os.path.realpath(path):
            raise RuntimeError("trusted module name is already bound to another path")
        return existing
    spec = importlib.util.spec_from_file_location(private_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[private_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(private_name, None)
        raise
    return mod


seeding = _trusted_import("seeding")
dp_harness = _trusted_import("dp_harness")


_REQUIRED_HOOKS = ("initial_arrays", "local_update")
_CHILD = os.path.join(_HERE, "egress_child.py")
_DEFAULT_TIMEOUT = 900                 # wall-clock seconds per child run
_NPY_HEADER_SLACK = 4096               # bytes of .npy header allowed above the array payload
_PAD_GUARD = 5.0                       # public setup/cleanup margin after all child timeouts
_MISSING_PATIENT_UNIT = "__dsflower_missing_patient_unit__"
_ASCII_ID_TRIM = " \t\r\n"
_APP_PARAMS_MAX_DEPTH = 8
_APP_PARAMS_MAX_ITEMS = 2048
_APP_PARAMS_MAX_BYTES = 65536
_APP_PARAMS_MAX_KEY_BYTES = 128
_APP_PARAMS_MAX_STRING_BYTES = 4096
_PUBLIC_HOOK_CONFIG_KEYS = frozenset({
    "app_params", "round_index", "num_rounds", "task", "num_classes",
})
_RESERVED_APP_PARAM_KEYS = frozenset({
    "privacy", "dp", "epsilon", "delta", "clipping_norm",
    "user_module", "app_params", "app_params_b64", "app_params_sha256",
    "round", "round_index", "server_round", "num_rounds",
    "num_server_rounds", "task", "task_type", "num_classes",
    "runtime_profile", "backend", "requirements", "requirement",
    "dependencies", "dependency", "pip", "pythonpath", "python_path",
})
_PATH_SECURITY_KEY = re.compile(
    r"(^|_)(path|dir|directory|file|filename|secret|token|password|credential|"
    r"requirements?|dependencies?)($|_)")
_DP_KEY = re.compile(
    r"(^|_)(privacy|dp|epsilon|delta|noise|sensitivity|accountant|clip|clipping)($|_)")


def load_user_module(module_name):
    """Import the uploaded app + confirm the interface. SERVER/CLIENT-side ONLY (the
    ServerApp runs on the researcher's own machine, for initial_arrays). The data NODE never
    calls this -- it runs the upload out-of-process via gated_local_update."""
    import importlib
    mod = importlib.import_module(module_name)
    missing = [h for h in _REQUIRED_HOOKS if not callable(getattr(mod, h, None))]
    if missing:
        raise ValueError(
            "Uploaded Tier-2 app '%s' is missing required hook(s): %s. It must "
            "define initial_arrays(cfg, input_dim) and "
            "local_update(global_arrays, X, y, cfg)." % (module_name, ", ".join(missing))
        )
    return mod


def _as_f64_list(weights):
    return [np.asarray(w, dtype=np.float64) for w in weights]


def _load_expected_f64_npy(path, expected_shape):
    """Load one child result only after its bounded ``.npy`` header proves that
    the allocation is exactly the expected float64 tensor."""
    file_size = os.path.getsize(path)
    expected_shape = tuple(expected_shape)
    expected_payload = int(np.prod(expected_shape, dtype=np.int64)) * 8
    if file_size > expected_payload + _NPY_HEADER_SLACK:
        raise ValueError("child array file is too large")

    with open(path, "rb") as handle:
        prefix = handle.read(12)
        if len(prefix) < 10 or prefix[:6] != b"\x93NUMPY":
            raise ValueError("child array has an invalid npy header")
        version = (prefix[6], prefix[7])
        if version == (1, 0):
            header_length = int.from_bytes(prefix[8:10], "little")
        elif version == (2, 0):
            if len(prefix) < 12:
                raise ValueError("child array has a truncated npy header")
            header_length = int.from_bytes(prefix[8:12], "little")
        else:
            # np.save emits v1/v2 for the child's plain numeric arrays. Reject
            # other versions instead of asking a version-specific parser to read
            # an attacker-declared length.
            raise ValueError("unsupported child npy version")
        if header_length > _NPY_HEADER_SLACK:
            raise ValueError("child npy header is too large")

        handle.seek(0)
        parsed_version = np.lib.format.read_magic(handle)
        if parsed_version == (1, 0):
            shape, _fortran_order, dtype = np.lib.format.read_array_header_1_0(
                handle, max_header_size=_NPY_HEADER_SLACK)
        else:
            shape, _fortran_order, dtype = np.lib.format.read_array_header_2_0(
                handle, max_header_size=_NPY_HEADER_SLACK)
        if (tuple(shape) != expected_shape
                or np.dtype(dtype) != np.dtype(np.float64)
                or np.dtype(dtype).hasobject):
            raise ValueError("child array header does not match the expected tensor")
        if file_size != handle.tell() + expected_payload:
            raise ValueError("child array payload length is invalid")

        handle.seek(0)
        return np.load(handle, allow_pickle=False)


def _take_rows(D, idx):
    """Row-subset X or y by integer positions, preserving a pandas object if used."""
    if hasattr(D, "iloc"):
        return D.iloc[idx]
    return np.asarray(D)[idx]


def _reserved_app_param_key(key):
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    normalized = normalized.lower().replace("-", "_").replace(".", "_")
    return (normalized.startswith(("privacy", "dp"))
            or normalized in _RESERVED_APP_PARAM_KEYS
            or bool(_PATH_SECURITY_KEY.search(normalized))
            or bool(_DP_KEY.search(normalized)))


def _safe_utf8(value, label, max_bytes, allow_empty=False):
    if not isinstance(value, str):
        raise ValueError("%s must be a string" % label)
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise ValueError("%s must be valid UTF-8" % label) from exc
    if (not encoded and not allow_empty) or len(encoded) > int(max_bytes):
        raise ValueError("%s has an invalid byte length" % label)
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("%s contains control characters" % label)
    return value


def _validate_app_params_value(value, depth, state, top=False):
    if depth > _APP_PARAMS_MAX_DEPTH:
        raise ValueError("app_params exceeds the maximum nesting depth")
    state[0] += 1
    if state[0] > _APP_PARAMS_MAX_ITEMS:
        raise ValueError("app_params exceeds the maximum item count")

    if value is None or type(value) is bool or type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("app_params numeric values must be finite")
        return value
    if type(value) is str:
        text = _safe_utf8(
            value, "app_params string", _APP_PARAMS_MAX_STRING_BYTES,
            allow_empty=True)
        if ("/" in text or "\\" in text
                or re.match(r"^[A-Za-z]:", text)
                or text.startswith(("~/", "~\\"))):
            raise ValueError("app_params strings cannot contain filesystem paths")
        return text
    if type(value) is list:
        if top:
            raise ValueError("app_params must be a JSON object")
        return [
            _validate_app_params_value(item, depth + 1, state)
            for item in value
        ]
    if type(value) is dict:
        out = {}
        for key in sorted(value):
            safe_key = _safe_utf8(
                key, "app_params object key", _APP_PARAMS_MAX_KEY_BYTES)
            if "/" in safe_key or "\\" in safe_key:
                raise ValueError("app_params object keys cannot contain paths")
            if _reserved_app_param_key(safe_key):
                raise ValueError("app_params contains a reserved key")
            out[safe_key] = _validate_app_params_value(
                value[key], depth + 1, state)
        return out
    raise ValueError("app_params contains a non-JSON value")


def _decode_app_params(cfg):
    """Decode the exact server-pinned canonical public HookApp payload."""
    if not isinstance(cfg, dict):
        raise ValueError("HookApp run config must be an object")
    encoded = cfg.get("app-params-b64")
    expected_hash = cfg.get("app-params-sha256")
    max_b64 = 4 * ((_APP_PARAMS_MAX_BYTES + 2) // 3)
    if (not isinstance(encoded, str) or not encoded
            or len(encoded.encode("ascii", errors="ignore")) != len(encoded)
            or len(encoded) > max_b64):
        raise ValueError("missing or oversized canonical app-params-b64")
    if (not isinstance(expected_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None):
        raise ValueError("missing canonical app-params-sha256")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("app-params-b64 is not canonical base64") from exc
    if (len(raw) > _APP_PARAMS_MAX_BYTES
            or base64.b64encode(raw).decode("ascii") != encoded):
        raise ValueError("app-params-b64 is not canonical bounded base64")
    actual_hash = hashlib.sha256(raw).hexdigest()
    if not hmac.compare_digest(actual_hash, expected_hash):
        raise ValueError("app_params does not match the server-pinned hash")

    def reject_constant(value):
        raise ValueError("app_params numeric values must be finite: %s" % value)

    def unique_object(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("app_params object keys must be unique")
            out[key] = value
        return out

    try:
        parsed = json.loads(
            raw.decode("utf-8", errors="strict"),
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("app_params must be valid UTF-8 JSON") from exc
    if type(parsed) is not dict:
        raise ValueError("app_params must be a JSON object")
    return _validate_app_params_value(parsed, 0, [0], top=True)


def _public_int(value, label, lower, upper):
    if type(value) is not int or value < lower or value > upper:
        raise ValueError("%s must be an integer in [%d, %d]" % (
            label, lower, upper))
    return value


def public_hook_config(cfg, round_index):
    """Build the only configuration visible to uploaded hooks.

    ``round_index`` is supplied by the trusted release claim (zero is reserved
    for researcher-side initialisation); no analyst field can override it.
    """
    num_rounds = _public_int(
        cfg.get("num-server-rounds"), "num_rounds", 1, 500)
    round_value = _public_int(
        round_index, "round_index", 0, num_rounds)
    task = cfg.get("task-type")
    if task not in ("classification", "regression", "count"):
        raise ValueError("task must be classification, regression, or count")
    num_classes = _public_int(
        cfg.get("num-classes"), "num_classes", 2, 1024)
    return {
        "app_params": _decode_app_params(cfg),
        "round_index": round_value,
        "num_rounds": num_rounds,
        "task": task,
        "num_classes": num_classes,
    }


def _sanitize_cfg(cfg):
    """Revalidate and copy the exact public payload before crossing the child boundary."""
    if type(cfg) is not dict or set(cfg) != _PUBLIC_HOOK_CONFIG_KEYS:
        raise ValueError("HookApp child config has unknown or missing fields")
    rounds = _public_int(cfg["num_rounds"], "num_rounds", 1, 500)
    round_value = _public_int(cfg["round_index"], "round_index", 0, rounds)
    task = cfg["task"]
    if task not in ("classification", "regression", "count"):
        raise ValueError("invalid HookApp task")
    classes = _public_int(cfg["num_classes"], "num_classes", 2, 1024)
    if type(cfg["app_params"]) is not dict:
        raise ValueError("app_params must be a JSON object")
    return {
        "app_params": _validate_app_params_value(
            cfg["app_params"], 0, [0], top=True),
        "round_index": round_value,
        "num_rounds": rounds,
        "task": task,
        "num_classes": classes,
    }


# --------------------------------------------------------------------------- #
# Sandbox capability preflight (subprocess is universal; the rest is platform-gated)
# --------------------------------------------------------------------------- #

def _code_dirs():
    """Read-only dirs the child needs to import Python + numpy + the user module -- and
    NOTHING else (crucially NOT the node's data/manifest dir). Binding only these means a
    sample-and-aggregate child can read code + its OWN block input, never other blocks'
    records, which is exactly what the bounded-block sensitivity argument requires."""
    import sysconfig
    dirs = [sys.prefix, sys.exec_prefix, os.path.dirname(os.path.abspath(sys.executable))]
    try:
        paths = sysconfig.get_paths()
        dirs += [paths.get(k) for k in ("stdlib", "platstdlib", "purelib", "platlib")]
    except Exception:
        pass
    pinned = os.environ.get("DSFLOWER_PINNED_APP_DIR")
    if pinned:
        dirs.append(pinned)
    seen, out = set(), []
    for d in dirs:
        if d and d not in seen and os.path.isdir(d):
            out.append(d); seen.add(d)
    return out


def _pinned_user_package(module_name):
    """Re-hash the exact node-installed package immediately before execution."""
    if not module_name or not module_name.replace("_", "a").isalnum() or not (
            module_name[0].isalpha() or module_name[0] == "_"):
        raise RuntimeError("invalid pinned hook module name")
    root = os.environ.get("DSFLOWER_PINNED_APP_DIR", "")
    manifest_dir = os.environ.get("DSFLOWER_MANIFEST_DIR", "")
    if not root or not manifest_dir:
        raise RuntimeError("pinned hook paths are missing")
    root = os.path.realpath(root)
    pkg = os.path.realpath(os.path.join(root, module_name))
    if not pkg.startswith(root + os.sep) or not os.path.isdir(pkg):
        raise RuntimeError("pinned hook package is outside its installed root")
    init_file = os.path.join(pkg, "__init__.py")
    try:
        import stat
        if (os.path.realpath(init_file) != init_file
                or not stat.S_ISREG(os.lstat(init_file).st_mode)):
            raise RuntimeError
    except Exception as exc:
        raise RuntimeError(
            "pinned hook package needs a regular __init__.py") from exc
    with open(os.path.join(manifest_dir, "pinned_packages.json"), encoding="utf-8") as fh:
        pins = json.load(fh)
    expected = str(pins.get(module_name, ""))
    if not expected:
        raise RuntimeError("hook package is not present in the node pin map")
    digest = hashlib.sha256()
    entries = []
    for current, dirs, files in os.walk(pkg):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for filename in files:
            if filename.endswith((".pyc", ".pyo")):
                continue
            full = os.path.join(current, filename)
            rel = os.path.relpath(full, pkg).replace(os.sep, "/")
            entries.append((rel, full))
    for rel, full in sorted(entries):
        with open(full, "rb") as fh:
            content = fh.read()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\n")
        digest.update(content)
        digest.update(b"\x00")
    if not hmac.compare_digest(digest.hexdigest(), expected):
        raise RuntimeError("node-installed hook package changed after verification")
    return init_file


def _bwrap_mount(path, td):
    """A MINIMAL bubblewrap sandbox: fresh tmpfs root, network unshared, ONLY the code dirs
    bound read-only and ONLY `td` (this block's input/output) writable. No host root, no data
    dir."""
    args = [path, "--unshare-all", "--die-with-parent",
            "--tmpfs", "/", "--proc", "/proc", "--dev", "/dev", "--bind", td, td]
    for d in _code_dirs():
        args += ["--ro-bind", d, d]
    return args


def _bwrap_works(path):
    """True iff bubblewrap can ACTUALLY run our minimal net+fs sandbox here (userns enabled)
    AND a fresh interpreter can still import numpy inside it. Tests the real sandbox we would
    enforce -- not a permissive one -- so capabilities never over-report."""
    try:
        d = tempfile.mkdtemp(prefix="dsf_bwtest_")
        try:
            cmd = _bwrap_mount(path, d) + ["--", sys.executable, "-I", "-c", "import numpy"]
            r = subprocess.run(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=30,
                               env={"PATH": "/usr/bin:/bin", "TMPDIR": d})
            return r.returncode == 0
        finally:
            shutil.rmtree(d, ignore_errors=True)
    except Exception:
        return False


def sandbox_caps():
    """What isolation the platform provides RIGHT NOW. subprocess + parent-side DP (the
    monkeypatch fix) is universal; cross-block net+fs isolation (needed to make
    sample-and-aggregate sound against malicious code) is reported only where the minimal
    bubblewrap sandbox is VERIFIED to run."""
    caps = {"subprocess": True, "linux": sys.platform.startswith("linux"),
            "rlimit": False, "bwrap": None, "net_lock": False, "fs_isolation": False}
    try:
        import resource  # noqa: F401
        caps["rlimit"] = True
    except Exception:
        pass
    tool = shutil.which("bwrap")
    if tool and _bwrap_works(tool):
        caps["bwrap"] = tool
        caps["net_lock"] = True       # --unshare-all removes the network namespace
        caps["fs_isolation"] = True   # minimal mount: code + this block only, not other data
    return caps


def _full_sandbox_ok(caps):
    """Sample-and-aggregate's multi-block bound needs GUARANTEED independence (no
    cross-block state via shared network OR filesystem). It is enabled ONLY when the minimal
    net+fs sandbox is verified AND the custodian has attested (DSF_SAA_SANDBOX_OK=1) that, on
    THIS host, that sandbox exposes only per-block input + code -- never other records. So SAA
    stays OFF by default (sound) and can never auto-enable on an unvetted host; the plain 2C
    floor (which needs only process isolation) is the universal mechanism."""
    return bool(caps.get("subprocess") and caps.get("net_lock") and caps.get("fs_isolation")
                and os.environ.get("DSF_SAA_SANDBOX_OK") == "1")


def _resource_isolation_ok():
    """The node operator attests externally enforced cgroup/quota isolation."""
    return os.environ.get("DSF_HOOK_RESOURCE_ISOLATION_OK") == "1"


def hook_execution_caps(pcfg, caps=None):
    """Return caps iff every sandbox, resource and timing gate is attested."""
    caps = sandbox_caps() if caps is None else caps
    pad_to = float(pcfg.get("egress_time_pad", 0))
    if (not bool(pcfg.get("hook_enabled", False))
            or not _full_sandbox_ok(caps)
            or not _resource_isolation_ok()
            or pad_to < hook_required_time_pad(pcfg)):
        return None
    return caps


def hook_required_time_pad(pcfg):
    """Public minimum envelope covering every sequential Hook child."""
    timeout = int(pcfg.get("egress_timeout", _DEFAULT_TIMEOUT))
    k = (max(2, min(64, int(pcfg.get("sa_blocks", 8))))
         if bool(pcfg.get("sample_aggregate", True)) else 1)
    return float(k * timeout) + _PAD_GUARD


def pad_hook_release(release_started, pcfg):
    """Complete the administrator-pinned minimum-duration release envelope."""
    started = float(release_started)
    pad_to = float(pcfg.get("egress_time_pad", 0))
    if not math.isfinite(started) or not math.isfinite(pad_to) or pad_to < 0:
        raise RuntimeError("invalid public Hook timing envelope")
    remaining = pad_to - (time.monotonic() - started)
    if remaining > 0:
        time.sleep(remaining)


def _wrap_sandbox(cmd, caps, td):
    """Run the child inside the VERIFIED minimal bubblewrap sandbox when SAA is enabled; else
    run it plain (the parent still applies ALL DP; network is best-effort via the child socket
    neuter + the container egress policy as the production boundary)."""
    if caps.get("bwrap") and _full_sandbox_ok(caps):
        return _bwrap_mount(caps["bwrap"], td) + ["--", *cmd]
    return cmd


def _killpg(pgid):
    """SIGKILL a whole process group by its cached pgid (== the child pid under
    start_new_session). Cached so it works even after wait() has reaped the session leader,
    so a backgrounded grandchild a malicious update spawned cannot survive."""
    try:
        os.killpg(pgid, signal.SIGKILL)
    except Exception:
        pass


def _is_regular(path):
    """True iff path is an existing REGULAR file (lstat, so NOT a symlink, FIFO, device, or
    dir). A backgrounded helper that escaped the process group could otherwise drop a FIFO /
    symlink-to-/dev/zero where a result file is expected and hang or OOM the parent on read."""
    try:
        import stat
        return stat.S_ISREG(os.lstat(path).st_mode)
    except Exception:
        return False


def _run_isolated(module_name, module_file, old, X, y, cfg, pcfg, caps, timeout):
    """Run the untrusted local_update on (X, y) in a FRESH interpreter. Returns f64 arrays,
    or None on ANY failure (crash, timeout, wrong count/shape, non-finite, unreadable). The
    parent never imports or executes the upload; the result is loaded allow_pickle=False so
    it can never execute code here. Every child retains its own fixed timeout; release-level
    minimum-duration padding is applied once by ``gated_local_update``."""
    td = tempfile.mkdtemp(prefix="dsf_egress_")
    try:
        inp = os.path.join(td, "in.npz")
        outd = os.path.join(td, "out")
        cfgf = os.path.join(td, "cfg.json")
        np.savez(inp, **{("g_%03d" % i): np.asarray(o, np.float64) for i, o in enumerate(old)},
                 X=np.asarray(X), y=np.asarray(y))
        with open(cfgf, "w") as f:
            json.dump(_sanitize_cfg(cfg), f)
        base = [sys.executable, "-I", "-B", _CHILD,
                "--in", inp, "--out", outd, "--cfg", cfgf,
                "--module", str(module_name), "--module-file", module_file]
        mib = 1024 * 1024
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "TMPDIR": td,
            "DSF_RLIMIT_CPU": str(int(timeout)),
            "DSF_RLIMIT_AS": str(int(pcfg.get("egress_memory_mb", 8192)) * mib),
            "DSF_RLIMIT_FSIZE": str(int(pcfg.get("egress_file_mb", 1024)) * mib),
            "DSF_RLIMIT_NPROC": str(int(pcfg.get("egress_processes", 128))),
            "DSF_NO_NET": "1",
        }
        cmd = _wrap_sandbox(base, caps, td)
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, env=env, start_new_session=True)
        except Exception:
            return None
        pgid = p.pid   # == process-group id (start_new_session); cache BEFORE wait reaps it
        try:
            p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
        finally:
            _killpg(pgid)   # kill the whole group -> reaps any backgrounded grandchild
        if p.returncode != 0:
            return None
        # Parse defensively: exact count FIRST, then validate each bounded .npy
        # header, dtype, shape and payload length BEFORE np.load can allocate.
        # Everything is in one try so ANY error -> None -> a zero delta.
        try:
            okf = os.path.join(outd, "_ok")
            if not _is_regular(okf) or os.path.getsize(okf) > 64:   # bounded, regular-file only
                return None
            with open(okf) as f:
                if int(f.read(64)) != len(old):                     # exact count BEFORE any load
                    return None
            res = []
            for i, o in enumerate(old):
                wf = os.path.join(outd, "w_%d.npy" % i)
                if not _is_regular(wf):                             # no FIFO/symlink/device
                    return None
                res.append(np.asarray(
                    _load_expected_f64_npy(wf, o.shape), np.float64))
            return res
        except Exception:
            return None
    finally:
        shutil.rmtree(td, ignore_errors=True)


def _validate(res, old):
    """Authoritative parent-side check: exact array count + shapes matching `old`, all
    finite. Anything else -> None (caller maps to a zero delta)."""
    if res is None or len(res) != len(old):
        return None
    for a, o in zip(res, old):
        if a.shape != o.shape or not bool(np.all(np.isfinite(a))):
            return None
    return res


def _choose_blocks(pcfg, full_sandbox):
    """Select only the fixed custodian-pinned S&A mechanism.

    The block count must be independent of private row/patient counts. Otherwise
    neighbouring datasets can receive Gaussian distributions with different
    variances, whose likelihood ratio is unbounded. The switch and fixed k are
    server policy, never researcher inputs.
    """
    if not full_sandbox:
        return 1
    if not bool(pcfg.get("sample_aggregate", True)):
        return 1
    return max(2, min(64, int(pcfg.get("sa_blocks", 8))))


def _canonical_patient_id(value):
    try:
        text = ("" if value is None else str(value)).strip(_ASCII_ID_TRIM)
        text.encode("utf-8", errors="strict")
    except (TypeError, UnicodeError, ValueError):
        return _MISSING_PATIENT_UNIT
    if (not text
            or text.lower() in ("na", "nan", "null", "<na>", "nat")):
        return _MISSING_PATIENT_UNIT
    return text


def _patient_row_blocks(unit_ids, n_rows, k, partition_seed):
    """Assign every row for one privacy unit to exactly one S&A block.

    Assignment is a keyed, data-independent hash of the server-pinned patient
    identifier.  It does not depend on row values, row multiplicity, or encounter
    order, so changing one patient's records cannot reshuffle other patients.
    Empty blocks are retained: the caller maps a failed/empty block update to zero,
    preserving the fixed ``k`` denominator and bounded sensitivity.
    """
    ids = np.asarray(unit_ids, dtype=object)
    if ids.ndim != 1 or len(ids) != int(n_rows):
        raise RuntimeError("patient identifiers must be one-dimensional and match X")
    if not isinstance(partition_seed, (bytes, bytearray)) or len(partition_seed) != 32:
        raise RuntimeError("patient partitioning requires a 256-bit release seed")

    blocks = [[] for _ in range(int(k))]
    assigned = {}
    for row_index, raw in enumerate(ids.tolist()):
        patient_id = _canonical_patient_id(raw)
        if patient_id not in assigned:
            digest = hmac.new(
                bytes(partition_seed),
                b"dsflower/patient-block/v1\x00"
                + patient_id.encode("utf-8"),
                hashlib.sha256,
            ).digest()
            assigned[patient_id] = int.from_bytes(digest[:8], "big") % int(k)
        blocks[assigned[patient_id]].append(row_index)
    return [np.asarray(rows, dtype=np.int64) for rows in blocks], len(assigned)


def gated_local_update(module_name, global_arrays, X, y, cfg, pcfg, seed=None,
                       hook_caps=None, unit_ids=None, release_started=None,
                       pad_release=True):
    """Run the upload out-of-process from the global model, then apply the DP gate in the
    trusted parent. The NODE picks the mechanism: sample-and-aggregate
    (conservative sensitivity min(2C,4C/k)) when the
    platform can guarantee block independence and policy says so, else the plain 2C floor.
    `module_name` (not an imported module) is passed so the parent never imports the upload.

    The first argument is the module NAME (str) -- never an imported object: the node must
    not import the upload in its own process."""
    if not isinstance(module_name, str):
        raise TypeError("gated_local_update requires the module NAME (str); the node never "
                        "imports the untrusted upload in-process")
    old = _as_f64_list(global_arrays)
    n = int(len(X))
    if int(len(y)) != n:
        raise RuntimeError("X and y must contain the same number of rows")
    caps = hook_execution_caps(pcfg, hook_caps)
    timeout = int(pcfg.get("egress_timeout", _DEFAULT_TIMEOUT))
    # Arbitrary hooks have filesystem/network/resource/timing channels outside
    # the numeric DP gate. Without every operator-attested control, do not touch
    # private data and complete with a data-independent unchanged model.
    if caps is None:
        return [o.astype(np.float32) for o in old]
    release_started = (time.monotonic() if release_started is None
                       else float(release_started))
    try:
        module_file = _pinned_user_package(module_name)
        partition_seed = seeding.sub_seed(seed, "partition")
        if unit_ids is None:
            n_units = n
            row_blocks = None
        else:
            # Validate/group before choosing k. First obtain the unique-unit
            # count with k=1; assignment is recomputed for the fixed final k.
            _, n_units = _patient_row_blocks(unit_ids, n, 1, partition_seed)
            row_blocks = None
        k = _choose_blocks(pcfg, True)

        if k >= 2:
            # A patient privacy unit may span many rows. Keep all those rows in
            # one independently sandboxed block so one neighbour changes at
            # most one block output.
            if unit_ids is None:
                perm = seeding.np_rng(partition_seed).permutation(n)
                row_blocks = np.array_split(perm, k)
            else:
                row_blocks, _ = _patient_row_blocks(
                    unit_ids, n, k, partition_seed)
            block_updates = []
            for idx in row_blocks:
                r = _validate(_run_isolated(
                    module_name, module_file, old, _take_rows(X, idx),
                    _take_rows(y, idx), cfg, pcfg, caps, timeout), old)
                block_updates.append(
                    r if r is not None else [o.copy() for o in old])
            gated = dp_harness.sample_and_aggregate(
                block_updates, old,
                clipping_norm=pcfg["clipping_norm"],
                epsilon=pcfg["epsilon"],
                delta=pcfg["delta"],
                num_releases=pcfg.get("composition_releases", 1),
                rng=seeding.np_rng(seeding.sub_seed(seed, "noise")),
            )
        else:
            r = _validate(_run_isolated(
                module_name, module_file, old, X, y, cfg, pcfg, caps, timeout), old)
            new = r if r is not None else [o.copy() for o in old]
            gated = dp_harness.output_perturbation(
                new, old,
                clipping_norm=pcfg["clipping_norm"],
                epsilon=pcfg["epsilon"],
                delta=pcfg["delta"],
                num_releases=pcfg.get("composition_releases", 1),
                rng=seeding.np_rng(seeding.sub_seed(seed, "noise")),
            )
        return [g.astype(np.float32) for g in gated]
    finally:
        # One minimum-duration envelope covers the complete release, including
        # all k S&A children. Each child still has its independent timeout.
        if pad_release:
            pad_hook_release(release_started, pcfg)
