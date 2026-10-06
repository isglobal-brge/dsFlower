"""Isolated ephemeral node state for fixed-public-data availability probes."""
from contextlib import contextmanager
import os
import tempfile


@contextmanager
def synthetic_node():
    """Keep synthetic probe releases out of the custodian's retained history."""
    names = ("DSFLOWER_NODE_SECRET_FILE", "DSFLOWER_NEIGHBOURHOOD_DIR",
             "DSFLOWER_NEIGHBOURHOOD_STORE_ID", "DSFLOWER_NEIGHBOURHOOD_K",
             "DSFLOWER_NEIGHBOURHOOD_MAX_ANCHORS", "DSFLOWER_NEIGHBOURHOOD_STORE_BYTES")
    previous = {name: os.environ.get(name) for name in names}
    with tempfile.TemporaryDirectory(prefix="dsflower-public-probe-") as directory:
        secret = os.path.join(directory, "node-secret")
        descriptor = os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as handle:
            handle.write("00" * 32 + "\n")
        try:
            for name in names:
                os.environ.pop(name, None)
            os.environ["DSFLOWER_NODE_SECRET_FILE"] = secret
            yield
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
