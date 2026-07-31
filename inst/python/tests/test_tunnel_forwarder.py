"""Lifecycle regressions for the node-side DSI tunnel forwarder."""

import os
import sys
import tempfile
import time
import unittest

REPO_ROOT = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from inst.python import dsi_tunnel_forward  # noqa: E402


class TunnelForwarderLifecycleTests(unittest.TestCase):
    def test_missing_or_stale_heartbeat_is_not_alive(self):
        with tempfile.TemporaryDirectory() as root:
            heartbeat = os.path.join(root, "relay_hb")
            self.assertFalse(dsi_tunnel_forward.relay_alive(heartbeat))

            with open(heartbeat, "w", encoding="ascii") as handle:
                handle.write(".")
            self.assertTrue(dsi_tunnel_forward.relay_alive(heartbeat))

            stale = time.time() - dsi_tunnel_forward.RELAY_TTL - 1.0
            os.utime(heartbeat, (stale, stale))
            self.assertFalse(dsi_tunnel_forward.relay_alive(heartbeat))


if __name__ == "__main__":
    unittest.main()
