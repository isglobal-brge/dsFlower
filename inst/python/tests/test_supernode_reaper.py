"""Process-group cleanup regressions for the SuperNode wrapper."""

import os
import signal
import sys
import unittest
from unittest import mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from inst.python import supernode_reaper


class SuperNodeReaperTests(unittest.TestCase):
    def test_echild_does_not_hide_a_live_descendant_process_group(self):
        with (mock.patch.object(supernode_reaper, "_set_child_subreaper"),
              mock.patch.object(supernode_reaper.os, "fork", return_value=4242),
              mock.patch.object(supernode_reaper.signal, "signal"),
              mock.patch.object(
                  supernode_reaper, "_reap_available",
                  side_effect=[([4242], False), ([], False)]),
              mock.patch.object(
                  supernode_reaper, "_process_group_exists",
                  side_effect=[True, False]),
              mock.patch.object(
                  supernode_reaper.time, "monotonic",
                  side_effect=[0.0, supernode_reaper._GRACE_SECS + 1.0]),
              mock.patch.object(supernode_reaper.time, "sleep"),
              mock.patch.object(supernode_reaper.os, "killpg") as killpg,
              mock.patch.object(
                  supernode_reaper.sys, "argv", ["reaper", "supernode"])):
            self.assertEqual(supernode_reaper.main(), 0)

        self.assertEqual(killpg.call_args_list, [
            mock.call(4242, signal.SIGTERM),
            mock.call(4242, signal.SIGKILL),
        ])

    def test_process_group_probe_distinguishes_absent_from_forbidden(self):
        with mock.patch.object(
                supernode_reaper.os, "killpg",
                side_effect=ProcessLookupError):
            self.assertFalse(supernode_reaper._process_group_exists(4242))
        with mock.patch.object(
                supernode_reaper.os, "killpg",
                side_effect=PermissionError):
            self.assertTrue(supernode_reaper._process_group_exists(4242))


if __name__ == "__main__":
    unittest.main()
