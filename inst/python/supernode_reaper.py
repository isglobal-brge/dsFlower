"""Subreaper wrapper for flower-supernode — kills zombie accumulation at the source.

WHY THIS EXISTS
---------------
The rock/Opal container's PID 1 is the JVM (Opal), NOT a reaping init (tini). A JVM
never waitpid()s reparented orphans, so ANY process that is orphaned to PID 1 becomes
a PERMANENT zombie (a zombie can only be cleared by its parent calling wait(); kill -9
does nothing to it). flower-supernode spawns a `flwr-clientapp` subprocess per run; when
the supernode is killed by PID (our cleanup, esp. SIGKILL) its clientapp is orphaned to
PID 1 and zombifies forever. ~hundreds accumulated this way.

WHAT THIS DOES
--------------
Run flower-supernode UNDER this wrapper:  python supernode_reaper.py flower-supernode <args...>

  * prctl(PR_SET_CHILD_SUBREAPER): orphaned DESCENDANTS reparent to THIS wrapper instead
    of PID 1, so we — not the non-reaping JVM — become responsible for them and can reap.
  * the supernode runs in its OWN session/group (child setsid), so we can signal the whole
    group (supernode + clientapp) without ever signalling ourselves.
  * a waitpid(-1) reap loop continuously reaps the supernode AND any reparented clientapp,
    so nothing is ever left defunct.
  * on SIGTERM/SIGINT (our cleanup signalling the wrapper) we SIGTERM the supernode group,
    keep reaping until it drains, then escalate to SIGKILL of the group (never ourselves)
    and reap the remains before exiting.

Net effect: no FL descendant can reach the JVM PID 1, so zombies cannot accumulate even
under SIGKILL of the supernode. The manager must signal the WRAPPER (not the bare
supernode) and give it a moment to drain.
"""
import ctypes
import os
import signal
import sys
import time

PR_SET_CHILD_SUBREAPER = 36
_GRACE_SECS = float(os.environ.get("DSF_REAPER_GRACE", "12"))


def _set_child_subreaper():
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    except Exception:
        pass  # non-Linux / no libc: degrade to a plain reap loop (still better than nothing)


def _reap_available():
    """Reap every child whose status is ready. Returns (reaped_pids, any_left)."""
    reaped = []
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return reaped, False          # no children remain
        if pid == 0:
            return reaped, True           # children exist but none ready yet
        reaped.append(pid)


def main():
    cmd = sys.argv[1:]
    if not cmd:
        sys.stderr.write("supernode_reaper: no command given\n")
        return 2

    _set_child_subreaper()

    sn_pid = os.fork()
    if sn_pid == 0:
        # Child: lead a NEW session/group so the manager can killpg the supernode
        # subtree without ever touching the wrapper, then exec the real supernode.
        try:
            os.setsid()
        except OSError:
            pass
        try:
            os.execvp(cmd[0], cmd)
        except Exception as e:
            sys.stderr.write("supernode_reaper: exec failed: %s\n" % e)
            os._exit(127)
    sn_pgid = sn_pid                       # setsid() makes the child its own pgid

    state = {"shutdown": False, "deadline": None, "killed": False, "sn_rc": None}

    def _begin_shutdown(_signum=None, _frame=None):
        if not state["shutdown"]:
            state["shutdown"] = True
            state["deadline"] = time.time() + _GRACE_SECS
            try:
                os.killpg(sn_pgid, signal.SIGTERM)   # the supernode group, never us
            except OSError:
                pass

    signal.signal(signal.SIGTERM, _begin_shutdown)
    signal.signal(signal.SIGINT, _begin_shutdown)

    while True:
        reaped, any_left = _reap_available()
        if sn_pid in reaped and state["sn_rc"] is None:
            state["sn_rc"] = 0
            _begin_shutdown()              # supernode gone -> drain any lingering clientapp
        if not any_left:
            break                          # everything reaped; we're done
        if state["shutdown"] and not state["killed"] and time.time() > state["deadline"]:
            state["killed"] = True
            try:
                os.killpg(sn_pgid, signal.SIGKILL)   # force the group (not the wrapper)
            except OSError:
                pass
        time.sleep(0.2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
