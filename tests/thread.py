import logging
import signal
import subprocess
import traceback

import psutil
from retry import retry

from .base import ProcessReader
from .common import ServerClass, is_debug

log = logging.getLogger(__name__)

# ThreadServer will attempt to kill all child processes recursively.
KILL_RETRY_COUNT = 15  # Total retry count to kill if not all child processes are terminated.
KILL_RETRY_WAIT_SECS = 1  # Wait time between two retries
KILL_WAIT_SECS = 5  # Time to wait for processes to terminate in a single retry.

try:
    SIG_KILL = signal.SIGKILL
except AttributeError:
    SIG_KILL = signal.SIGINT


class ProcessStillRunningException(Exception):
    pass


@retry(ProcessStillRunningException, tries=KILL_RETRY_COUNT, delay=KILL_RETRY_WAIT_SECS)
def _kill_all(procs, sig):
    log.debug(f"Killing {len(procs)} processes with signal {sig}")
    for p in procs:
        p.send_signal(sig)

    log.debug(f"Waiting for {len(procs)} processes to die")
    gone, alive = psutil.wait_procs(procs, timeout=KILL_WAIT_SECS)

    if len(alive) == 0:
        log.debug("All processes are terminated")
        return

    log.warning(f"{len(alive)} processes remainings: {','.join([p.name() for p in alive])}")
    raise ProcessStillRunningException()


def _kill_proc_tree(pid, sig=SIG_KILL, timeout=None):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    children.append(parent)
    log.debug("Killing process tree for %d (total_procs_to_kill=%d)" % (parent.pid, len(children)))
    _kill_all(children, sig)


class ThreadServer(ServerClass):
    """Thread server class."""

    def __init__(self, cmd, get_args, env, workspace, cwd=None, listen_hostname=None):
        super(ThreadServer, self).__init__(cmd, get_args, env)

        self.exit = False
        self._workspace = workspace
        self._cwd = cwd
        self._hostname = listen_hostname
        self._proc = None

    def launch(self):
        log.debug("Launching thread server.")

        run_cmd = [self._cmd] + self._get_args(workspace=self._workspace)

        debug = is_debug()

        extra_args = dict()
        if debug:
            extra_args["stdout"] = subprocess.PIPE
            extra_args["stderr"] = subprocess.PIPE

        print(run_cmd)
        print(self._env)
        print(self._cwd)

        self._proc = subprocess.Popen(run_cmd) #, env=self._env, cwd=self._cwd
        log.debug(f"Running server: {' '.join(run_cmd)}")
        log.debug(f"CWD: {self._cwd}")

        if debug:
            ProcessReader(self._proc, self._proc.stdout, False).start()
            ProcessReader(self._proc, self._proc.stderr, True).start()

        self.start()

    def run(self):
        """Run in thread"""
        try:
            self._proc.wait()
        except OSError:
            if not self.exit:
                traceback.print_exc()

    @property
    def is_running(self):
        """Check if the main process is still running."""
        # return False if the process is not started yet
        if not self._proc:
            return False
        # return False if there is a return code from the main process
        return self._proc.poll() is None

    @property
    def hostname(self):
        return self._hostname

    def teardown(self):
        if not self._proc:
            log.warning("No process is running, skip teardown.")
            return

        _kill_proc_tree(self._proc.pid)
        self._proc = None
