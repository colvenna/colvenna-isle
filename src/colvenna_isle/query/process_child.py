"""Linux bootstrap before importing native libraries; invoked only by process.run."""

import ctypes
import os
import resource
import signal
import sys


def main():
    parent, maximum_bytes = int(sys.argv[1]), int(sys.argv[2])
    # Do not use preexec_fn: the query HTTP service has concurrent threads.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise OSError(ctypes.get_errno(), "Cannot establish parent-death custody")
    if os.getppid() != parent:
        raise SystemExit(70)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (maximum_bytes, maximum_bytes))
    os.execvpe(sys.argv[3], sys.argv[3:], os.environ)


if __name__ == "__main__":
    main()
