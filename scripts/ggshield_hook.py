#!/usr/bin/env python3
"""pre-commit hook: `ggshield_hook.py pre-commit` scans the staged changes, `ggshield_hook.py pre-push` the
commits about to be pushed (in a public repository a push publishes them). Either runs the ggshield on
PATH only at the version runtime/ggshield-requirements.lock pins (CONTRIBUTING.md#secret-scanning)."""

import os
import shutil
import sys

from ggshield_pin import installed_version, pinned_version

MODES = ("pre-commit", "pre-push")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode not in MODES:
        sys.exit(f"usage: ggshield_hook.py {{{','.join(MODES)}}} [hook arguments]")
    binary = shutil.which("ggshield")
    want, have = pinned_version(), installed_version(binary)
    if have != want:
        sys.exit(f"the ggshield hook needs ggshield {want} on PATH, installed from runtime/ggshield-requirements.lock "
                 f"(found: {have}); see CONTRIBUTING.md#secret-scanning")
    os.execv(binary, [binary, "secret", "scan", mode, *sys.argv[2:]])


if __name__ == "__main__":
    main()
