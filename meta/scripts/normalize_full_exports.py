#!/usr/bin/env python3
"""Stage and normalize full-account exports into a lane-specific output root."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


STAGE_SCRIPT = Path("/path/to/data-connect/meta/scripts/stage_full_exports.py")
NORMALIZE_SCRIPT = Path("/path/to/data-connect/meta/scripts/normalize_exports.py")


def main() -> int:
    stage_result = subprocess.call([sys.executable, str(STAGE_SCRIPT)])
    if stage_result != 0:
        return stage_result

    cmd = [
        sys.executable,
        str(NORMALIZE_SCRIPT),
        "--source-root",
        "/path/to/data-connect/meta/working/staged-sources/full-export",
        "--output-root",
        "/path/to/data-connect/meta/lanes/full-export",
    ]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
