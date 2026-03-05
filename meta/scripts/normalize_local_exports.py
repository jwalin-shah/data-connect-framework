#!/usr/bin/env python3
"""Normalize locally collected desktop export data into a lane-specific output root."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT = Path("/path/to/data-connect/meta/scripts/normalize_exports.py")
STAGE_SCRIPT = Path("/path/to/data-connect/meta/scripts/stage_app_exports.py")


def main() -> int:
    stage_cmd = [sys.executable, str(STAGE_SCRIPT)]
    stage_rc = subprocess.call(stage_cmd)
    if stage_rc != 0:
        return stage_rc

    cmd = [
        sys.executable,
        str(SCRIPT),
        "--source-root",
        "/path/to/data-connect/meta/working/staged-sources/local",
        "--output-root",
        "/path/to/data-connect/meta/lanes/local",
    ]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
