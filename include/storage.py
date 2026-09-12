"""Landing-zone writes.

Two things matter here and nothing else:

1. **Atomic writes.** Write to a temp file in the same directory, then
   `os.replace`. A task killed mid-write leaves no half-written parquet that a
   later run would happily read as if it were complete.

2. **Deterministic paths.** The path is a pure function of (series, interval
   start). Re-running the same interval overwrites exactly the same file, which
   is what makes the landing zone idempotent.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path

log = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/opt/airflow/data"))


def raw_path(series_name: str, interval_start: date, root: Path | None = None) -> Path:
    """raw/<series>/dt=YYYY-MM-DD/data.json  - Hive-style partitioning."""
    base = root or DATA_ROOT
    return base / "raw" / series_name / f"dt={interval_start.isoformat()}" / "data.json"


def write_atomic(path: Path, rows: list[dict]) -> Path:
    """Serialise rows to `path` atomically. Returns the path written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=None)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)  # atomic within the same filesystem
    except Exception:
        # Never leave the temp file behind on failure.
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise

    log.info("wrote %d rows to %s", len(rows), path)
    return path


def read_rows(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
