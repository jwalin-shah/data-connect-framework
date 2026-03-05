#!/usr/bin/env python3
import json
import sqlite3
from pathlib import Path

DB = Path('/path/to/data-connect/personal-server/index.db')
META = Path('/path/to/data-connect/meta/manifests/last_run_manifest.json')

if DB.exists():
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT scope, COUNT(*) as files, MAX(collected_at) as last_seen, SUM(size_bytes) as total_bytes FROM data_files GROUP BY scope ORDER BY scope"
    ).fetchall()
    print('Personal Server scopes:')
    for scope, files, last_seen, total_bytes in rows:
        print(f'- {scope}: files={files}, last_seen={last_seen}, bytes={total_bytes}')
    conn.close()
else:
    print('No personal-server index.db found')

if META.exists():
    data = json.loads(META.read_text())
    print('\nMeta normalized counts:')
    for key, value in data.get('record_counts', {}).items():
        print(f'- {key}: {value}')
else:
    print('\nNo meta run manifest found')
