#!/usr/bin/env bash
# master_sync.sh — Full pipeline: normalize → canonical → views → index
# Usage: bash master_sync.sh [--full-only | --connector-only | --all (default)]
#
# Data sources and promotion flow:
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ SOURCE                      │ RECORDS  │ LANE         │ PROMOTED BY          │
# ├─────────────────────────────┼──────────┼──────────────┼──────────────────────┤
# │ Spotify extended streaming  │ 166,227  │ full-export  │ merge_activities (spotify) │
# │ Claude conversations/msgs   │ 1,308+   │ full-export  │ merge_claude_full_export   │
# │ YouTube / ChatGPT events    │ 72K+     │ connector    │ staged directly            │
# │ LinkedIn activities         │ 34,926   │ full-export  │ merge_records (activities) │
# │ LinkedIn documents          │ 3,181    │ full-export  │ merge_records (documents)  │
# │ LinkedIn people             │ 714      │ full-export  │ merge_records (people)     │
# │ LinkedIn messages           │ 424      │ full-export  │ merge_records (messages)   │
# │ iMessage                    │ 458,026  │ local/chat_db│ merge_local_records        │
# │ Apple Mail                  │ 5,000    │ local/mail   │ merge_local_records        │
# │ Calendar events             │ 2,220    │ local/cal    │ merge_local_records        │
# │ Gmail MBOX (run separately) │ varies   │ local/mail   │ merge_local_records        │
# │ Browser history (Brave/FF)  │ varies   │ device       │ merge_browser_history      │
# │ macOS Screen Time           │ varies   │ device       │ merge_screentime           │
# └─────────────────────────────┴──────────┴──────────────┴──────────────────────┘
#
# To extract Gmail (~1.7 GB MBOX) before sync, run once:
#   python3 meta/scripts/extract_gmail_mbox.py
#
# To force iPhone Screen Time sync: Settings → Screen Time → Share Across Devices ON
set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
PDR_PYTHON="/path/to/personal-context-sdk/.venv/bin/python3"
PDR_SRC="/path/to/personal-context-sdk/src"

MODE="${1:---all}"

echo "=== master_sync $(date -u '+%Y-%m-%dT%H:%M:%SZ') mode=$MODE ==="

# Step 1: Normalize full exports
if [[ "$MODE" != "--connector-only" ]]; then
  echo ""
  echo "--- Step 1: normalize_full_exports ---"
  python3 "$SCRIPTS_DIR/normalize_full_exports.py" 2>&1 | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    rc = d.get('record_counts', {})
    print('  ' + '  '.join(f'{v:,} {k}' for k,v in rc.items() if v))
except Exception as e:
    print(sys.stdin.read())
"
fi

# Step 2: Normalize connector exports + promote LinkedIn, local lane, browser, screentime
if [[ "$MODE" != "--full-only" ]]; then
  echo ""
  echo "--- Step 2: normalize_connector_exports ---"
  python3 "$SCRIPTS_DIR/normalize_connector_exports.py" 2>&1 | python3 -c "
import sys, json, re
out = sys.stdin.read()
# print merge lines
for line in out.splitlines():
    if 'merged' in line or 'skipping' in line or 'spotify' in line.lower():
        print(' ', line.strip())
try:
    d = json.loads(out[out.rfind('{'):out.rfind('}')+1])
    rc = d.get('record_counts', {})
    print('  ' + '  '.join(f'{v:,} {k}' for k,v in rc.items() if v))
except Exception:
    pass
" || true
fi

# Step 3: Build canonical NDJSON
# Reads from the connector lane normalized (which includes all merge promotions from Step 2)
# Writes to the connector lane canonical (where Step 4/5 read from)
CONNECTOR_NORMALIZED="$SCRIPTS_DIR/../lanes/connector/normalized"
CONNECTOR_CANONICAL="$SCRIPTS_DIR/../lanes/connector/canonical"
echo ""
echo "--- Step 3: build_canonical_ndjson ---"
python3 "$SCRIPTS_DIR/build_canonical_ndjson.py" \
  --normalized-root "$CONNECTOR_NORMALIZED" \
  --output-root "$CONNECTOR_CANONICAL" \
  --source-index "$SCRIPTS_DIR/../lanes/connector/raw/source_index.jsonl" \
  --source-root "/path/to/data-connect/meta/working/staged-sources/connector" \
  2>&1 | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    counts = d.get('counts', {})
    print('  ' + '  '.join(f'{v:,} {k}' for k,v in counts.items()))
except Exception:
    print(sys.stdin.read())
"

# Step 4: Build views
echo ""
echo "--- Step 4: build_views ---"
python3 "$SCRIPTS_DIR/build_views.py" 2>&1 | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    counts = d.get('counts', {})
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f'  {v:7,}  {k}')
except Exception:
    print(sys.stdin.read())
"

# Step 5: Rebuild SQLite index
echo ""
echo "--- Step 5: indexes build ---"
PYTHONPATH="$PDR_SRC" "$PDR_PYTHON" -c "
from personal_data_router.cli import main
import sys
sys.argv = ['pdr', 'indexes', 'build']
main()
" 2>&1 | grep -v "^$" || true

# Step 6: Print final index summary
echo ""
echo "--- Final index summary ---"
"$PDR_PYTHON" -c "
import sqlite3
conn = sqlite3.connect('/path/to/data-connect/runtime/indexes/connector.sqlite')
print('Events by platform:')
for row in conn.execute('SELECT platform, COUNT(*) FROM events GROUP BY platform ORDER BY COUNT(*) DESC'):
    print(f'  {row[1]:7,}  {row[0]}')
total = conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
print(f'  {total:7,}  TOTAL')
print()
print('Views:')
for row in conn.execute('SELECT view_name, COUNT(*) FROM views GROUP BY view_name ORDER BY COUNT(*) DESC'):
    print(f'  {row[1]:7,}  {row[0]}')
"

echo ""
echo "=== Done ==="
