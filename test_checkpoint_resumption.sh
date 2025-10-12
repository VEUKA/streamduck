#!/bin/bash
# Test script to verify checkpoint resumption works correctly

echo "============================================================"
echo "Checkpoint Resumption Test"
echo "============================================================"
echo ""

echo "Step 1: Get current MAX checkpoints from MotherDuck"
echo "------------------------------------------------------------"
uv run python -c "
from dotenv import load_dotenv
load_dotenv('.env')
from utils.motherduck import get_partition_checkpoints

result = get_partition_checkpoints(
    eventhub_namespace='mpz2025.servicebus.windows.net',
    eventhub='topic1',
    target_db='mother_ducklake',
    target_schema='ingest',
    target_table='table1',
)

if result:
    print('Current checkpoints (will resume from checkpoint+1):')
    for pid in sorted(result.keys()):
        print(f'  Partition {pid}: MAX waterlevel = {result[pid]} → will start from {result[pid]+1}')
else:
    print('No checkpoints found')
"
echo ""

echo "Step 2: Count current records in data table"
echo "------------------------------------------------------------"
uv run python -c "
import duckdb
conn = duckdb.connect('md:')
count = conn.execute('SELECT COUNT(*) FROM mother_ducklake.ingest.table1').fetchone()[0]
print(f'Current record count: {count}')
conn.close()
"
echo ""

echo "Step 3: Truncate data table"
echo "------------------------------------------------------------"
uv run python -c "
import duckdb
conn = duckdb.connect('md:')
conn.execute('TRUNCATE TABLE mother_ducklake.ingest.table1')
count = conn.execute('SELECT COUNT(*) FROM mother_ducklake.ingest.table1').fetchone()[0]
print(f'After truncate: {count} records')
conn.close()
"
echo ""

echo "============================================================"
echo "NOW: Start the pipeline in another terminal:"
echo "  uv run python src/main.py run"
echo ""
echo "WATCH FOR:"
echo "  ✅ Resuming from per-partition checkpoints: {...}"
echo "  📨 Received event on partition X, sequence: YYYY"
echo ""
echo "EXPECTED BEHAVIOR:"
echo "  - First sequence numbers should be checkpoint+1"
echo "  - NO old messages should be ingested"
echo "  - Should only process NEW messages from EventHub"
echo ""
echo "WRONG BEHAVIOR (if bug still exists):"
echo "  - First sequence numbers would be much lower than checkpoint"
echo "  - Would immediately ingest 1000+ old messages"
echo "============================================================"
