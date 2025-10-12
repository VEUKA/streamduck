#!/bin/bash
# Clear old checkpoint data (contains sequence numbers, not offsets)

echo "⚠️  CLEARING OLD CHECKPOINTS"
echo "   Old checkpoints contain sequence numbers, not offsets"
echo "   Clearing them so new checkpoints will use correct offset values"
echo ""

uv run python -c "
import duckdb
conn = duckdb.connect('md:mother_ducklake')

# Show current checkpoints
print('Current checkpoints (INVALID - contain sequence numbers):')
result = conn.execute('SELECT partition_id, waterlevel FROM mother_ducklake.control.waterleveleh ORDER BY partition_id').fetchall()
for r in result:
    print(f'  Partition {r[0]}: {r[1]}')
print()

# Clear all checkpoints
print('Truncating checkpoint table...')
conn.execute('DELETE FROM mother_ducklake.control.waterleveleh')
print('✅ Checkpoint table cleared')
print()

print('Verification (should be empty):')
result = conn.execute('SELECT COUNT(*) FROM mother_ducklake.control.waterleveleh').fetchone()
print(f'  Checkpoint count: {result[0]}')
print()

conn.close()
"

echo ""
echo "✅ Ready to start pipeline with fresh checkpoints"
echo "   New checkpoints will use OFFSET values (not sequence numbers)"
echo ""
echo "Start pipeline with: uv run python src/main.py run"
