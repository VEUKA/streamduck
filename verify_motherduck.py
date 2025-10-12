#!/usr/bin/env python3
"""Quick script to verify data in MotherDuck."""
import duckdb

# Connect to MotherDuck
conn = duckdb.connect("md:")

print("🦆 Checking MotherDuck tables...")
print()

# Check ingest table
print("📊 Table: mother_ducklake.ingest.table1")
result = conn.execute("SELECT COUNT(*) as count FROM mother_ducklake.ingest.table1").fetchone()
print(f"   Total rows: {result[0]}")

# Show sample rows
print("\n📝 Sample rows (first 5):")
sample = conn.execute("SELECT * FROM mother_ducklake.ingest.table1 LIMIT 5").fetchall()
for row in sample:
    print(f"   {row}")

# Check control table
print("\n📊 Table: mother_ducklake.control.waterleveleh")
result = conn.execute("SELECT COUNT(*) as count FROM mother_ducklake.control.waterleveleh").fetchone()
print(f"   Total checkpoint rows: {result[0]}")

# Show checkpoints
print("\n📝 Checkpoints:")
checkpoints = conn.execute("""
    SELECT partition_id, sequence_number, last_updated 
    FROM mother_ducklake.control.waterleveleh 
    ORDER BY partition_id
""").fetchall()
for row in checkpoints:
    print(f"   Partition {row[0]}: seq={row[1]}, updated={row[2]}")

conn.close()
print("\n✅ Verification complete!")
