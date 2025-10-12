#!/bin/bash

# Test graceful shutdown

echo "Starting pipeline..."
uv run python src/main.py run > /tmp/test_shutdown.log 2>&1 &
PID=$!
echo "Pipeline started with PID: $PID"

echo "Waiting 20 seconds for messages to accumulate..."
sleep 20

echo "Sending SIGINT to trigger graceful shutdown..."
kill -INT $PID

echo "Waiting for shutdown to complete..."
sleep 10

echo "Checking shutdown logs..."
tail -50 /tmp/test_shutdown.log | grep -E "(🛑|📦|✅.*remaining|shutdown)"

echo ""
echo "Full shutdown sequence:"
tail -100 /tmp/test_shutdown.log | grep -E "(Message.*added|🛑|📦|✅.*remaining|checkpoint)" | tail -20
