#!/bin/bash
echo "Starting StreamDuck pipeline..."
echo "Let it run for 20 seconds to accumulate messages..."
echo "Then it will stop gracefully (simulating Ctrl+C)"
echo ""

timeout -s INT 20 uv run python src/main.py run 2>&1 | grep -E "(Starting|🚀|📨|✅ Message|🛑|📦|💾|checkpoint|stopped)" || true

echo ""
echo "Test completed! Check logs above for graceful shutdown sequence."
