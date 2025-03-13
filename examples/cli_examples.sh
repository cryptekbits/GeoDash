#!/bin/bash
# Examples of using the citizen module CLI

echo "=== CitiZen Module CLI Examples ==="
echo

echo "1. Search for cities:"
echo "python -m citizen search 'New York' --limit 3"
python -m citizen search "New York" --limit 3
echo

echo "2. Get cities near coordinates:"
echo "python -m citizen coordinates 40.7128 -74.0060 --radius 5"
python -m citizen coordinates 40.7128 -74.0060 --radius 5
echo

echo "3. Get a list of countries:"
echo "python -m citizen countries | head -n 10"
python -m citizen countries | head -n 10
echo

echo "4. Get states in the United States:"
echo "python -m citizen states 'United States' | head -n 10"
python -m citizen states "United States" | head -n 10
echo

echo "5. Get cities in California, United States:"
echo "python -m citizen cities-in-state 'California' 'United States' | head -n 100"
python -m citizen cities-in-state "California" "United States" | head -n 100
echo

echo "6. Start the API server (Ctrl+C to stop):"
echo "# The server command isn't part of the CLI module, you need to use the API directly:"
echo "python -c 'from citizen import start_server; start_server(host=\"localhost\", port=5000, debug=True)'"
echo "# Not running automatically in this script"
echo

echo "=== End of Examples ===" 