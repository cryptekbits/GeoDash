#!/bin/bash
# Examples of using the GeoDash module CLI

echo "=== GeoDash Module CLI Examples ==="
echo

echo "1. Search for cities:"
echo "python -m GeoDash search 'New York' --limit 3"
python -m GeoDash search "New York" --limit 3
echo

echo "2. Get cities near coordinates:"
echo "python -m GeoDash coordinates 40.7128 -74.0060 --radius 5"
python -m GeoDash coordinates 40.7128 -74.0060 --radius 5
echo

echo "3. Get a list of countries:"
echo "python -m GeoDash countries | head -n 10"
python -m GeoDash countries | head -n 10
echo

echo "4. Get states in the United States:"
echo "python -m GeoDash states 'United States' | head -n 10"
python -m GeoDash states "United States" | head -n 10
echo

echo "5. Get cities in California, United States:"
echo "python -m GeoDash cities-in-state 'California' 'United States' | head -n 100"
python -m GeoDash cities-in-state "California" "United States" | head -n 100
echo

echo "6. Start the API server (Ctrl+C to stop):"
echo "# The server command isn't part of the CLI module, you need to use the API directly:"
echo "python -c 'from GeoDash import start_server; start_server(host=\"localhost\", port=5000, debug=True)'"
echo "# Not running automatically in this script"
echo

echo "=== End of Examples ===" 