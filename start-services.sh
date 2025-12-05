#!/bin/bash
# Start all necessary services for NFL All Day Wallet Viewer
# This script starts the main server and background sync service

echo "========================================"
echo "Starting NFL All Day Wallet Viewer Services"
echo "========================================"
echo ""

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js is not installed or not in PATH"
    echo "Please install Node.js from https://nodejs.org/"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "WARNING: .env file not found"
    echo "Please create a .env file with your configuration"
    echo ""
fi

# Change to script directory
cd "$(dirname "$0")"

echo "[1/2] Starting main server..."
node server.js &
SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"
sleep 2

echo "[2/2] Starting background wallet sync service..."
node scripts/background_wallet_sync.js &
SYNC_PID=$!
echo "Sync service started with PID: $SYNC_PID"
sleep 2

echo ""
echo "========================================"
echo "Services started!"
echo "========================================"
echo ""
echo "Main Server: http://localhost:3000"
echo "Background Sync: Running (PID: $SYNC_PID)"
echo ""
echo "Server PID: $SERVER_PID"
echo "Sync PID: $SYNC_PID"
echo ""
echo "To stop services, run: kill $SERVER_PID $SYNC_PID"
echo "Or press Ctrl+C"
echo ""

# Wait for user interrupt
trap "echo ''; echo 'Stopping services...'; kill $SERVER_PID $SYNC_PID 2>/dev/null; exit" INT TERM

# Keep script running
wait

