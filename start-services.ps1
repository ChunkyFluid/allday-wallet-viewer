# PowerShell script to start all necessary services for NFL All Day Wallet Viewer
# This script starts the main server and background sync service

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Starting NFL All Day Wallet Viewer Services" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Node.js is installed
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Node.js is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Node.js from https://nodejs.org/" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if .env file exists
if (-not (Test-Path .env)) {
    Write-Host "WARNING: .env file not found" -ForegroundColor Yellow
    Write-Host "Please create a .env file with your configuration" -ForegroundColor Yellow
    Write-Host ""
}

# Change to script directory
Set-Location $PSScriptRoot

Write-Host "[1/2] Starting main server..." -ForegroundColor Green
$serverJob = Start-Process -FilePath "node" -ArgumentList "server.js" -PassThru -WindowStyle Normal
Start-Sleep -Seconds 2

Write-Host "[2/2] Starting background wallet sync service..." -ForegroundColor Green
$syncJob = Start-Process -FilePath "node" -ArgumentList "scripts/background_wallet_sync.js" -PassThru -WindowStyle Normal
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Services started!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Main Server: http://localhost:3000" -ForegroundColor Yellow
Write-Host "Background Sync: Running in separate window" -ForegroundColor Yellow
Write-Host ""
Write-Host "Server Process ID: $($serverJob.Id)" -ForegroundColor Gray
Write-Host "Sync Process ID: $($syncJob.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "To stop services, close the windows or run:" -ForegroundColor Gray
Write-Host "  Stop-Process -Id $($serverJob.Id), $($syncJob.Id)" -ForegroundColor Gray
Write-Host ""
Write-Host "Press Ctrl+C to exit this script (services will continue running)" -ForegroundColor Gray
Write-Host ""

# Keep script running
try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
} finally {
    Write-Host ""
    Write-Host "Stopping services..." -ForegroundColor Yellow
    Stop-Process -Id $serverJob.Id, $syncJob.Id -ErrorAction SilentlyContinue
    Write-Host "Services stopped." -ForegroundColor Green
}

