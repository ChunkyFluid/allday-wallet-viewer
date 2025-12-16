# run_master_sync.ps1
# PowerShell wrapper script for running master_sync.js via Windows Task Scheduler
# This script handles environment setup and logging

# Set working directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
Set-Location $projectRoot

# Create logs directory if it doesn't exist
$logDir = Join-Path $projectRoot "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

# Log file with timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = Join-Path $logDir "master_sync_$timestamp.log"

# Run the sync and capture output
$startTime = Get-Date
Write-Output "========================================" | Tee-Object -FilePath $logFile -Append
Write-Output "Master Sync Started: $startTime" | Tee-Object -FilePath $logFile -Append
Write-Output "========================================" | Tee-Object -FilePath $logFile -Append

try {
    # Run node master_sync.js
    & node scripts/master_sync.js 2>&1 | Tee-Object -FilePath $logFile -Append
    $exitCode = $LASTEXITCODE
}
catch {
    Write-Output "ERROR: $($_.Exception.Message)" | Tee-Object -FilePath $logFile -Append
    $exitCode = 1
}

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Output "========================================" | Tee-Object -FilePath $logFile -Append
Write-Output "Master Sync Completed: $endTime" | Tee-Object -FilePath $logFile -Append
Write-Output "Duration: $($duration.TotalSeconds) seconds" | Tee-Object -FilePath $logFile -Append
Write-Output "Exit Code: $exitCode" | Tee-Object -FilePath $logFile -Append
Write-Output "========================================" | Tee-Object -FilePath $logFile -Append

# Clean up old log files (keep last 50)
$oldLogs = Get-ChildItem -Path $logDir -Filter "master_sync_*.log" | Sort-Object CreationTime -Descending | Select-Object -Skip 50
if ($oldLogs) {
    $oldLogs | Remove-Item -Force
}

exit $exitCode
