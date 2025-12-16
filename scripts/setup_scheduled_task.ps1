# setup_scheduled_task.ps1
# Creates a Windows Task Scheduler task to run master_sync.js every 15 minutes
# Run this script as Administrator to set up the scheduled task

param(
    [int]$IntervalMinutes = 15,
    [switch]$Remove
)

$TaskName = "AllDayWalletViewer-MasterSync"
$TaskDescription = "Runs the Master Sync Orchestrator for NFL All Day Wallet Viewer"

# Get the project directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$syncScript = Join-Path $scriptDir "run_master_sync.ps1"

if ($Remove) {
    # Remove the scheduled task
    Write-Host "Removing scheduled task: $TaskName"
    
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($task) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "✅ Scheduled task removed successfully"
    } else {
        Write-Host "Task not found"
    }
    exit 0
}

Write-Host "========================================="
Write-Host "Setting up Master Sync Scheduled Task"
Write-Host "========================================="
Write-Host ""
Write-Host "Task Name:     $TaskName"
Write-Host "Interval:      Every $IntervalMinutes minutes"
Write-Host "Script Path:   $syncScript"
Write-Host "Project Root:  $projectRoot"
Write-Host ""

# Check if running as admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "⚠️  WARNING: This script should be run as Administrator for best results."
    Write-Host "   The task will be created but may have limited permissions."
    Write-Host ""
}

# Check if task already exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Task already exists. Removing and recreating..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the action
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$syncScript`"" `
    -WorkingDirectory $projectRoot

# Create the trigger (every X minutes)
$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration ([TimeSpan]::MaxValue)

# Create settings
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

# Create the principal (run only when user is logged on)
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

# Register the task
try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $TaskDescription `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal | Out-Null
    
    Write-Host ""
    Write-Host "✅ Scheduled task created successfully!"
    Write-Host ""
    Write-Host "The task will run every $IntervalMinutes minutes."
    Write-Host ""
    Write-Host "To manage the task:"
    Write-Host "  - Open Task Scheduler (taskschd.msc)"
    Write-Host "  - Look for: $TaskName"
    Write-Host ""
    Write-Host "To remove the task:"
    Write-Host "  .\setup_scheduled_task.ps1 -Remove"
    Write-Host ""
    Write-Host "To start manually:"
    Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
}
catch {
    Write-Host ""
    Write-Host "❌ Error creating scheduled task:"
    Write-Host "   $($_.Exception.Message)"
    Write-Host ""
    Write-Host "Try running this script as Administrator."
    exit 1
}
