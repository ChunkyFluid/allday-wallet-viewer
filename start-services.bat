@echo off
REM Start all necessary services for NFL All Day Wallet Viewer
REM This script starts the main server and background sync service

echo ========================================
echo Starting NFL All Day Wallet Viewer Services
echo ========================================
echo.

REM Check if Node.js is installed
where node >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Node.js is not installed or not in PATH
    echo Please install Node.js from https://nodejs.org/
    pause
    exit /b 1
)

REM Check if .env file exists
if not exist .env (
    echo WARNING: .env file not found
    echo Please create a .env file with your configuration
    echo.
)

REM Change to script directory (where this .bat file is located)
cd /d "%~dp0"

REM Verify we're in the right directory
if not exist "server.js" (
    echo ERROR: server.js not found in current directory
    echo Current directory: %CD%
    echo Script location: %~dp0
    echo.
    echo Please make sure start-services.bat is in the project root directory
    pause
    exit /b 1
)

REM Store the project directory
set PROJECT_DIR=%CD%

echo Project directory: %PROJECT_DIR%
echo.

echo [1/3] Starting main server...
start "NFL All Day - Main Server" cmd /k "cd /d %PROJECT_DIR% && node server.js"
timeout /t 2 /nobreak >nul

echo [2/3] Syncing leaderboards...
node scripts/sync_leaderboards.js
echo.

echo [3/3] Starting background wallet sync service...
start "NFL All Day - Wallet Sync" cmd /k "cd /d %PROJECT_DIR% && node scripts/background_wallet_sync.js"
timeout /t 2 /nobreak >nul

echo.
echo ========================================
echo Services started!
echo ========================================
echo.
echo Main Server: http://localhost:3000
echo Background Sync: Running in separate window
echo.
echo To stop services, close the command windows or press Ctrl+C in each window
echo.
pause

