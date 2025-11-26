@echo off
setlocal enabledelayedexpansion

:: >>> UPDATE THIS IF YOU EVER MOVE THE FOLDER <<<
set "PROJECT_DIR=C:\Users\kseaver\OneDrive\NFL\allday-wallet-viewer"

echo.
echo ==============================
echo  NFL ALL DAY ETL REFRESH TOOL
echo ==============================
echo.

pushd "%PROJECT_DIR%" || (
  echo Failed to change directory to "%PROJECT_DIR%"
  echo Check the PROJECT_DIR at the top of this file.
  pause
  exit /b 1
)

echo 1^) Quick refresh  (wallets + profiles + prices + top wallets snapshot)
echo 2^) Full refresh   (metadata + wallets + profiles + prices + snapshots)
echo Q^) Quit
echo.
set /p CHOICE=Select option (1/2/Q): 

if /I "%CHOICE%"=="1" goto QUICK
if /I "%CHOICE%"=="2" goto FULL
if /I "%CHOICE%"=="Q" goto END
if /I "%CHOICE%"=="q" goto END

echo.
echo Invalid choice.
goto END

:: --------- Helper: run one step and bail on error ----------
:RUNSTEP
:: %1 = label text, %2 = command
echo.
echo --------------------------------------
echo %~1
echo --------------------------------------
echo Running: %~2
echo.

%~2
if errorlevel 1 (
  echo.
  echo *** ERROR running: %~2
  echo Stopping batch. Fix the error above and re-run.
  goto END
)

goto :eof

:: --------- Option 1: Quick refresh ----------
:QUICK
call :RUNSTEP "Sync wallet holdings from Snowflake" "node scripts\sync_wallet_holdings_from_snowflake.js"
call :RUNSTEP "Sync wallet profiles from Dapper" "node scripts\sync_wallet_profiles_from_dapper.js"
call :RUNSTEP "Load edition prices CSV into edition_price_scrape" "node scripts\load_edition_prices_from_csv.js"
call :RUNSTEP "Refresh editions_snapshot" "node etl_editions_snapshot.js"
call :RUNSTEP "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_team_snapshot" "node etl_top_wallets_by_team_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_tier_snapshot" "node etl_top_wallets_by_tier_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_value_snapshot" "node etl_top_wallets_by_value_snapshot.js"
call :RUNSTEP "Refresh explorer_filters_snapshot" "node etl_explorer_filters_snapshot.js"
goto END

:: --------- Option 2: Full refresh ----------
:FULL
call :RUNSTEP "Sync NFT core metadata from Snowflake (BIG / SLOW)" "node scripts\sync_nft_core_metadata_from_snowflake.js"
call :RUNSTEP "Sync wallet holdings from Snowflake" "node scripts\sync_wallet_holdings_from_snowflake.js"
call :RUNSTEP "Sync wallet profiles from Dapper" "node scripts\sync_wallet_profiles_from_dapper.js"
call :RUNSTEP "Load edition prices CSV into edition_price_scrape" "node scripts\load_edition_prices_from_csv.js"
call :RUNSTEP "Refresh editions_snapshot" "node etl_editions_snapshot.js"
call :RUNSTEP "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_team_snapshot" "node etl_top_wallets_by_team_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_tier_snapshot" "node etl_top_wallets_by_tier_snapshot.js"
call :RUNSTEP "Refresh top_wallets_by_value_snapshot" "node etl_top_wallets_by_value_snapshot.js"
call :RUNSTEP "Refresh explorer_filters_snapshot" "node etl_explorer_filters_snapshot.js"
goto END

:END
echo.
echo Done. Press any key to close...
pause >nul
popd
endlocal
