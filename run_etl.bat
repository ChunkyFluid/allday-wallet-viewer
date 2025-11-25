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
call :RUNSTEP "Sync prices from marketplace scrape (may be slow, optional)" "node scripts\sync_prices_from_scrape.js"
call :RUNSTEP "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
goto END

:: --------- Option 2: Full refresh ----------
:FULL
call :RUNSTEP "Sync NFT core metadata from Snowflake (BIG / SLOW)" "node scripts\sync_nft_core_metadata_from_snowflake.js"
call :RUNSTEP "Sync wallet holdings from Snowflake" "node scripts\sync_wallet_holdings_from_snowflake.js"
call :RUNSTEP "Sync wallet profiles from Dapper" "node scripts\sync_wallet_profiles_from_dapper.js"
call :RUNSTEP "Load edition price stats from OTM CSV" "node scripts\load_edition_price_stats_from_otm.js"
call :RUNSTEP "Sync prices from marketplace scrape (may be slow)" "node scripts\sync_prices_from_scrape.js"
call :RUNSTEP "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"

:: If/when you add more snapshot ETLs, just uncomment these:
:: call :RUNSTEP "Refresh top_holders_snapshot" "node etl_top_holders_snapshot.js"
:: call :RUNSTEP "Refresh wallet_profile_stats" "node etl_wallet_profile_stats.js"
goto END

:END
echo.
echo Done. Press any key to close...
pause >nul
popd
endlocal
