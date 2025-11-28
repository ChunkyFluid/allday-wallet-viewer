@echo off
setlocal

REM Change to project folder
cd /d C:\Users\kseaver\OneDrive\NFL\allday-wallet-viewer

REM Make sure logs folder exists
if not exist logs mkdir logs

echo [%%date%% %%time%%] Starting full ETL >> logs\etl.log 2>&1

REM 1) Metadata (players/editions/plays)
echo --- Running etl_metadata.js --- >> logs\etl.log 2>&1
node etl_metadata.js >> logs\etl.log 2>&1

REM 2) Holdings (who owns what, lock state, etc.)
echo --- Running etl_holdings.js --- >> logs\etl.log 2>&1
node etl_holdings.js >> logs\etl.log 2>&1

REM 3) Edition prices (ASP, etc.)
echo --- Running etl_edition_prices.js --- >> logs\etl.log 2>&1
node etl_edition_prices.js >> logs\etl.log 2>&1

REM 4) Low asks (optional; comment out if it’s too slow)
echo --- Running etl_low_asks.js --- >> logs\etl.log 2>&1
node etl_low_asks.js >> logs\etl.log 2>&1

REM 5) Explorer filters snapshot (Browse dropdowns)
echo --- Running etl_explorer_filters_snapshot.js --- >> logs\etl.log 2>&1
node etl_explorer_filters_snapshot.js >> logs\etl.log 2>&1

REM 6) Top wallets snapshot
echo --- Running etl_top_wallets_snapshot.js --- >> logs\etl.log 2>&1
node etl_top_wallets_snapshot.js >> logs\etl.log 2>&1

REM 7) Wallet summary snapshot (pre-warm summaries)
echo --- Running etl_wallet_summary_snapshot.js --- >> logs\etl.log 2>&1
node etl_wallet_summary_snapshot.js >> logs\etl.log 2>&1

echo [%%date%% %%time%%] ETL finished >> logs\etl.log 2>&1

endlocal
