@echo off
setlocal
cd /d C:\Users\kseaver\OneDrive\NFL\allday-wallet-viewer
if not exist logs mkdir logs

echo [%%date%% %%time%%] Fast holdings refresh >> logs\etl_fast.log 2>&1
node etl_holdings.js >> logs\etl_fast.log 2>&1
node etl_wallet_summary_snapshot.js >> logs\etl_fast.log 2>&1

endlocal
