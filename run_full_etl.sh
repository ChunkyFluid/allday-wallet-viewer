#!/bin/bash
set -e
echo "==============================" 
echo " NFL ALL DAY ETL FULL REFRESH"
echo "=============================="
echo "Started at $(date)"
echo ""

echo "Step 1/10: Sync NFT core metadata from Snowflake..."
node scripts/sync_nft_core_metadata_from_snowflake.js 2>&1

echo "Step 2/10: Sync wallet holdings from Snowflake..."
node scripts/sync_wallet_holdings_from_snowflake.js 2>&1

echo "Step 3/10: Sync wallet profiles from Dapper..."
node scripts/sync_wallet_profiles_from_dapper.js 2>&1

echo "Step 4/10: Load edition prices..."
node scripts/load_edition_prices_from_csv.js 2>&1

echo "Step 5/10: Refresh editions_snapshot..."
node etl_editions_snapshot.js 2>&1

echo "Step 6/10: Refresh top_wallets_snapshot..."
node etl_top_wallets_snapshot.js 2>&1

echo "Step 7/10: Refresh top_wallets_by_team_snapshot..."
node etl_top_wallets_by_team_snapshot.js 2>&1

echo "Step 8/10: Refresh top_wallets_by_tier_snapshot..."
node etl_top_wallets_by_tier_snapshot.js 2>&1

echo "Step 9/10: Refresh top_wallets_by_value_snapshot..."
node etl_top_wallets_by_value_snapshot.js 2>&1

echo "Step 10/10: Refresh explorer_filters_snapshot..."
node etl_explorer_filters_snapshot.js 2>&1

echo ""
echo "=============================="
echo " ETL COMPLETE!"
echo "=============================="
echo "Finished at $(date)"
