#!/bin/bash
# NFL ALL DAY ETL REFRESH TOOL (macOS version)

set -e  # Exit on error

# Change to project directory
cd "$(dirname "$0")"

echo ""
echo "=============================="
echo " NFL ALL DAY ETL REFRESH TOOL"
echo "=============================="
echo ""

echo "1) Quick refresh   (~2 min) - wallets + profiles + snapshots (SKIP metadata)"
echo "2) Full refresh    (~30 min) - EVERYTHING from scratch"  
echo "3) Delta refresh   (~5 min) - Only NEW data since last sync"
echo "Q) Quit"
echo ""
read -p "Select option (1/2/3/Q): " CHOICE

run_step() {
    local label="$1"
    local cmd="$2"
    echo ""
    echo "--------------------------------------"
    echo "$label"
    echo "--------------------------------------"
    echo "Running: $cmd"
    echo ""
    eval "$cmd"
    if [ $? -ne 0 ]; then
        echo ""
        echo "*** ERROR running: $cmd"
        echo "Stopping. Fix the error above and re-run."
        exit 1
    fi
}

case "$CHOICE" in
    1)
        # Quick refresh - skip metadata (fastest)
        echo "🚀 Quick refresh - skipping metadata sync..."
        run_step "Sync wallet holdings from Snowflake" "node scripts/sync_wallet_holdings_from_snowflake.js"
        run_step "Sync wallet profiles from Dapper" "node scripts/sync_wallet_profiles_from_dapper.js"
        run_step "Load edition prices CSV" "node scripts/load_edition_prices_from_csv.js"
        run_step "Refresh editions_snapshot" "node etl_editions_snapshot.js"
        run_step "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
        run_step "Refresh top_wallets_by_team_snapshot" "node etl_top_wallets_by_team_snapshot.js"
        run_step "Refresh top_wallets_by_tier_snapshot" "node etl_top_wallets_by_tier_snapshot.js"
        run_step "Refresh top_wallets_by_value_snapshot" "node etl_top_wallets_by_value_snapshot.js"
        run_step "Refresh explorer_filters_snapshot" "node etl_explorer_filters_snapshot.js"
        ;;
    2)
        # Full refresh - everything
        echo "🔄 Full refresh - this will take ~30 minutes..."
        run_step "Sync NFT core metadata from Snowflake (SLOW)" "node scripts/sync_nft_core_metadata_from_snowflake.js"
        run_step "Sync wallet holdings from Snowflake" "node scripts/sync_wallet_holdings_from_snowflake.js"
        run_step "Sync wallet profiles from Dapper" "node scripts/sync_wallet_profiles_from_dapper.js"
        run_step "Load edition prices CSV" "node scripts/load_edition_prices_from_csv.js"
        run_step "Refresh editions_snapshot" "node etl_editions_snapshot.js"
        run_step "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
        run_step "Refresh top_wallets_by_team_snapshot" "node etl_top_wallets_by_team_snapshot.js"
        run_step "Refresh top_wallets_by_tier_snapshot" "node etl_top_wallets_by_tier_snapshot.js"
        run_step "Refresh top_wallets_by_value_snapshot" "node etl_top_wallets_by_value_snapshot.js"
        run_step "Refresh explorer_filters_snapshot" "node etl_explorer_filters_snapshot.js"
        ;;
    3)
        # Delta refresh - incremental updates only
        echo "⚡ Delta refresh - only new data since last sync..."
        run_step "Sync NEW wallet holdings (incremental)" "node scripts/sync_wallet_holdings_from_snowflake.js --incremental"
        run_step "Sync wallet profiles from Dapper" "node scripts/sync_wallet_profiles_from_dapper.js"
        run_step "Load edition prices CSV" "node scripts/load_edition_prices_from_csv.js"
        run_step "Refresh editions_snapshot" "node etl_editions_snapshot.js"
        run_step "Refresh top_wallets_snapshot" "node etl_top_wallets_snapshot.js"
        run_step "Refresh top_wallets_by_team_snapshot" "node etl_top_wallets_by_team_snapshot.js"
        run_step "Refresh top_wallets_by_tier_snapshot" "node etl_top_wallets_by_tier_snapshot.js"
        run_step "Refresh top_wallets_by_value_snapshot" "node etl_top_wallets_by_value_snapshot.js"
        run_step "Refresh explorer_filters_snapshot" "node etl_explorer_filters_snapshot.js"
        ;;
    Q|q|quit|Quit)
        echo "Exiting."
        exit 0
        ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

echo ""
echo "=============================="
echo " ETL COMPLETE!"
echo "=============================="
echo "Finished at $(date)"
echo ""
