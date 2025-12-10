# Render Refresh Runbook (after Snowflake backfill)

Assumes `.env` points to Render Postgres and Snowflake creds are set.

```bash
# 1) Core metadata from Snowflake -> Render
node scripts/sync_nft_core_metadata_from_snowflake.js

# 2) Wallet holdings (full refresh) from Snowflake -> Render
node scripts/sync_wallet_holdings_from_snowflake.js

# 3) Wallet profiles (Dapper display names)
node scripts/sync_wallet_profiles_from_dapper.js

# 4) Prices
#   Preferred: CSV loader into edition_price_scrape (if available)
#   Fallback: scraper (initial small batch)
node scripts/sync_prices_from_scrape.js

# 5) Leaderboards / snapshots
node scripts/sync_leaderboards.js
```

Verification (Render, via psql/pgAdmin):
- `SELECT COUNT(*) FROM wallet_holdings WHERE wallet_address='0xYOURWALLET';`
- `SELECT COUNT(*) FROM top_wallets_snapshot;`
- `SELECT COUNT(*) FROM edition_price_scrape;`
- Spot-check wallet in UI.
