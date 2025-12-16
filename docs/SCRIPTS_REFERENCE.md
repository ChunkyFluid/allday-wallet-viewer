# NFL All Day Wallet Viewer - Scripts Reference Guide

## Main Scripts

### master_sync.js
**Master orchestrator that runs all data syncs in optimal order.**

| Flag | Description |
|------|-------------|
| `--full` | Force full refresh (ignores incremental sync) |
| `--metadata-only` | Only sync NFT metadata |
| `--holdings-only` | Only sync wallet holdings (cheaper) |
| `--prices-only` | Only sync prices |
| `--wallet=ADDRESS` | Sync a single wallet from blockchain |
| `--refresh-usernames` | Re-fetch usernames for wallets with NULL names |

**Example Usage:**
```
node scripts/master_sync.js
node scripts/master_sync.js --holdings-only
node scripts/master_sync.js --wallet=0x07943321994c72c6
```
**Queries Snowflake:** Yes

---

### run_master_sync.ps1
PowerShell wrapper for master_sync.js with logging. Creates timestamped logs in `logs/` directory.

**Queries Snowflake:** Yes (runs master_sync)

---

### setup_scheduled_task.ps1
Creates Windows Task Scheduler entry for automated sync.

```
.\scripts\setup_scheduled_task.ps1                    # Create (15 min)
.\scripts\setup_scheduled_task.ps1 -IntervalMinutes 60 # Custom interval
.\scripts\setup_scheduled_task.ps1 -Remove             # Remove task
```
**Queries Snowflake:** No (just setup)

---

## Individual Sync Scripts

### sync_nft_core_metadata_from_snowflake.js
Syncs NFT metadata (player name, tier, serial number, team, etc.) from Snowflake to PostgreSQL.

**Queries Snowflake:** Yes

---

### sync_wallet_holdings_from_snowflake.js
Syncs which wallets hold which NFTs from Snowflake.

**Queries Snowflake:** Yes

---

### sync_wallet_profiles_from_dapper.js
Fetches usernames/display names from Dapper API for wallet addresses.

**Queries Snowflake:** No (uses Dapper API)

---

### sync_wallets_from_blockchain.js
Gets wallet data directly from Flow blockchain using Cadence scripts.

**Queries Snowflake:** No (uses Flow blockchain)

---

### sync_wallets_from_blockchain_only.js
Blockchain-only version of wallet sync.

**Queries Snowflake:** No (uses Flow blockchain)

---

### sync_prices_from_scrape.js
Old Playwright-based price scraper. **Deprecated** - use master_sync.js --prices-only instead.

**Queries Snowflake:** No (browser scrape)

---

### sync_leaderboards.js
Syncs leaderboard/challenge data.

---

## Utility Scripts

### setup-database.js
Creates all required PostgreSQL tables and indexes.

```
node scripts/setup-database.js
```

---

### snowflake-utils.js
Shared utilities for Snowflake connection, retries, and rate limiting. Not run directly.

---

### debug_wallet.js
Debug a specific wallet's data in the database.

```
node scripts/debug_wallet.js 0x07943321994c72c6
```

---

### background_wallet_sync.js
Background sync for newly discovered wallets.

---

## Snapshot Builders

### build_editions_snapshot.js
Builds edition data snapshot for caching.

---

### build_explorer_filters_snapshot.js
Builds filter options for the explorer page.

---

### build_set_editions_snapshot.js
Builds set/edition mapping snapshot.

---

### build_set_totals_snapshot.js
Builds set completion totals snapshot.

---

### create_set_completion_indexes.js
Creates database indexes for set completion queries.

---

## SQL Files (Run manually in Snowflake)

### snowflake_backfill_metadata.sql
SQL to rebuild the ALLDAY_CORE_NFT_METADATA table in Snowflake.

---

### snowflake_backfill_holdings.sql
SQL to rebuild the ALLDAY_WALLET_HOLDINGS_CURRENT table in Snowflake.

---

### snowflake_diagnose_holdings.sql
Diagnostic queries to troubleshoot holdings sync issues.

---

## Other Scripts

### fetch_unknown_nft_metadata.js
Fetches metadata for NFTs not in the database.

---

### discover_findlabs_nft.js
Discovers FindLabs NFTs on the blockchain.

---

### refresh-kickoffs.js
Refreshes kickoff challenges data.

---

## Cost-Saving Tips

To minimize Snowflake costs:

1. **Use `--holdings-only`** for regular syncs (cheaper query)
2. **Run `--prices-only` rarely** (once per week) - this is the expensive query
3. **Use `--wallet=ADDRESS`** for individual wallets (uses blockchain, not Snowflake)
4. **Don't automate** - run syncs manually when needed

---

*Generated: December 16, 2025*
