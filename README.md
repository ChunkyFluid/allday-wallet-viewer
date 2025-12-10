# NFL All Day – Wallet Viewer

A full‑stack app for exploring NFL ALL DAY wallets, moments, and collections.

- Paste a Flow wallet (e.g. `0x7541bafd155b683e`) to see all moments, filters, prices and stats.
- Browse top holders, profiles, and search metadata.

## Quick Start

```bash
npm install
cp .env.example .env
# Fill in .env with your Snowflake + Postgres (Render) creds
npm run dev
# open http://localhost:3000
```

## Data pipeline (high level)

**Snowflake (source)**

- Raw on‑chain events: `FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS`
- Derived tables you maintain in Snowflake:
  - `ALLDAY_CORE_NFT_METADATA` – one row per `nft_id` (edition, play, player, team, tier, etc.)
  - `ALLDAY_WALLET_HOLDINGS_CURRENT` – one row per current `(wallet_address, nft_id)` with `is_locked` and `last_event_ts`

**Render Postgres (app DB)**

- `nft_core_metadata` – synced from `ALLDAY_CORE_NFT_METADATA`
- `wallet_holdings` – synced from `ALLDAY_WALLET_HOLDINGS_CURRENT`
- `edition_price_scrape` – pricing snapshot loaded from `edition_prices.csv`
- Snapshots:
  - `top_wallets_snapshot`
  - `wallet_summary_snapshot`

**Sync scripts (Node)**

- `scripts/sync_nft_core_metadata_from_snowflake.js`
  - Reads from Snowflake `ALLDAY_CORE_NFT_METADATA`
  - Upserts into `nft_core_metadata`
- `scripts/sync_wallet_holdings_from_snowflake.js`
  - Reads from Snowflake `ALLDAY_WALLET_HOLDINGS_CURRENT`
  - **Incremental sync**: only pulls rows with `last_event_ts` newer than what Render already has
  - Upserts into `wallet_holdings`
- `scripts/load_edition_prices_from_csv.js`
  - Loads `edition_prices.csv` into `public.edition_price_scrape`
- `etl_top_wallets_snapshot.js`, `etl_wallet_summary_snapshot.js`
  - Build summary tables used by `/api/top-wallets` and `/api/wallet-summary`

## ETL runner (Windows)

Use `run_etl.bat` to run common ETL flows:

- **Quick refresh** – wallets + profiles + prices + top wallets snapshot
  - `sync_wallet_holdings_from_snowflake.js`
  - `sync_wallet_profiles_from_dapper.js`
  - `load_edition_prices_from_csv.js`
  - `etl_top_wallets_snapshot.js`
- **Full refresh** – metadata + wallets + profiles + prices + snapshots
  - `sync_nft_core_metadata_from_snowflake.js`
  - `sync_wallet_holdings_from_snowflake.js`
  - `sync_wallet_profiles_from_dapper.js`
  - `load_edition_prices_from_csv.js`
  - `etl_top_wallets_snapshot.js`

## Backend API overview

- `GET /api/query?wallet=0x…`
  - Full wallet moments from Render (`wallet_holdings` + `nft_core_metadata`)
- `GET /api/wallet-summary?wallet=0x…`
  - Counts (total, locked/unlocked, per tier) and value estimates:
    - Floor value = sum of per‑edition `lowest_ask_usd` from `edition_price_scrape` times copies
    - ASP value  = sum of per‑edition `avg_sale_usd` from `edition_price_scrape` times copies
  - Also returns:
    - `holdingsLastSyncedAt` – latest `last_synced_at` for that wallet in `wallet_holdings`
    - `pricesLastScrapedAt` – latest `scraped_at` in `edition_price_scrape`
- `GET /api/prices?editions=1,2,3`
  - Batched per‑edition prices from `edition_price_scrape`
- `GET /api/top-wallets?limit=50`
  - Leaderboard from `top_wallets_snapshot`
- `GET /api/health`
  - Simple health status for Postgres and Snowflake connectivity

## Frontend

- Mostly static HTML in `public/` with vanilla JS (`public/app.js`, `top-holders.js`, etc.).
- Wallet page:
  - Client‑side filters (team, player, series, set, tier, position, locked)
  - Sortable columns (tier, serial, prices, last event)
  - Wallet summary showing counts, floor value, ASP value, and data freshness timestamps.

## Logging

- ETL scripts log progress to stdout (Node `console.log`).
- You can control verbosity per script with environment variables (e.g., `LOG_LEVEL=debug` in future refinements), or by redirecting output to a log file when running `run_etl.bat`.
