// scripts/master_sync.js
// Master Sync Orchestrator - Runs all data syncs in optimal order
// Target: Complete all syncs in < 5 minutes

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { executeSnowflakeWithRetry, createSnowflakeConnection } from "./snowflake-utils.js";
import * as flowService from "../services/flow-blockchain.js";
import { syncLeaderboards } from "./sync_leaderboards.js";

dotenv.config();

// Configuration
const PARALLEL_WORKERS = 5;
const BATCH_SIZE = 50000;
const USERNAME_CONCURRENCY = 10;
const PRICE_CONCURRENCY = 5;

// Parse command line args
const args = process.argv.slice(2);
const isFullSync = args.includes('--full');
const metadataOnly = args.includes('--metadata-only');
const pricesOnly = args.includes('--prices-only');
const holdingsOnly = args.includes('--holdings-only');
const leaderboardsOnly = args.includes('--leaderboards-only');
const refreshUsernames = args.includes('--refresh-usernames');

// Check for --wallet=ADDRESS flag
const walletArg = args.find(a => a.startsWith('--wallet='));
const singleWallet = walletArg ? walletArg.split('=')[1]?.toLowerCase() : null;

// ============================================================
// SYNC STATUS TRACKING
// ============================================================

async function ensureSyncStatusTable() {
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS sync_status (
      sync_type TEXT PRIMARY KEY,
      last_success_at TIMESTAMPTZ,
      last_snowflake_ts TIMESTAMPTZ,
      rows_synced INTEGER DEFAULT 0,
      duration_ms INTEGER DEFAULT 0
    );
  `);
}

async function getLastSyncTime(syncType) {
    if (isFullSync) {
        console.log(`[Master Sync] Full sync requested for ${syncType}`);
        return null;
    }

    const result = await pgQuery(
        `SELECT last_snowflake_ts FROM sync_status WHERE sync_type = $1`,
        [syncType]
    );

    if (result.rows.length === 0) {
        console.log(`[Master Sync] No previous sync found for ${syncType}, doing full sync`);
        return null;
    }

    return result.rows[0].last_snowflake_ts;
}

async function updateSyncStatus(syncType, snowflakeTs, rowsSynced, durationMs) {
    await pgQuery(`
    INSERT INTO sync_status (sync_type, last_success_at, last_snowflake_ts, rows_synced, duration_ms)
    VALUES ($1, NOW(), $2, $3, $4)
    ON CONFLICT (sync_type) DO UPDATE SET
      last_success_at = NOW(),
      last_snowflake_ts = COALESCE($2, sync_status.last_snowflake_ts),
      rows_synced = $3,
      duration_ms = $4
  `, [syncType, snowflakeTs, rowsSynced, durationMs]);
}

// ============================================================
// PHASE 1: PARALLEL DATA FETCH FROM SNOWFLAKE
// ============================================================

async function syncNewNFTMetadata(connection) {
    const startTime = Date.now();

    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    // Strategy: Find NFT IDs in holdings that don't have metadata yet
    // This is smarter than full table scan - only sync what we need
    console.log(`[NFT Metadata] Finding NFTs missing metadata...`);

    const missingResult = await pgQuery(`
    SELECT DISTINCT h.nft_id
    FROM holdings h
    LEFT JOIN nft_core_metadata m ON m.nft_id = h.nft_id
    WHERE m.nft_id IS NULL
    LIMIT ${BATCH_SIZE}
  `);

    const missingNftIds = missingResult.rows.map(r => r.nft_id);
    console.log(`[NFT Metadata] Found ${missingNftIds.length} NFTs missing metadata`);

    if (missingNftIds.length === 0) {
        const duration = Date.now() - startTime;
        await updateSyncStatus('nft_metadata', null, 0, duration);
        return { count: 0, duration };
    }

    // Fetch metadata for these specific NFTs from Snowflake
    // Split into chunks to avoid query size limits
    let totalInserted = 0;
    const SNOWFLAKE_CHUNK = 5000;

    for (let i = 0; i < missingNftIds.length; i += SNOWFLAKE_CHUNK) {
        const chunk = missingNftIds.slice(i, i + SNOWFLAKE_CHUNK);
        const nftIdList = chunk.map(id => `'${id}'`).join(',');

        const sql = `
      SELECT
        m.NFT_ID AS nft_id,
        m.EDITION_ID AS edition_id,
        m.PLAY_ID AS play_id,
        m.SERIES_ID AS series_id,
        m.SET_ID AS set_id,
        m.TIER AS tier,
        TRY_TO_NUMBER(m.SERIAL_NUMBER) AS serial_number,
        TRY_TO_NUMBER(m.MAX_MINT_SIZE) AS max_mint_size,
        m.FIRST_NAME AS first_name,
        m.LAST_NAME AS last_name,
        m.TEAM_NAME AS team_name,
        m.POSITION AS position,
        TRY_TO_NUMBER(m.JERSEY_NUMBER) AS jersey_number,
        m.SERIES_NAME AS series_name,
        m.SET_NAME AS set_name
      FROM ${db}.${schema}.ALLDAY_CORE_NFT_METADATA m
      WHERE m.NFT_ID IN (${nftIdList})
    `;

        console.log(`[NFT Metadata] Fetching chunk ${Math.floor(i / SNOWFLAKE_CHUNK) + 1}/${Math.ceil(missingNftIds.length / SNOWFLAKE_CHUNK)} from Snowflake...`);
        const rows = await executeSnowflakeWithRetry(connection, sql);
        console.log(`[NFT Metadata] Got ${rows.length} rows`);

        if (rows.length === 0) continue;

        // Batch insert to PostgreSQL
        const PG_CHUNK_SIZE = 1000;

        for (let j = 0; j < rows.length; j += PG_CHUNK_SIZE) {
            const pgChunk = rows.slice(j, j + PG_CHUNK_SIZE);
            const values = [];
            const params = [];
            let paramIdx = 1;

            for (const row of pgChunk) {
                const nftId = String(row.NFT_ID ?? row.nft_id);
                if (!nftId) continue;

                values.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++})`);
                params.push(
                    nftId,
                    row.EDITION_ID ?? row.edition_id,
                    row.PLAY_ID ?? row.play_id,
                    row.SERIES_ID ?? row.series_id,
                    row.SET_ID ?? row.set_id,
                    row.TIER ?? row.tier,
                    row.SERIAL_NUMBER ?? row.serial_number,
                    row.MAX_MINT_SIZE ?? row.max_mint_size,
                    row.FIRST_NAME ?? row.first_name,
                    row.LAST_NAME ?? row.last_name,
                    row.TEAM_NAME ?? row.team_name,
                    row.POSITION ?? row.position,
                    row.JERSEY_NUMBER ?? row.jersey_number,
                    row.SERIES_NAME ?? row.series_name,
                    row.SET_NAME ?? row.set_name
                );
            }

            if (values.length === 0) continue;

            const insertSql = `
        INSERT INTO nft_core_metadata (
          nft_id, edition_id, play_id, series_id, set_id, tier,
          serial_number, max_mint_size, first_name, last_name,
          team_name, position, jersey_number, series_name, set_name
        ) VALUES ${values.join(', ')}
        ON CONFLICT (nft_id) DO UPDATE SET
          edition_id = EXCLUDED.edition_id,
          tier = EXCLUDED.tier,
          serial_number = EXCLUDED.serial_number,
          max_mint_size = EXCLUDED.max_mint_size,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          team_name = EXCLUDED.team_name,
          series_name = EXCLUDED.series_name,
          set_name = EXCLUDED.set_name
      `;

            await pgQuery(insertSql, params);
            totalInserted += pgChunk.length;
        }
    }

    const duration = Date.now() - startTime;
    await updateSyncStatus('nft_metadata', null, totalInserted, duration);

    console.log(`[NFT Metadata] ✅ Synced ${totalInserted} records in ${(duration / 1000).toFixed(1)}s`);
    return { count: totalInserted, duration };
}

async function syncHoldingsChanges(connection) {
    const startTime = Date.now();

    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    // Strategy: Just fetch all current holdings from Snowflake
    // ALLDAY_WALLET_HOLDINGS_CURRENT is already filtered to current state
    // It's small enough (~100k rows) to fetch in reasonable time
    console.log(`[Holdings] Fetching current holdings from Snowflake...`);

    const sql = `
    SELECT
      LOWER(wallet_address) AS wallet_address,
      nft_id,
      COALESCE(is_locked, FALSE) AS is_locked,
      last_event_ts
    FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
    ORDER BY wallet_address, nft_id
    LIMIT ${BATCH_SIZE}
  `;

    const rows = await executeSnowflakeWithRetry(connection, sql);
    console.log(`[Holdings] Got ${rows.length} rows from Snowflake`);

    if (rows.length === 0) {
        const duration = Date.now() - startTime;
        await updateSyncStatus('holdings', null, 0, duration);
        return { count: 0, duration };
    }

    // Batch insert to PostgreSQL
    const CHUNK_SIZE = 1000;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
        const chunk = rows.slice(i, i + CHUNK_SIZE);
        const valueLiterals = [];

        for (const row of chunk) {
            const wallet = (row.WALLET_ADDRESS ?? row.wallet_address)?.toLowerCase();
            const nftId = String(row.NFT_ID ?? row.nft_id);
            if (!wallet || !nftId) continue;

            const isLocked = Boolean(row.IS_LOCKED ?? row.is_locked);
            const ts = row.LAST_EVENT_TS ?? row.last_event_ts ?? null;

            // Build SQL literals
            const waLit = `'${wallet.replace(/'/g, "''")}'`;
            const nftLit = `'${nftId.replace(/'/g, "''")}'`;
            const lockedLit = isLocked ? "TRUE" : "FALSE";

            let tsLit = "NULL";
            if (ts) {
                const date = ts instanceof Date ? ts : new Date(ts);
                if (!isNaN(date.getTime())) {
                    tsLit = `'${date.toISOString()}'::timestamptz`;
                }
            }

            valueLiterals.push(`(${waLit}, ${nftLit}, ${lockedLit}, ${tsLit})`);
        }

        if (valueLiterals.length === 0) continue;

        const insertSql = `
      INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
      VALUES ${valueLiterals.join(',')}
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked = EXCLUDED.is_locked,
        acquired_at = COALESCE(EXCLUDED.acquired_at, holdings.acquired_at),
        last_synced_at = NOW()
    `;

        await pgQuery(insertSql);
        inserted += valueLiterals.length;

        if (inserted % 10000 === 0) {
            console.log(`[Holdings] Inserted ${inserted}/${rows.length}...`);
        }
    }

    const duration = Date.now() - startTime;
    await updateSyncStatus('holdings', null, inserted, duration);

    console.log(`[Holdings] ✅ Synced ${inserted} records in ${(duration / 1000).toFixed(1)}s`);
    return { count: inserted, duration };
}

async function syncNewAccounts(connection) {
    const startTime = Date.now();

    // Get wallets that either:
    // 1. Don't exist in wallet_profiles at all, OR
    // 2. Have NULL display_name (if --refresh-usernames flag is set)
    let query;
    if (refreshUsernames) {
        query = `
      SELECT DISTINCT h.wallet_address
      FROM holdings h
      LEFT JOIN wallet_profiles wp ON wp.wallet_address = h.wallet_address
      WHERE wp.wallet_address IS NULL
         OR wp.display_name IS NULL
      LIMIT 1000
    `;
        console.log(`[Accounts] Including wallets with NULL usernames (--refresh-usernames)`);
    } else {
        query = `
      SELECT DISTINCT h.wallet_address
      FROM holdings h
      LEFT JOIN wallet_profiles wp ON wp.wallet_address = h.wallet_address
      WHERE wp.wallet_address IS NULL
      LIMIT 1000
    `;
    }

    const result = await pgQuery(query);
    const newWallets = result.rows.map(r => r.wallet_address);
    console.log(`[Accounts] Found ${newWallets.length} wallets to process`);

    const duration = Date.now() - startTime;
    return { wallets: newWallets, count: newWallets.length, duration };
}

// ============================================================
// SINGLE WALLET SYNC (from blockchain, not Snowflake)
// ============================================================

async function syncSingleWallet(walletAddress, connection) {
    const wallet = walletAddress.toLowerCase();
    const startTime = Date.now();

    console.log(`\n=== Syncing wallet ${wallet} from blockchain ===\n`);

    // 1. Fetch NFT IDs from blockchain
    console.log(`[Wallet Sync] Fetching NFTs from Flow blockchain...`);
    let nftIds = [];
    try {
        nftIds = await flowService.getWalletNFTIds(wallet);
        console.log(`[Wallet Sync] Found ${nftIds.length} NFTs on blockchain`);
    } catch (err) {
        console.error(`[Wallet Sync] Error fetching NFTs:`, err.message);
        return { count: 0, error: err.message };
    }

    if (nftIds.length === 0) {
        console.log(`[Wallet Sync] Wallet has no NFTs`);
        return { count: 0 };
    }

    // 2. Insert/update holdings in database
    console.log(`[Wallet Sync] Updating holdings table...`);
    let holdingsInserted = 0;

    const nftIdStrings = nftIds.map(id => id.toString());
    const valueLiterals = nftIdStrings.map(nftId => {
        const waLit = `'${wallet.replace(/'/g, "''")}'`;
        const nftLit = `'${nftId.replace(/'/g, "''")}'`;
        return `(${waLit}, ${nftLit}, FALSE, NOW(), NOW())`;
    });

    if (valueLiterals.length > 0) {
        const insertSql = `
      INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
      VALUES ${valueLiterals.join(',')}
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        last_synced_at = NOW()
    `;
        await pgQuery(insertSql);
        holdingsInserted = valueLiterals.length;
        console.log(`[Wallet Sync] ✅ Inserted/updated ${holdingsInserted} holdings`);
    }

    // 3. Fetch metadata for these NFTs from Snowflake
    console.log(`[Wallet Sync] Fetching NFT metadata from Snowflake...`);
    let metadataInserted = 0;

    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    const nftIdList = nftIdStrings.map(id => `'${id}'`).join(',');
    const metadataSql = `
    SELECT
      m.NFT_ID AS nft_id,
      m.EDITION_ID AS edition_id,
      m.PLAY_ID AS play_id,
      m.SERIES_ID AS series_id,
      m.SET_ID AS set_id,
      m.TIER AS tier,
      TRY_TO_NUMBER(m.SERIAL_NUMBER) AS serial_number,
      TRY_TO_NUMBER(m.MAX_MINT_SIZE) AS max_mint_size,
      m.FIRST_NAME AS first_name,
      m.LAST_NAME AS last_name,
      m.TEAM_NAME AS team_name,
      m.POSITION AS position,
      TRY_TO_NUMBER(m.JERSEY_NUMBER) AS jersey_number,
      m.SERIES_NAME AS series_name,
      m.SET_NAME AS set_name
    FROM ${db}.${schema}.ALLDAY_CORE_NFT_METADATA m
    WHERE m.NFT_ID IN (${nftIdList})
  `;

    try {
        const rows = await executeSnowflakeWithRetry(connection, metadataSql);
        console.log(`[Wallet Sync] Got ${rows.length} metadata rows from Snowflake`);

        if (rows.length > 0) {
            const values = [];
            const params = [];
            let paramIdx = 1;

            for (const row of rows) {
                const nftId = String(row.NFT_ID ?? row.nft_id);
                if (!nftId) continue;

                values.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++})`);
                params.push(
                    nftId,
                    row.EDITION_ID ?? row.edition_id,
                    row.PLAY_ID ?? row.play_id,
                    row.SERIES_ID ?? row.series_id,
                    row.SET_ID ?? row.set_id,
                    row.TIER ?? row.tier,
                    row.SERIAL_NUMBER ?? row.serial_number,
                    row.MAX_MINT_SIZE ?? row.max_mint_size,
                    row.FIRST_NAME ?? row.first_name,
                    row.LAST_NAME ?? row.last_name,
                    row.TEAM_NAME ?? row.team_name,
                    row.POSITION ?? row.position,
                    row.JERSEY_NUMBER ?? row.jersey_number,
                    row.SERIES_NAME ?? row.series_name,
                    row.SET_NAME ?? row.set_name
                );
            }

            if (values.length > 0) {
                const insertSql = `
          INSERT INTO nft_core_metadata (
            nft_id, edition_id, play_id, series_id, set_id, tier,
            serial_number, max_mint_size, first_name, last_name,
            team_name, position, jersey_number, series_name, set_name
          ) VALUES ${values.join(', ')}
          ON CONFLICT (nft_id) DO UPDATE SET
            edition_id = EXCLUDED.edition_id,
            tier = EXCLUDED.tier,
            serial_number = EXCLUDED.serial_number,
            max_mint_size = EXCLUDED.max_mint_size,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            team_name = EXCLUDED.team_name,
            series_name = EXCLUDED.series_name,
            set_name = EXCLUDED.set_name
        `;
                await pgQuery(insertSql, params);
                metadataInserted = rows.length;
                console.log(`[Wallet Sync] ✅ Inserted/updated ${metadataInserted} metadata records`);
            }
        }
    } catch (err) {
        console.error(`[Wallet Sync] Error fetching metadata:`, err.message);
    }

    // 4. Fetch username from Dapper
    console.log(`[Wallet Sync] Fetching username from Dapper...`);
    let username = null;
    try {
        const res = await fetch(`https://open.meetdapper.com/profile?address=${wallet}`, {
            headers: { 'user-agent': 'allday-wallet-viewer/1.0' },
            signal: AbortSignal.timeout(5000)
        });
        if (res.ok) {
            const data = await res.json();
            username = data?.displayName || null;
            if (username) {
                console.log(`[Wallet Sync] ✅ Found username: ${username}`);
            }
        }
    } catch (err) {
        console.log(`[Wallet Sync] Could not fetch username: ${err.message}`);
    }

    // Update wallet_profiles
    await pgQuery(`
    INSERT INTO wallet_profiles (wallet_address, display_name, source, last_checked)
    VALUES ($1, $2, 'dapper', NOW())
    ON CONFLICT (wallet_address) DO UPDATE SET
      display_name = EXCLUDED.display_name,
      last_checked = NOW()
  `, [wallet, username]);

    const duration = Date.now() - startTime;
    console.log(`\n=== Wallet sync complete in ${(duration / 1000).toFixed(1)}s ===`);
    console.log(`   Holdings: ${holdingsInserted}`);
    console.log(`   Metadata: ${metadataInserted}`);
    console.log(`   Username: ${username || '(not found)'}\n`);

    return {
        holdings: holdingsInserted,
        metadata: metadataInserted,
        username,
        duration
    };
}

// ============================================================
// PHASE 4: FAST PRICE SYNC (Snowflake-based, no browser needed)
// ============================================================

async function syncPrices(connection) {
    const startTime = Date.now();

    console.log(`[Prices] Fetching ASP from Snowflake sale events...`);

    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    // Ensure edition_price_scrape table exists
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_scrape (
      edition_id TEXT PRIMARY KEY,
      lowest_ask_usd NUMERIC,
      avg_sale_usd NUMERIC,
      top_sale_usd NUMERIC,
      scraped_at TIMESTAMPTZ DEFAULT now()
    );
  `);

    // Get edition IDs from holdings that need prices
    const editionsResult = await pgQuery(`
    SELECT DISTINCT m.edition_id
    FROM holdings h
    JOIN nft_core_metadata m ON m.nft_id = h.nft_id
    WHERE m.edition_id IS NOT NULL
    AND m.edition_id ~ '^[0-9]+$'
  `);

    const editionIds = editionsResult.rows.map(r => r.edition_id);
    console.log(`[Prices] Found ${editionIds.length} editions to update`);

    if (editionIds.length === 0) {
        const duration = Date.now() - startTime;
        return { count: 0, duration };
    }

    // Query Snowflake for recent sale prices per edition (last 90 days)
    // Strategy: Join ListingAvailable (which has price) with ListingCompleted (which confirms purchase)
    // The price field in ListingAvailable is already in USD (no division needed)
    const editionList = editionIds.slice(0, 5000).map(id => `'${id}'`).join(',');

    const sql = `
    WITH purchases AS (
      -- Get all completed purchases (purchased=true) in last 90 days
      SELECT 
        EVENT_DATA:listingResourceID::STRING AS listing_id,
        EVENT_DATA:nftID::STRING AS nft_id
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront' 
        AND EVENT_TYPE = 'ListingCompleted'
        AND TX_SUCCEEDED = true
        AND EVENT_DATA:purchased::STRING = 'true'
        AND BLOCK_TIMESTAMP >= DATEADD(day, -90, CURRENT_TIMESTAMP())
    ),
    listings AS (
      -- Get listing prices (lookback 120 days to catch older listings)
      SELECT 
        EVENT_DATA:listingResourceID::STRING AS listing_id,
        TRY_TO_DOUBLE(EVENT_DATA:price::STRING) AS price_usd
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront' 
        AND EVENT_TYPE = 'ListingAvailable'
        AND TX_SUCCEEDED = true
        AND BLOCK_TIMESTAMP >= DATEADD(day, -120, CURRENT_TIMESTAMP())
    ),
    sales_with_prices AS (
      -- Join purchases to listings by listing_id
      SELECT 
        p.nft_id,
        l.price_usd
      FROM purchases p
      JOIN listings l ON l.listing_id = p.listing_id
      WHERE l.price_usd IS NOT NULL AND l.price_usd > 0
    ),
    edition_map AS (
      -- Map NFT IDs to Edition IDs via metadata
      SELECT 
        s.nft_id,
        s.price_usd,
        m.EDITION_ID
      FROM sales_with_prices s
      JOIN ${db}.${schema}.ALLDAY_CORE_NFT_METADATA m ON m.NFT_ID = s.nft_id
      WHERE m.EDITION_ID IN (${editionList})
    )
    SELECT
      EDITION_ID,
      AVG(price_usd) AS avg_sale_usd,
      MIN(price_usd) AS low_sale_usd,
      MAX(price_usd) AS top_sale_usd,
      COUNT(*) AS sale_count
    FROM edition_map
    GROUP BY EDITION_ID
    ORDER BY sale_count DESC
    LIMIT 5000
  `;

    try {
        const rows = await executeSnowflakeWithRetry(connection, sql);
        console.log(`[Prices] Got ASP data for ${rows.length} editions from Snowflake`);

        if (rows.length > 0) {
            // Batch insert
            const CHUNK_SIZE = 500;
            let inserted = 0;

            for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
                const chunk = rows.slice(i, i + CHUNK_SIZE);
                const values = [];
                const params = [];
                let paramIdx = 1;

                for (const row of chunk) {
                    const editionId = String(row.EDITION_ID ?? row.edition_id);
                    const avgSale = row.AVG_SALE_USD ?? row.avg_sale_usd;
                    const lowSale = row.LOW_SALE_USD ?? row.low_sale_usd;
                    const topSale = row.TOP_SALE_USD ?? row.top_sale_usd;

                    if (!editionId || !avgSale) continue;

                    values.push(`($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++})`);
                    params.push(editionId, lowSale, avgSale, topSale);
                }

                if (values.length === 0) continue;

                const insertSql = `
          INSERT INTO edition_price_scrape (edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd, scraped_at)
          VALUES ${values.join(', ').replace(/\)/g, ', NOW())')}
          ON CONFLICT (edition_id) DO UPDATE SET
            lowest_ask_usd = COALESCE(EXCLUDED.lowest_ask_usd, edition_price_scrape.lowest_ask_usd),
            avg_sale_usd = EXCLUDED.avg_sale_usd,
            top_sale_usd = COALESCE(EXCLUDED.top_sale_usd, edition_price_scrape.top_sale_usd),
            scraped_at = NOW()
        `;

                // Fix values to include NOW()
                const fixedValues = values.map((v, idx) => {
                    const baseIdx = idx * 4 + 1;
                    return `($${baseIdx}, $${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3}, NOW())`;
                });

                const fixedSql = `
          INSERT INTO edition_price_scrape (edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd, scraped_at)
          VALUES ${fixedValues.join(', ')}
          ON CONFLICT (edition_id) DO UPDATE SET
            lowest_ask_usd = COALESCE(EXCLUDED.lowest_ask_usd, edition_price_scrape.lowest_ask_usd),
            avg_sale_usd = EXCLUDED.avg_sale_usd,
            top_sale_usd = COALESCE(EXCLUDED.top_sale_usd, edition_price_scrape.top_sale_usd),
            scraped_at = NOW()
        `;

                await pgQuery(fixedSql, params);
                inserted += chunk.length;
            }

            const duration = Date.now() - startTime;
            console.log(`[Prices] ✅ Updated ${inserted} edition prices in ${(duration / 1000).toFixed(1)}s`);
            return { count: inserted, duration };
        }
    } catch (err) {
        console.error(`[Prices] Error fetching prices from Snowflake:`, err.message);
    }

    const duration = Date.now() - startTime;
    return { count: 0, duration };
}

// ============================================================
// PHASE 3: PARALLEL USERNAME SYNC
// ============================================================

async function syncUsernames(wallets) {
    if (wallets.length === 0) {
        console.log(`[Usernames] No new wallets to process`);
        return { count: 0, duration: 0 };
    }

    const startTime = Date.now();
    console.log(`[Usernames] Fetching profiles for ${wallets.length} wallets (${USERNAME_CONCURRENCY} concurrent)...`);

    let processed = 0;
    let found = 0;

    // Process in parallel batches
    for (let i = 0; i < wallets.length; i += USERNAME_CONCURRENCY) {
        const batch = wallets.slice(i, i + USERNAME_CONCURRENCY);

        await Promise.all(batch.map(async (wallet) => {
            try {
                const res = await fetch(`https://open.meetdapper.com/profile?address=${wallet}`, {
                    headers: { 'user-agent': 'allday-wallet-viewer/1.0' },
                    signal: AbortSignal.timeout(5000)
                });

                let displayName = null;
                if (res.ok) {
                    try {
                        const data = await res.json();
                        displayName = data?.displayName || null;
                        if (displayName) found++;
                    } catch (e) { }
                }

                await pgQuery(`
          INSERT INTO wallet_profiles (wallet_address, display_name, source, last_checked)
          VALUES ($1, $2, 'dapper', NOW())
          ON CONFLICT (wallet_address) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            last_checked = NOW()
        `, [wallet.toLowerCase(), displayName]);

                processed++;
            } catch (e) {
                // Skip failed requests
            }
        }));

        if (processed % 50 === 0 && processed > 0) {
            console.log(`[Usernames] Processed ${processed}/${wallets.length}...`);
        }
    }

    const duration = Date.now() - startTime;
    console.log(`[Usernames] ✅ Processed ${processed} wallets (${found} with names) in ${(duration / 1000).toFixed(1)}s`);
    return { count: processed, found, duration };
}

// ============================================================
// MAIN ORCHESTRATOR
// ============================================================

async function main() {
    console.log('');
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║           MASTER SYNC ORCHESTRATOR                       ║');
    console.log('╚══════════════════════════════════════════════════════════╝');
    console.log('');

    const totalStart = Date.now();

    // Ensure tables exist
    await ensureSyncStatusTable();

    // Create Snowflake connection
    console.log('[Master Sync] Connecting to Snowflake...');
    const connection = await createSnowflakeConnection();
    console.log('[Master Sync] ✅ Snowflake connected');

    // If --wallet= flag provided, only sync that single wallet
    if (singleWallet) {
        try {
            const result = await syncSingleWallet(singleWallet, connection);
            console.log('✅ Single wallet sync complete.');
            return;
        } finally {
            connection.destroy(() => console.log('[Master Sync] Snowflake connection closed'));
        }
    }

    const results = {
        metadata: { count: 0, duration: 0 },
        holdings: { count: 0, duration: 0 },
        accounts: { count: 0, duration: 0 },
        usernames: { count: 0, duration: 0 }
    };

    try {
        // ============================================================
        // PHASE 1: Parallel Snowflake Fetches
        // ============================================================
        console.log('');
        console.log('━━━ PHASE 1: Parallel Data Fetch ━━━');

        if (!pricesOnly) {
            const phase1Start = Date.now();

            // Run metadata and holdings in parallel
            const [metadataResult, holdingsResult] = await Promise.all([
                metadataOnly || !holdingsOnly ? syncNewNFTMetadata(connection) : { count: 0, duration: 0 },
                holdingsOnly || !metadataOnly ? syncHoldingsChanges(connection) : { count: 0, duration: 0 }
            ]);

            results.metadata = metadataResult;
            results.holdings = holdingsResult;

            console.log(`[Phase 1] Completed in ${((Date.now() - phase1Start) / 1000).toFixed(1)}s`);
        }

        // ============================================================
        // PHASE 2: Find New Accounts
        // ============================================================
        if (!metadataOnly && !pricesOnly && !holdingsOnly) {
            console.log('');
            console.log('━━━ PHASE 2: Find New Accounts ━━━');

            const accountsResult = await syncNewAccounts(connection);
            results.accounts = accountsResult;

            // ============================================================
            // PHASE 3: Username Sync
            // ============================================================
            console.log('');
            console.log('━━━ PHASE 3: Username Sync ━━━');

            results.usernames = await syncUsernames(accountsResult.wallets || []);
        }

        // ============================================================
        // PHASE 4: Price Sync
        // ============================================================
        if (!metadataOnly && !holdingsOnly) {
            console.log('');
            console.log('━━━ PHASE 4: Price Sync ━━━');

            results.prices = await syncPrices(connection);
        }

        // ============================================================
        // PHASE 5: Leaderboard Sync
        // ============================================================
        if (!metadataOnly && !pricesOnly && !holdingsOnly) {
            console.log('');
            console.log('━━━ PHASE 5: Leaderboard Sync ━━━');

            const lbResult = await syncLeaderboards();
            results.leaderboards = lbResult;
        }

    } finally {
        // Close Snowflake connection
        connection.destroy(() => {
            console.log('[Master Sync] Snowflake connection closed');
        });
    }

    // ============================================================
    // SUMMARY
    // ============================================================
    const totalDuration = Date.now() - totalStart;

    console.log('');
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║                    SYNC COMPLETE                         ║');
    console.log('╠══════════════════════════════════════════════════════════╣');
    console.log(`║  NFT Metadata:  ${String(results.metadata.count).padStart(6)} records (${(results.metadata.duration / 1000).toFixed(1)}s)`.padEnd(60) + '║');
    console.log(`║  Holdings:      ${String(results.holdings.count).padStart(6)} records (${(results.holdings.duration / 1000).toFixed(1)}s)`.padEnd(60) + '║');
    console.log(`║  New Accounts:  ${String(results.accounts.count).padStart(6)} found`.padEnd(60) + '║');
    console.log(`║  Usernames:     ${String(results.usernames.count).padStart(6)} processed (${(results.usernames.duration / 1000).toFixed(1)}s)`.padEnd(60) + '║');
    console.log(`║  Prices:        ${String(results.prices?.count || 0).padStart(6)} editions (${((results.prices?.duration || 0) / 1000).toFixed(1)}s)`.padEnd(60) + '║');
    console.log(`║  Leaderboards:  ${String(results.leaderboards?.topWallets || 0).padStart(6)} wallets (${((results.leaderboards?.elapsed || 0) / 1000).toFixed(1)}s)`.padEnd(60) + '║');
    console.log('╠══════════════════════════════════════════════════════════╣');
    console.log(`║  TOTAL TIME:    ${(totalDuration / 1000).toFixed(1)}s`.padEnd(60) + '║');
    console.log('╚══════════════════════════════════════════════════════════╝');
    console.log('');
}

main()
    .then(() => {
        console.log('✅ Master sync complete.');
        process.exit(0);
    })
    .catch((err) => {
        console.error('💥 Fatal error during master sync:', err);
        process.exit(1);
    });
