// scripts/sync_wallet_holdings_from_snowflake.js
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { executeSnowflakeWithRetry, delay, createSnowflakeConnection } from "./snowflake-utils.js";

dotenv.config();

const BATCH_SIZE = 100000;
const BATCH_DELAY_MS = parseInt(process.env.SNOWFLAKE_BATCH_DELAY_MS || '500', 10); // Delay between batches

// Check for --incremental flag
const isIncremental = process.argv.includes('--incremental');

function escapeLiteral(str) {
    if (str == null) return null;
    return String(str).replace(/'/g, "''");
}

function formatTimestampForSnowflake(ts) {
    if (!ts) return null;
    const date = ts instanceof Date ? ts : new Date(ts);
    if (Number.isNaN(date.getTime())) return null;
    
    // Validate timestamp is within reasonable range (1970 to 2100)
    const minDate = new Date('1970-01-01');
    const maxDate = new Date('2100-01-01');
    if (date < minDate || date > maxDate) {
        console.warn(`⚠️  Timestamp out of range: ${ts}, skipping`);
        return null;
    }
    
    return date.toISOString().replace('T', ' ').replace('Z', '');
}

async function ensureWalletHoldingsTable() {
    console.log("Ensuring Render wallet_holdings table exists...");
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_holdings (
      wallet_address TEXT NOT NULL,
      nft_id        TEXT NOT NULL,
      is_locked     BOOLEAN NOT NULL DEFAULT FALSE,
      last_event_ts TIMESTAMPTZ,
      last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (wallet_address, nft_id)
    );
  `);

    const before = await pgQuery(`SELECT COUNT(*) AS c FROM wallet_holdings;`);
    console.log("Current Render wallet_holdings row count (before sync):", before.rows[0].c);
    return parseInt(before.rows[0].c, 10);
}

async function syncWalletHoldings() {
    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    console.log("Using Snowflake", `${db}.${schema}`);
    console.log("Mode:", isIncremental ? "INCREMENTAL (only new changes)" : "FULL REFRESH");

    const connection = await createSnowflakeConnection();
    const existingCount = await ensureWalletHoldingsTable();

    let minTimestamp = null;

    if (isIncremental && existingCount > 0) {
        // Get the max timestamp from existing data
        // Make sure to handle invalid timestamps gracefully
        const maxTsRes = await pgQuery(`
            SELECT COALESCE(MAX(last_event_ts), '2021-01-01'::timestamptz) AS max_ts
            FROM wallet_holdings
            WHERE last_event_ts IS NOT NULL 
            AND last_event_ts >= '2021-01-01'::timestamptz
            AND last_event_ts <= NOW();
        `);
        minTimestamp = maxTsRes.rows[0]?.max_ts;
        
        // Validate timestamp is reasonable (not in the future and not too old)
        if (minTimestamp) {
            const ts = new Date(minTimestamp);
            const now = new Date();
            const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
            
            // If timestamp is invalid, too old, or in the future, use a default
            if (isNaN(ts.getTime()) || ts > now || ts < oneYearAgo) {
                console.log(`⚠️  Invalid timestamp found (${minTimestamp}), using default: 2021-01-01`);
                minTimestamp = new Date('2021-01-01');
            }
        }
        
        console.log("Incremental sync: fetching records since", minTimestamp);
    } else {
        // Full refresh - truncate and reload
        console.log("Truncating wallet_holdings for full refresh...");
        await pgQuery(`TRUNCATE TABLE wallet_holdings;`);
    }

    let lastWallet = null;
    let lastNftId = null;
    let total = 0;

    while (true) {
        let whereConditions = [];
        
        // Filter out invalid timestamps at the SQL level to prevent Snowflake proto errors
        // Only include rows with valid timestamps or NULL
        whereConditions.push(`(
            last_event_ts IS NULL 
            OR (last_event_ts >= '1970-01-01'::timestamp_ntz AND last_event_ts <= '2100-01-01'::timestamp_ntz)
        )`);
        
        // For incremental, filter by timestamp
        // Include records where last_event_ts > minTimestamp OR last_event_ts IS NULL
        // (NULL timestamps need to be synced too)
        if (minTimestamp) {
            const tsFormatted = formatTimestampForSnowflake(minTimestamp);
            if (tsFormatted) {
                whereConditions.push(`(
                    last_event_ts IS NULL 
                    OR last_event_ts > '${escapeLiteral(tsFormatted)}'::timestamp_ntz
                )`);
            }
        }
        
        // Keyset pagination
        if (lastWallet && lastNftId) {
            const lastWalletLit = `'${escapeLiteral(lastWallet)}'`;
            const lastNftLit = `'${escapeLiteral(lastNftId)}'`;
            whereConditions.push(`(wallet_address, nft_id) > (${lastWalletLit}, ${lastNftLit})`);
        }

        const whereClause = whereConditions.length > 0 
            ? `WHERE ${whereConditions.join(' AND ')}` 
            : '';

        // Use TRY_TO_TIMESTAMP_NTZ to handle invalid timestamps gracefully
        // This prevents Snowflake proto errors when encountering bad timestamp data
        const sql = `
      SELECT
        wallet_address,
        nft_id,
        is_locked,
        CASE 
          WHEN last_event_ts IS NULL THEN NULL
          WHEN TRY_TO_TIMESTAMP_NTZ(last_event_ts::STRING) IS NULL THEN NULL
          WHEN TRY_TO_TIMESTAMP_NTZ(last_event_ts::STRING) < '1970-01-01'::timestamp_ntz THEN NULL
          WHEN TRY_TO_TIMESTAMP_NTZ(last_event_ts::STRING) > '2100-01-01'::timestamp_ntz THEN NULL
          ELSE TRY_TO_TIMESTAMP_NTZ(last_event_ts::STRING)
        END AS last_event_ts
      FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
      ${whereClause}
      ORDER BY wallet_address, nft_id
      LIMIT ${BATCH_SIZE};
    `;

        console.log(
            `Fetching holdings batch: after=(${lastWallet || "START"}, ${lastNftId || "START"}), limit=${BATCH_SIZE}...`
        );
        console.log(`⏳ Querying Snowflake... (this may take 30-60 seconds for large queries)`);
        const queryStartTime = Date.now();
        
        let rows;
        try {
            rows = await executeSnowflakeWithRetry(connection, sql);
        } catch (err) {
            // If query fails due to timestamp/proto errors, try without timestamp column
            const errorMsg = (err.message || err.code || '').toLowerCase();
            if (errorMsg.includes('timestamp') || errorMsg.includes('proto') || errorMsg.includes('out of range')) {
                console.warn(`⚠️  Timestamp serialization error detected, retrying without timestamp column...`);
                console.warn(`   Error: ${err.message || err.code}`);
                
                // Build fallback query without timestamp filters
                const fallbackWhereConditions = [];
                if (lastWallet && lastNftId) {
                    const lastWalletLit = `'${escapeLiteral(lastWallet)}'`;
                    const lastNftLit = `'${escapeLiteral(lastNftId)}'`;
                    fallbackWhereConditions.push(`(wallet_address, nft_id) > (${lastWalletLit}, ${lastNftLit})`);
                }
                const fallbackWhereClause = fallbackWhereConditions.length > 0 
                    ? `WHERE ${fallbackWhereConditions.join(' AND ')}` 
                    : '';
                
                // Try query without timestamp to avoid proto serialization issues
                const fallbackSql = `
                  SELECT
                    wallet_address,
                    nft_id,
                    is_locked,
                    NULL AS last_event_ts
                  FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
                  ${fallbackWhereClause}
                  ORDER BY wallet_address, nft_id
                  LIMIT ${BATCH_SIZE};
                `;
                try {
                    rows = await executeSnowflakeWithRetry(connection, fallbackSql);
                    console.warn(`⚠️  Using fallback query (no timestamps) due to Snowflake timestamp errors`);
                } catch (retryErr) {
                    console.error(`❌ Fallback query also failed:`, retryErr.message);
                    throw err; // Throw original error
                }
            } else {
                throw err; // Re-throw non-timestamp errors
            }
        }
        
        const queryDuration = ((Date.now() - queryStartTime) / 1000).toFixed(1);
        console.log(`✅ Snowflake query completed in ${queryDuration} seconds`);

        console.log(`Snowflake returned ${rows.length} rows for this batch.`);
        if (!rows.length) {
            console.log("No more rows from Snowflake. Done reading.");
            break;
        }

        const valueLiterals = [];
        const seenInBatch = new Set();

        for (const row of rows) {
            const waRaw = row.WALLET_ADDRESS ?? row.wallet_address;
            const walletAddress = (waRaw || "").toLowerCase();
            const nftId = String(row.NFT_ID ?? row.nft_id);
            const isLocked = Boolean(row.IS_LOCKED ?? row.is_locked);
            const ts = row.LAST_EVENT_TS ?? row.last_event_ts ?? null;

            if (!walletAddress || !nftId) continue;

            const key = `${walletAddress}|${nftId}`;
            if (seenInBatch.has(key)) continue;
            seenInBatch.add(key);

            const waLit = `'${escapeLiteral(walletAddress)}'`;
            const nftLit = `'${escapeLiteral(nftId)}'`;
            const lockedLit = isLocked ? "TRUE" : "FALSE";

            let tsLit = "NULL";
            if (ts) {
                // Validate timestamp before using it
                let date = null;
                if (ts instanceof Date) {
                    date = ts;
                } else if (typeof ts === 'string' || typeof ts === 'number') {
                    date = new Date(ts);
                }
                
                if (date && !isNaN(date.getTime())) {
                    // Check if timestamp is in reasonable range (1970 to 2100)
                    const minDate = new Date('1970-01-01');
                    const maxDate = new Date('2100-01-01');
                    if (date >= minDate && date <= maxDate) {
                        const tsStr = date.toISOString();
                        tsLit = `'${escapeLiteral(tsStr)}'::timestamptz`;
                    } else {
                        // Invalid timestamp - use NULL
                        console.warn(`⚠️  Skipping invalid timestamp from Snowflake: ${ts}`);
                        tsLit = "NULL";
                    }
                } else {
                    // Invalid date - use NULL
                    tsLit = "NULL";
                }
            }

            valueLiterals.push(`(${waLit}, ${nftLit}, ${lockedLit}, ${tsLit})`);
        }

        if (!valueLiterals.length) {
            console.log("No valid rows in this batch, skipping insert.");
            continue;
        }

        const insertSql = `
      INSERT INTO wallet_holdings (
        wallet_address,
        nft_id,
        is_locked,
        last_event_ts
      )
      VALUES ${valueLiterals.join(",")}
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked      = EXCLUDED.is_locked,
        last_event_ts  = EXCLUDED.last_event_ts,
        last_synced_at = now();
    `;

        console.log(`Inserting ${valueLiterals.length} rows into Render...`);
        await pgQuery(insertSql);

        total += valueLiterals.length;

        // Advance cursor
        const lastRow = rows[rows.length - 1];
        const waRawLast = lastRow.WALLET_ADDRESS ?? lastRow.wallet_address;
        lastWallet = (waRawLast || "").toLowerCase();
        lastNftId = String(lastRow.NFT_ID ?? lastRow.nft_id);

        console.log(`Upserted ${total} wallet_holdings rows so far... last=(${lastWallet}, ${lastNftId})`);
        
        // Add delay between batches to reduce load on Snowflake
        if (BATCH_DELAY_MS > 0) {
            await delay(BATCH_DELAY_MS);
        }
    }

    const after = await pgQuery(`SELECT COUNT(*) AS c FROM wallet_holdings;`);
    console.log("✅ Final Render wallet_holdings row count:", after.rows[0].c);

    connection.destroy(() => {
        console.log("Snowflake connection closed.");
    });
}

syncWalletHoldings()
    .then(() => {
        console.log("✅ Wallet holdings sync complete.");
        process.exit(0);
    })
    .catch((err) => {
        console.error("💥 Fatal error during holdings sync:", err);
        process.exit(1);
    });
