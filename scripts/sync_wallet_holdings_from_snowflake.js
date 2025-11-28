// scripts/sync_wallet_holdings_from_snowflake.js
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import { pgQuery } from "../db.js";

dotenv.config();

const BATCH_SIZE = 100000;

// Check for --incremental flag
const isIncremental = process.argv.includes('--incremental');

function createSnowflakeConnection() {
    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    return new Promise((resolve, reject) => {
        connection.connect((err, conn) => {
            if (err) {
                console.error("❌ Snowflake connect error:", err);
                return reject(err);
            }
            console.log("✅ Connected to Snowflake as", conn.getId());
            resolve(connection);
        });
    });
}

function executeSnowflake(connection, sqlText) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText,
            complete(err, stmt, rows) {
                if (err) {
                    console.error("❌ Snowflake query error:", err);
                    return reject(err);
                }
                resolve(rows || []);
            }
        });
    });
}

function escapeLiteral(str) {
    if (str == null) return null;
    return String(str).replace(/'/g, "''");
}

function formatTimestampForSnowflake(ts) {
    if (!ts) return null;
    const date = ts instanceof Date ? ts : new Date(ts);
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString().replace('T', ' ').replace('Z', '');
}

async function ensureWalletHoldingsTable() {
    console.log("Ensuring Neon wallet_holdings table exists...");
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
    console.log("Current Neon wallet_holdings row count (before sync):", before.rows[0].c);
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
        const maxTsRes = await pgQuery(`
            SELECT COALESCE(MAX(last_event_ts), '2021-01-01'::timestamptz) AS max_ts
            FROM wallet_holdings;
        `);
        minTimestamp = maxTsRes.rows[0]?.max_ts;
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
        
        // For incremental, filter by timestamp
        if (minTimestamp) {
            const tsFormatted = formatTimestampForSnowflake(minTimestamp);
            whereConditions.push(`last_event_ts > '${escapeLiteral(tsFormatted)}'::timestamp_ntz`);
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

        const sql = `
      SELECT
        wallet_address,
        nft_id,
        is_locked,
        last_event_ts
      FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
      ${whereClause}
      ORDER BY wallet_address, nft_id
      LIMIT ${BATCH_SIZE};
    `;

        console.log(
            `Fetching holdings batch: after=(${lastWallet || "START"}, ${lastNftId || "START"}), limit=${BATCH_SIZE}...`
        );
        const rows = await executeSnowflake(connection, sql);

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
                const tsStr = ts instanceof Date ? ts.toISOString() : escapeLiteral(ts);
                tsLit = `'${tsStr}'::timestamptz`;
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

        console.log(`Inserting ${valueLiterals.length} rows into Neon...`);
        await pgQuery(insertSql);

        total += valueLiterals.length;

        // Advance cursor
        const lastRow = rows[rows.length - 1];
        const waRawLast = lastRow.WALLET_ADDRESS ?? lastRow.wallet_address;
        lastWallet = (waRawLast || "").toLowerCase();
        lastNftId = String(lastRow.NFT_ID ?? lastRow.nft_id);

        console.log(`Upserted ${total} wallet_holdings rows so far... last=(${lastWallet}, ${lastNftId})`);
    }

    const after = await pgQuery(`SELECT COUNT(*) AS c FROM wallet_holdings;`);
    console.log("✅ Final Neon wallet_holdings row count:", after.rows[0].c);

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
