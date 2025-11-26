// scripts/sync_wallet_holdings_from_snowflake.js
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import { pgQuery } from "../db.js";

dotenv.config();

// Keep batches small enough that a single INSERT isn't insane
// (large enough for throughput, small enough for memory / query time)
const BATCH_SIZE = 100000;

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
    // Convert to Date if it's a string
    const date = ts instanceof Date ? ts : new Date(ts);
    if (Number.isNaN(date.getTime())) return null;
    // Format as ISO 8601 string (Snowflake accepts this format)
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
}

async function syncWalletHoldings() {
    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    console.log("Using Snowflake", `${db}.${schema}`);

    const connection = await createSnowflakeConnection();
    await ensureWalletHoldingsTable();

    // Incremental sync:
    // - We keep existing rows in wallet_holdings.
    // - Only pull rows from Snowflake with last_event_ts newer than what we already have.
    // This makes repeated runs much faster than a full truncate+reload.
    const maxTsRes = await pgQuery(`
      SELECT COALESCE(MAX(last_event_ts), '2021-01-01'::timestamptz) AS max_ts
      FROM wallet_holdings;
    `);
    const maxTs = maxTsRes.rows[0]?.max_ts;
    console.log("Current max last_event_ts in Neon wallet_holdings:", maxTs);

    // Keyset pagination over (wallet_address, nft_id) instead of OFFSET/LIMIT.
    // Combined with a last_event_ts filter, this scales well as the table grows.
    let lastWallet = null;
    let lastNftId = null;
    let total = 0;

    while (true) {
        // Base filter: only rows newer than the max last_event_ts we already have.
        // Format timestamp as ISO 8601 for Snowflake compatibility
        const tsFormatted = maxTs ? formatTimestampForSnowflake(maxTs) : '2021-01-01 00:00:00';
        const tsLit = tsFormatted ? `'${escapeLiteral(tsFormatted)}'::timestamp_ntz` : `'2021-01-01 00:00:00'::timestamp_ntz`;

        let whereClause = `
        WHERE last_event_ts > ${tsLit}`;

        if (lastWallet && lastNftId) {
            const lastWalletLit = `'${escapeLiteral(lastWallet)}'`;
            const lastNftLit = `'${escapeLiteral(lastNftId)}'`;
            whereClause += `
          AND (wallet_address, nft_id) > (${lastWalletLit}, ${lastNftLit})`;
        }

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
            `Fetching holdings batch from Snowflake: last_event_ts > ${maxTs || "2021-01-01"}, ` +
                `after=(${lastWallet || "START"}, ${lastNftId || "START"}), limit=${BATCH_SIZE}...`
        );
        const rows = await executeSnowflake(connection, sql);

        console.log(`Snowflake returned ${rows.length} rows for this batch.`);
        if (!rows.length) {
            console.log("No more rows from Snowflake. Done reading.");
            break;
        }

        const valueLiterals = [];
        const seenInBatch = new Set(); // dedupe (wallet_address, nft_id) per INSERT

        for (const row of rows) {
            const waRaw = row.WALLET_ADDRESS ?? row.wallet_address;
            const walletAddress = (waRaw || "").toLowerCase();
            const nftId = String(row.NFT_ID ?? row.nft_id);
            const isLocked = Boolean(row.IS_LOCKED ?? row.is_locked);
            const ts = row.LAST_EVENT_TS ?? row.last_event_ts ?? null;

            if (!walletAddress || !nftId) continue;

            const key = `${walletAddress}|${nftId}`;
            if (seenInBatch.has(key)) {
                // skip duplicate in same batch to avoid ON CONFLICT hitting same row twice
                continue;
            }
            seenInBatch.add(key);

            const waLit = `'${escapeLiteral(walletAddress)}'`;
            const nftLit = `'${escapeLiteral(nftId)}'`;
            const lockedLit = isLocked ? "TRUE" : "FALSE";

            let tsLit = "NULL";
            if (ts) {
                const tsStr = ts instanceof Date ? ts.toISOString() : escapeLiteral(ts);
                tsLit = `'${tsStr}'::timestamptz`;
            }

            // (wallet_address, nft_id, is_locked, last_event_ts)
            valueLiterals.push(`(${waLit}, ${nftLit}, ${lockedLit}, ${tsLit})`);
        }

        if (!valueLiterals.length) {
            console.log("No valid rows in this batch after cleaning/dedupe, skipping insert.");
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
        await pgQuery(insertSql); // no params

        total += valueLiterals.length;

        // Advance keyset cursor using the last row in this batch
        const lastRow = rows[rows.length - 1];
        const waRawLast = lastRow.WALLET_ADDRESS ?? lastRow.wallet_address;
        lastWallet = (waRawLast || "").toLowerCase();
        lastNftId = String(lastRow.NFT_ID ?? lastRow.nft_id);

        console.log(
            `Upserted ${total} wallet_holdings rows so far... last=(${lastWallet}, ${lastNftId})`
        );
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
