// scripts/load_wallet_holdings_from_csv.js
// Stream + batch insert wallet_holdings.csv into Neon using multi-row INSERTs,
// while skipping rows whose nft_id is not present in moments (to satisfy FK).

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse";
import { pgQuery } from "../db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvPath = path.join(__dirname, "..", "data", "wallet_holdings.csv");

// Tune as needed
const BATCH_SIZE = 1000;

// Build multi-row INSERT ... ON CONFLICT for wallet_holdings
function buildBatchInsert(rows) {
    const cols = ["wallet_address", "nft_id", "is_locked", "last_event_ts"];

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const r of rows) {
        values.push(`(${cols.map(() => `$${paramIndex++}`).join(", ")})`);
        params.push(r.wallet_address, r.nft_id, r.is_locked, r.last_event_ts);
    }

    const sql = `
    INSERT INTO wallet_holdings (${cols.join(", ")})
    VALUES ${values.join(", ")}
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
      is_locked     = EXCLUDED.is_locked,
      last_event_ts = EXCLUDED.last_event_ts,
      last_updated_at = NOW();
  `;

    return { sql, params };
}

// Given a batch, filter to only rows whose nft_id exists in moments
async function filterBatchByExistingMoments(batch) {
    if (!batch.length) return { filtered: [], skippedForMissingMoment: 0 };

    const uniqueNftIds = [...new Set(batch.map((r) => r.nft_id).filter(Boolean))];

    if (!uniqueNftIds.length) {
        return { filtered: [], skippedForMissingMoment: batch.length };
    }

    // Ask Postgres which of these nft_ids exist in moments
    const res = await pgQuery(
        `
      SELECT nft_id
      FROM moments
      WHERE nft_id = ANY($1::text[])
    `,
        [uniqueNftIds]
    );

    const validIds = new Set(res.rows.map((row) => String(row.nft_id)));

    const filtered = batch.filter((r) => validIds.has(r.nft_id));
    const skippedForMissingMoment = batch.length - filtered.length;

    return { filtered, skippedForMissingMoment };
}

async function loadWalletHoldings() {
    console.log("Postgres config:", {
        host: process.env.PGHOST,
        database: process.env.PGDATABASE,
        ssl: !!process.env.PGSSLMODE
    });

    if (!fs.existsSync(csvPath)) {
        console.error("❌ wallet_holdings.csv not found at:", csvPath);
        process.exit(1);
    }

    console.log("Looking for CSV at:", csvPath);
    console.log(
        "Found wallet_holdings.csv, starting load (streaming + batched inserts, verifying nft_id exists in moments)..."
    );

    const parser = fs.createReadStream(csvPath).pipe(
        parse({
            columns: (header) => header.map((h) => h.toLowerCase().trim()),
            skip_empty_lines: true,
            trim: true
        })
    );

    let batch = [];
    let totalInserted = 0;
    let totalFailedRows = 0;
    let totalSkippedMissingKeys = 0;
    let totalSkippedMissingMoment = 0;
    let rowCount = 0;

    try {
        for await (const row of parser) {
            rowCount++;

            const wallet_address = row.wallet_address && String(row.wallet_address).trim().toLowerCase();
            const nft_id = row.nft_id && String(row.nft_id).trim();

            if (!wallet_address || !nft_id) {
                totalSkippedMissingKeys++;
                continue;
            }

            const is_locked_raw =
                row.is_locked !== undefined && row.is_locked !== null ? String(row.is_locked).trim().toLowerCase() : "";
            const is_locked = is_locked_raw === "true" || is_locked_raw === "t" || is_locked_raw === "1";

            const last_event_ts =
                row.last_event_ts && String(row.last_event_ts).trim() !== "" ? row.last_event_ts : null;

            batch.push({
                wallet_address,
                nft_id,
                is_locked,
                last_event_ts
            });

            if (batch.length >= BATCH_SIZE) {
                try {
                    // Filter out rows whose nft_id does NOT exist in moments
                    const { filtered, skippedForMissingMoment } = await filterBatchByExistingMoments(batch);

                    totalSkippedMissingMoment += skippedForMissingMoment;

                    if (filtered.length > 0) {
                        const { sql, params } = buildBatchInsert(filtered);
                        await pgQuery(sql, params);
                        totalInserted += filtered.length;
                    }
                } catch (err) {
                    totalFailedRows += batch.length;
                    console.error(
                        `❌ Batch insert failed for ${batch.length} rows at rowCount=${rowCount}:`,
                        err.message || err
                    );
                }

                batch = [];

                if (totalInserted % (BATCH_SIZE * 10) === 0) {
                    console.log(
                        `Upserted ~${totalInserted} wallet holdings so far... (failed_rows: ${totalFailedRows}, skipped_missing_keys: ${totalSkippedMissingKeys}, skipped_missing_moment: ${totalSkippedMissingMoment})`
                    );
                }
            }
        }

        // Flush last partial batch
        if (batch.length > 0) {
            try {
                const { filtered, skippedForMissingMoment } = await filterBatchByExistingMoments(batch);
                totalSkippedMissingMoment += skippedForMissingMoment;

                if (filtered.length > 0) {
                    const { sql, params } = buildBatchInsert(filtered);
                    await pgQuery(sql, params);
                    totalInserted += filtered.length;
                }
            } catch (err) {
                totalFailedRows += batch.length;
                console.error(`❌ Final batch insert failed for ${batch.length} rows:`, err.message || err);
            }
        }

        console.log("==========================================");
        console.log(
            `✅ Done. Wallet holdings inserted/updated: ${totalInserted}, failed_rows: ${totalFailedRows}, skipped_missing_keys: ${totalSkippedMissingKeys}, skipped_missing_moment: ${totalSkippedMissingMoment}`
        );
        console.log("Total CSV rows seen:", rowCount);
        console.log("==========================================");
    } catch (err) {
        console.error("❌ Fatal error while streaming wallet_holdings CSV:", err);
        process.exit(1);
    }
}

loadWalletHoldings().catch((err) => {
    console.error("Unexpected top-level error:", err);
    process.exit(1);
});
