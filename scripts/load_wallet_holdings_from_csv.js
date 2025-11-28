// scripts/load_wallet_holdings_from_csv.js
// Stream + batch insert wallet_holdings.csv into Neon using multi-row INSERTs,
// with row-level fallback on batch failure so we can see exactly what is bad.

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

function buildBatchInsert(rows) {
  const cols = ["wallet_address", "nft_id", "is_locked", "last_event_ts"];

  const values = [];
  const params = [];
  let paramIndex = 1;

  for (const r of rows) {
    values.push(`(${cols.map(() => `$${paramIndex++}`).join(", ")})`);
    params.push(
      r.wallet_address,
      r.nft_id,
      r.is_locked,
      r.last_event_ts
    );
  }

  const sql = `
    INSERT INTO wallet_holdings (${cols.join(", ")})
    VALUES ${values.join(", ")}
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
      is_locked       = EXCLUDED.is_locked,
      last_event_ts   = EXCLUDED.last_event_ts,
      last_updated_at = NOW();
  `;

  return { sql, params };
}

async function insertSingleRow(row) {
  const sql = `
    INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
      is_locked       = EXCLUDED.is_locked,
      last_event_ts   = EXCLUDED.last_event_ts,
      last_updated_at = NOW();
  `;

  const params = [
    row.wallet_address,
    row.nft_id,
    row.is_locked,
    row.last_event_ts,
  ];

  await pgQuery(sql, params);
}

async function loadWalletHoldings() {
  console.log(
    "Postgres config:",
    {
      host: process.env.PGHOST,
      database: process.env.PGDATABASE,
      ssl: !!process.env.PGSSLMODE,
    }
  );

  if (!fs.existsSync(csvPath)) {
    console.error("❌ wallet_holdings.csv not found at:", csvPath);
    process.exit(1);
  }

  console.log("Looking for CSV at:", csvPath);
  console.log(
    "Found wallet_holdings.csv, starting load (streaming + batched inserts with row-level fallback)..."
  );

  const parser = fs
    .createReadStream(csvPath)
    .pipe(
      parse({
        columns: (header) => header.map((h) => h.toLowerCase().trim()),
        skip_empty_lines: true,
        trim: true,
      })
    );

  let batch = [];
  let totalInserted = 0;
  let totalBatchFailures = 0;
  let totalRowFailures = 0;
  let totalSkippedMissingKeys = 0;
  let rowCount = 0;
  let batchNumber = 0;

  try {
    for await (const row of parser) {
      rowCount++;

      const wallet_address =
        row.wallet_address && String(row.wallet_address).trim().toLowerCase();
      const nft_id = row.nft_id && String(row.nft_id).trim();

      if (!wallet_address || !nft_id) {
        totalSkippedMissingKeys++;
        continue;
      }

      const is_locked_raw =
        row.is_locked !== undefined && row.is_locked !== null
          ? String(row.is_locked).trim().toLowerCase()
          : "";
      const is_locked =
        is_locked_raw === "true" ||
        is_locked_raw === "t" ||
        is_locked_raw === "1";

      const last_event_ts =
        row.last_event_ts && String(row.last_event_ts).trim() !== ""
          ? row.last_event_ts
          : null;

      batch.push({
        wallet_address,
        nft_id,
        is_locked,
        last_event_ts,
      });

      if (batch.length >= BATCH_SIZE) {
        batchNumber++;
        await flushBatch(batch, batchNumber, {
          totalInsertedRef: (n) => { totalInserted += n; },
          totalBatchFailuresRef: () => { totalBatchFailures++; },
          totalRowFailuresRef: (n) => { totalRowFailures += n; },
        });
        batch = [];

        console.log(
          `After batch #${batchNumber}: inserted=${totalInserted}, batch_failures=${totalBatchFailures}, ` +
          `row_failures=${totalRowFailures}, skipped_missing_keys=${totalSkippedMissingKeys}, rows_seen=${rowCount}`
        );
      }
    }

    // Flush last partial batch
    if (batch.length > 0) {
      batchNumber++;
      await flushBatch(batch, batchNumber, {
        totalInsertedRef: (n) => { totalInserted += n; },
        totalBatchFailuresRef: () => { totalBatchFailures++; },
        totalRowFailuresRef: (n) => { totalRowFailures += n; },
      });
      console.log(
        `After final batch #${batchNumber}: inserted=${totalInserted}, batch_failures=${totalBatchFailures}, ` +
        `row_failures=${totalRowFailures}, skipped_missing_keys=${totalSkippedMissingKeys}, rows_seen=${rowCount}`
      );
    }

    console.log("==========================================");
    console.log(
      `✅ Done. Wallet holdings inserted/updated: ${totalInserted}, batch_failures: ${totalBatchFailures}, ` +
      `row_failures: ${totalRowFailures}, skipped_missing_keys: ${totalSkippedMissingKeys}`
    );
    console.log("Total CSV rows seen:", rowCount);
    console.log("==========================================");
  } catch (err) {
    console.error("❌ Fatal error while streaming wallet_holdings CSV:", err);
    process.exit(1);
  }
}

async function flushBatch(batch, batchNumber, refs) {
  const { totalInsertedRef, totalBatchFailuresRef, totalRowFailuresRef } = refs;

  try {
    const { sql, params } = buildBatchInsert(batch);
    await pgQuery(sql, params);
    totalInsertedRef(batch.length);
  } catch (err) {
    totalBatchFailuresRef();
    console.error(
      `❌ Batch #${batchNumber} insert failed for ${batch.length} rows:`,
      err.message || err
    );

    // Row-by-row fallback to salvage good rows and log bad ones
    let batchInserted = 0;
    let batchRowFailures = 0;

    for (const r of batch) {
      try {
        await insertSingleRow(r);
        batchInserted++;
      } catch (rowErr) {
        batchRowFailures++;
        console.error(
          `   ↳ Row failed wallet=${r.wallet_address} nft_id=${r.nft_id}:`,
          rowErr.message || rowErr
        );
      }
    }

    totalInsertedRef(batchInserted);
    totalRowFailuresRef(batchRowFailures);

    console.log(
      `   ↳ Batch #${batchNumber} fallback summary: inserted=${batchInserted}, row_failures=${batchRowFailures}`
    );
  }
}

loadWalletHoldings().catch((err) => {
  console.error("Unexpected top-level error:", err);
  process.exit(1);
});
