// scripts/load_moments_from_csv.js
// Stream + batch insert moments.csv into Neon using multi-row INSERTs.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse";
import { pgQuery } from "../db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Where the CSV is expected
const csvPath = path.join(__dirname, "..", "data", "moments.csv");

// Tune this if needed; bigger = fewer SQL calls, but larger statements
const BATCH_SIZE = 1000;

function buildBatchInsert(rows) {
  // Columns in the moments table
  const cols = [
    "nft_id",
    "edition_id",
    "play_id",
    "serial_number",
    "minted_at",
    "burned_at",
    "current_owner",
  ];

  const values = [];
  const params = [];
  let paramIndex = 1;

  for (const r of rows) {
    values.push(
      `(${cols.map(() => `$${paramIndex++}`).join(", ")})`
    );
    params.push(
      r.nft_id,
      r.edition_id,
      r.play_id,
      r.serial_number,
      r.minted_at,
      r.burned_at,
      r.current_owner
    );
  }

  const sql = `
    INSERT INTO moments (${cols.join(", ")})
    VALUES ${values.join(", ")}
    ON CONFLICT (nft_id) DO UPDATE SET
      edition_id   = EXCLUDED.edition_id,
      play_id      = EXCLUDED.play_id,
      serial_number = EXCLUDED.serial_number,
      minted_at    = EXCLUDED.minted_at,
      burned_at    = EXCLUDED.burned_at,
      current_owner = EXCLUDED.current_owner;
  `;

  return { sql, params };
}

async function loadMoments() {
  if (!fs.existsSync(csvPath)) {
    console.error("❌ moments.csv not found at:", csvPath);
    process.exit(1);
  }

  console.log("Looking for CSV at:", csvPath);
  console.log(
    "Found moments.csv, starting load (streaming + batched inserts)..."
  );

  const parser = fs
    .createReadStream(csvPath)
    .pipe(
      parse({
        // Force headers to lowercase and trimmed
        columns: (header) => header.map((h) => h.toLowerCase().trim()),
        skip_empty_lines: true,
        trim: true,
      })
    );

  let batch = [];
  let totalInserted = 0;
  let totalFailed = 0;
  let totalSkippedMissingKeys = 0;
  let rowCount = 0;

  try {
    for await (const row of parser) {
      rowCount++;

      const nft_id = row.nft_id && String(row.nft_id).trim();
      const edition_id = row.edition_id && String(row.edition_id).trim();

      if (!nft_id || !edition_id) {
        totalSkippedMissingKeys++;
        continue;
      }

      const play_id = row.play_id ? String(row.play_id).trim() : null;
      const serial_number =
        row.serial_number && row.serial_number !== ""
          ? Number(row.serial_number)
          : null;
      const minted_at = row.minted_at || null;
      const burned_at = row.burned_at || null;
      const current_owner =
        row.current_owner && String(row.current_owner).trim() !== ""
          ? String(row.current_owner).trim()
          : null;

      batch.push({
        nft_id,
        edition_id,
        play_id,
        serial_number,
        minted_at,
        burned_at,
        current_owner,
      });

      if (batch.length >= BATCH_SIZE) {
        try {
          const { sql, params } = buildBatchInsert(batch);
          await pgQuery(sql, params);
          totalInserted += batch.length;
        } catch (err) {
          totalFailed += batch.length;
          console.error(
            `❌ Batch insert failed for ${batch.length} rows at rowCount=${rowCount}:`,
            err.message || err
          );
        }
        batch = [];

        if (totalInserted % (BATCH_SIZE * 10) === 0) {
          console.log(
            `Upserted ~${totalInserted} moments so far... (failed: ${totalFailed}, skipped_missing_keys: ${totalSkippedMissingKeys})`
          );
        }
      }
    }

    // Flush final partial batch
    if (batch.length > 0) {
      try {
        const { sql, params } = buildBatchInsert(batch);
        await pgQuery(sql, params);
        totalInserted += batch.length;
      } catch (err) {
        totalFailed += batch.length;
        console.error(
          `❌ Final batch insert failed for ${batch.length} rows:`,
          err.message || err
        );
      }
    }

    console.log("==========================================");
    console.log(
      `✅ Done. Total moments inserted/updated: ${totalInserted}, failed_batches_rows: ${totalFailed}, skipped_missing_keys: ${totalSkippedMissingKeys}`
    );
    console.log("Total CSV rows seen:", rowCount);
    console.log("==========================================");
  } catch (err) {
    console.error("❌ Fatal error while streaming CSV:", err);
    process.exit(1);
  } finally {
    // Let pg pool close in db.js if needed; otherwise we could explicitly end it
    // but pgQuery is using a shared pool, so just exit.
  }
}

loadMoments();
