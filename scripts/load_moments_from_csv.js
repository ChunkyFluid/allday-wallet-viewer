// scripts/load_moments_from_csv.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";
import dotenv from "dotenv";
import { parse } from "csv-parse";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
    host: process.env.PGHOST,
    port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
    ssl: { rejectUnauthorized: false }
});

pool.on("error", (err) => {
    console.error("Postgres pool error:", err);
});

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function loadMoments() {
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "moments.csv");

    console.log("Looking for CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error("❌ moments.csv not found. Create data/moments.csv first.");
        await pool.end();
        process.exit(1);
    }

    console.log("Found moments.csv, starting load (row-by-row, headers forced to lowercase)...");

    // Force all header names to lowercase so row.nft_id / row.edition_id work
const stream = fs.createReadStream(filePath).pipe(
  parse({
    columns: (header) => {
      const cols = header.map((h) => String(h).trim().toLowerCase());
      console.log("CSV header columns:", cols);
      return cols;
    },
    skip_empty_lines: true,
    trim: true
  })
);


    let count = 0;
    let failed = 0;

    try {
        for await (const row of stream) {
            const nft_id = row.nft_id;
            const edition_id = row.edition_id;
            const play_id = row.play_id;
            const serial_number = row.serial_number;
            const minted_at = row.minted_at;
            const burned_at = row.burned_at;
            const current_owner = row.current_owner;

            if (!nft_id || !edition_id) {
                console.warn("Skipping row with missing nft_id or edition_id:", row);
                continue;
            }

            try {
                await pool.query(
                    `
          INSERT INTO moments (
            nft_id,
            edition_id,
            play_id,
            serial_number,
            minted_at,
            burned_at,
            current_owner,
            updated_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
          ON CONFLICT (nft_id)
          DO UPDATE SET
            edition_id = EXCLUDED.edition_id,
            play_id = EXCLUDED.play_id,
            serial_number = EXCLUDED.serial_number,
            minted_at = EXCLUDED.minted_at,
            burned_at = EXCLUDED.burned_at,
            current_owner = EXCLUDED.current_owner,
            updated_at = NOW()
          `,
                    [
                        nft_id,
                        edition_id,
                        play_id || null,
                        serial_number ? Number(serial_number) : null,
                        minted_at || null,
                        burned_at || null,
                        current_owner || null
                    ]
                );
            } catch (err) {
                failed += 1;
                console.error(`Row failed for nft_id=${nft_id}: ${err.code || ""} ${err.message || String(err)}`);
            }

            count += 1;
            if (count % 1000 === 0) {
                console.log(`Upserted ${count} moments so far... (failed: ${failed})`);
            }
        }

        console.log(`✅ Done. Total moments inserted/updated: ${count}, failures: ${failed}`);
    } catch (err) {
        console.error("❌ Fatal error while streaming CSV:", err);
    } finally {
        await pool.end();
    }
}

loadMoments().catch((err) => {
    console.error("Unexpected top-level error:", err);
    process.exit(1);
});
