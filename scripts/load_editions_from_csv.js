// scripts/load_editions_from_csv.js
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

const BATCH_SIZE = 1000;

async function loadEditions() {
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "editions.csv");

    console.log("Looking for CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error("❌ editions.csv not found. Create data/editions.csv first.");
        await pool.end();
        process.exit(1);
    }

    console.log("Found editions.csv, starting load (row-by-row, headers forced to lowercase)...");

    const stream = fs.createReadStream(filePath).pipe(
        parse({
            columns: (header) => {
                const cols = header.map((h) => String(h).trim().toLowerCase());
                console.log("CSV header columns (editions):", cols);
                return cols;
            },
            skip_empty_lines: true,
            trim: true
        })
    );

    let count = 0;
    let failed = 0;
    let batch = [];

    try {
        for await (const row of stream) {
            const edition_id = row.edition_id;
            const set_id = row.set_id || null;
            const set_name = row.set_name || null;
            const series_id = row.series_id || null;
            const series_name = row.series_name || null;
            const tier = row.tier || null;
            const max_mint_size = row.max_mint_size ? Number(row.max_mint_size) : null;

            if (!edition_id) {
                console.warn("Skipping row with missing edition_id:", row);
                continue;
            }

            batch.push({
                edition_id,
                set_id,
                set_name,
                series_id,
                series_name,
                tier,
                max_mint_size
            });

            if (batch.length >= BATCH_SIZE) {
                try {
                    const values = [];
                    const params = [];
                    let idx = 1;

                    for (const b of batch) {
                        values.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
                        params.push(
                            b.edition_id,
                            b.set_id,
                            b.set_name,
                            b.series_id,
                            b.series_name,
                            b.tier,
                            b.max_mint_size
                        );
                    }

                    const sql = `
          INSERT INTO editions (
            edition_id,
            set_id,
            set_name,
            series_id,
            series_name,
            tier,
            max_mint_size
          )
          VALUES ${values.join(",")}
          ON CONFLICT (edition_id)
          DO UPDATE SET
            set_id = EXCLUDED.set_id,
            set_name = EXCLUDED.set_name,
            series_id = EXCLUDED.series_id,
            series_name = EXCLUDED.series_name,
            tier = EXCLUDED.tier,
            max_mint_size = EXCLUDED.max_mint_size
          `;

                    await pool.query(sql, params);
                    count += batch.length;
                } catch (err) {
                    failed += batch.length;
                    console.error(
                        `Batch upsert failed for ${batch.length} editions: ${err.code || ""} ${
                            err.message || String(err)
                        }`
                    );
                }

                batch = [];

                if (count % 1000 === 0) {
                    console.log(`Upserted ${count} editions so far... (failed: ${failed})`);
                }
            }
        }

        // Flush remaining batch
        if (batch.length > 0) {
            try {
                const values = [];
                const params = [];
                let idx = 1;

                for (const b of batch) {
                    values.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
                    params.push(
                        b.edition_id,
                        b.set_id,
                        b.set_name,
                        b.series_id,
                        b.series_name,
                        b.tier,
                        b.max_mint_size
                    );
                }

                const sql = `
        INSERT INTO editions (
          edition_id,
          set_id,
          set_name,
          series_id,
          series_name,
          tier,
          max_mint_size
        )
        VALUES ${values.join(",")}
        ON CONFLICT (edition_id)
        DO UPDATE SET
          set_id = EXCLUDED.set_id,
          set_name = EXCLUDED.set_name,
          series_id = EXCLUDED.series_id,
          series_name = EXCLUDED.series_name,
          tier = EXCLUDED.tier,
          max_mint_size = EXCLUDED.max_mint_size
        `;

                await pool.query(sql, params);
                count += batch.length;
            } catch (err) {
                failed += batch.length;
                console.error(
                    `Final batch upsert failed for ${batch.length} editions: ${err.code || ""} ${
                        err.message || String(err)
                    }`
                );
            }
        }

        console.log(`✅ Done. Total editions inserted/updated: ${count}, failures: ${failed}`);
    } catch (err) {
        console.error("❌ Fatal error while streaming editions CSV:", err);
    } finally {
        await pool.end();
    }
}

loadEditions().catch((err) => {
    console.error("Unexpected top-level error in loadEditions:", err);
    process.exit(1);
});
