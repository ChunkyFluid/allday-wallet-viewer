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

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function loadEditions() {
    const client = await pool.connect();
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "editions.csv");

    console.log("Looking for CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error("❌ editions.csv not found. Create data/editions.csv first.");
        client.release();
        await pool.end();
        process.exit(1);
    }

    console.log("Found editions.csv, starting load...");

    const stream = fs.createReadStream(filePath).pipe(
        parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        })
    );

    try {
        await client.query("BEGIN");

        let count = 0;

        for await (const row of stream) {
            const { edition_id, set_id, set_name, series_id, series_name, tier, max_mint_size } = row;

            if (!edition_id) {
                console.warn("Skipping row with missing edition_id:", row);
                continue;
            }

            await client.query(
                `
        INSERT INTO editions (
          edition_id,
          set_id,
          set_name,
          series_id,
          series_name,
          tier,
          max_mint_size,
          updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT (edition_id)
        DO UPDATE SET
          set_id = EXCLUDED.set_id,
          set_name = EXCLUDED.set_name,
          series_id = EXCLUDED.series_id,
          series_name = EXCLUDED.series_name,
          tier = EXCLUDED.tier,
          max_mint_size = EXCLUDED.max_mint_size,
          updated_at = NOW()
        `,
                [
                    edition_id,
                    set_id || null,
                    set_name || null,
                    series_id || null,
                    series_name || null,
                    tier || null,
                    max_mint_size ? Number(max_mint_size) : null
                ]
            );

            count += 1;
            if (count % 100 === 0) {
                console.log(`Inserted/updated ${count} editions...`);
            }
        }

        await client.query("COMMIT");
        console.log(`✅ Done. Total editions inserted/updated: ${count}`);
    } catch (err) {
        console.error("❌ Error loading editions, rolling back:", err);
        try {
            await client.query("ROLLBACK");
        } catch (rollbackErr) {
            console.error("Rollback failed:", rollbackErr);
        }
    } finally {
        client.release();
        await pool.end();
    }
}

loadEditions().catch((err) => {
    console.error("Unexpected top-level error:", err);
    process.exit(1);
});
