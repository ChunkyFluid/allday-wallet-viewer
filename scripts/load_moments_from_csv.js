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

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function loadMoments() {
    const client = await pool.connect();
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "moments.csv");

    console.log("Looking for CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error("❌ moments.csv not found. Create data/moments.csv first.");
        client.release();
        await pool.end();
        process.exit(1);
    }

    console.log("Found moments.csv, starting load...");

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
            const { nft_id, edition_id, play_id, serial_number, minted_at, burned_at, current_owner } = row;

            if (!nft_id || !edition_id) {
                console.warn("Skipping row with missing nft_id or edition_id:", row);
                continue;
            }

            await client.query(
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

            count += 1;
            if (count % 100 === 0) {
                console.log(`Inserted/updated ${count} moments...`);
            }
        }

        await client.query("COMMIT");
        console.log(`✅ Done. Total moments inserted/updated: ${count}`);
    } catch (err) {
        console.error("❌ Error loading moments, rolling back:", err);
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

loadMoments().catch((err) => {
    console.error("Unexpected top-level error:", err);
    process.exit(1);
});
