// scripts/load_edition_price_stats_from_otm.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse";
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const csvPath = path.join(__dirname, "..", "data", "all_day_values.csv");

console.log("Postgres config:", {
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    ssl: process.env.PGSSLMODE || "require"
});

if (!fs.existsSync(csvPath)) {
    console.error("❌ CSV not found at:", csvPath);
    process.exit(1);
}

const statsByEdition = new Map();

function parseMoney(str) {
    if (!str) return null;
    const cleaned = String(str).replace(/[^0-9.]/g, "");
    if (!cleaned) return null;
    const num = Number(cleaned);
    return Number.isNaN(num) ? null : num;
}

console.log("Reading CSV from:", csvPath);

const parser = fs.createReadStream(csvPath).pipe(
    parse({
        columns: true,
        trim: true,
        skip_empty_lines: true
    })
);

parser.on("data", (row) => {
    try {
        const link = row["Link"] || row["link"];
        if (!link) return;
        const m = String(link).match(/edition\/(\d+)/);
        if (!m) return;
        const editionId = m[1]; // e.g. "2275"

        const lowAskStr = row["Low Ask"] || row["low ask"];
        const aspStr = row["Average Sale Price"] || row["Average Sale Price (All Day)"] || row["avg_sale_price"];

        const lowAsk = parseMoney(lowAskStr);
        const asp = parseMoney(aspStr);

        let existing = statsByEdition.get(editionId);
        if (!existing) {
            existing = { edition_id: editionId, asp_90d: null, low_ask: null };
        }

        // Low ask – keep the smallest non-null value
        if (lowAsk != null) {
            if (existing.low_ask == null || lowAsk < existing.low_ask) {
                existing.low_ask = lowAsk;
            }
        }

        // ASP – just overwrite (snapshot, so they should all match anyway)
        if (asp != null) {
            existing.asp_90d = asp;
        }

        statsByEdition.set(editionId, existing);
    } catch (err) {
        // row-level errors are ignored
    }
});

parser.on("error", (err) => {
    console.error("❌ CSV parse error:", err);
    process.exit(1);
});

parser.on("end", async () => {
    console.log(`Finished reading CSV. Unique editions: ${statsByEdition.size}`);

    if (statsByEdition.size === 0) {
        console.log("No edition stats parsed, aborting.");
        process.exit(0);
    }

    // Ensure table exists
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_stats (
      edition_id TEXT PRIMARY KEY,
      asp_90d NUMERIC,
      low_ask NUMERIC,
      last_updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);

    const entries = Array.from(statsByEdition.values());
    const batchSize = 500;
    let upserted = 0;

    for (let i = 0; i < entries.length; i += batchSize) {
        const batch = entries.slice(i, i + batchSize);
        const values = [];
        const params = [];
        let idx = 1;

        for (const row of batch) {
            params.push(row.edition_id, row.asp_90d, row.low_ask);
            values.push(`($${idx++}, $${idx++}, $${idx++})`);
        }

        const sql = `
      INSERT INTO edition_price_stats (edition_id, asp_90d, low_ask)
      VALUES ${values.join(", ")}
      ON CONFLICT (edition_id) DO UPDATE
      SET asp_90d = EXCLUDED.asp_90d,
          low_ask = EXCLUDED.low_ask,
          last_updated_at = now();
    `;

        await pgQuery(sql, params);
        upserted += batch.length;
        console.log(`Upserted ${upserted} / ${entries.length} edition_price_stats rows...`);
    }

    console.log("✅ Finished upserting edition_price_stats.");
    process.exit(0);
});
