// scripts/load_edition_prices_from_csv.js
// Load edition_prices.csv into public.edition_price_scrape in Neon

import fs from "fs";
import readline from "readline";
import * as dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

if (!process.env.PGHOST) {
    throw new Error("PGHOST is not set – check your .env file");
}

const pool = new Pool({
    host: process.env.PGHOST,
    port: process.env.PGPORT || 5432,
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    const client = await pool.connect();
    try {
        // Make sure table exists (safe if you already created it in SQL)
        await client.query(`
      CREATE TABLE IF NOT EXISTS public.edition_price_scrape (
        edition_id       text PRIMARY KEY,
        lowest_ask_usd   numeric,
        avg_sale_usd     numeric,
        top_sale_usd     numeric,
        scraped_at       timestamptz DEFAULT now()
      );
    `);

        const filePath = "edition_prices.csv";
        if (!fs.existsSync(filePath)) {
            throw new Error(`CSV file not found: ${filePath}`);
        }

        console.log(`Loading prices from ${filePath}...`);

        const rl = readline.createInterface({
            input: fs.createReadStream(filePath),
            crlfDelay: Infinity
        });

        const sql = `
      INSERT INTO public.edition_price_scrape
        (edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd, scraped_at)
      VALUES ($1, $2, $3, $4, now())
      ON CONFLICT (edition_id) DO UPDATE
      SET lowest_ask_usd = EXCLUDED.lowest_ask_usd,
          avg_sale_usd   = EXCLUDED.avg_sale_usd,
          top_sale_usd   = EXCLUDED.top_sale_usd,
          scraped_at     = now();
    `;

        let lineNo = 0;
        let inserted = 0;

        for await (const line of rl) {
            lineNo++;
            if (lineNo === 1) {
                // header row: edition_id,lowest_ask,avg_sale,top_sale
                continue;
            }
            const trimmed = line.trim();
            if (!trimmed) continue;

            // CSV is simple: no commas in numbers, so split is safe
            const [edition_id, lowest_ask, avg_sale, top_sale] = trimmed.split(",");

            if (!edition_id) continue;

            const lowest = lowest_ask ? Number(lowest_ask) : null;
            const avg = avg_sale ? Number(avg_sale) : null;
            const top = top_sale ? Number(top_sale) : null;

            await client.query(sql, [edition_id, lowest, avg, top]);
            inserted++;

            if (inserted % 200 === 0) {
                console.log(`Upserted ${inserted} rows so far...`);
            }
        }

        console.log(`Done. Total rows upserted: ${inserted}`);
    } finally {
        client.release();
        await pool.end();
    }
}

main().catch((err) => {
    console.error("Fatal error loading edition prices:", err);
    process.exit(1);
});
