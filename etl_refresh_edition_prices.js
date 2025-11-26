// etl_refresh_edition_prices.js
// LEGACY: earlier pipeline that scraped Lowest Ask / Avg Sale / Top Sale
// into edition_price_stats. The main app now reads prices from
// public.edition_price_scrape (see scripts/load_edition_prices_from_csv.js
// and /api/prices). Kept for reference only.

import * as dotenv from "dotenv";
import { Pool } from "pg";
import fetch from "node-fetch";

dotenv.config();

// ---- Postgres / Neon connection ----
const pool = new Pool({
    host: process.env.PGHOST,
    port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
    ssl: process.env.PGSSL === "false" ? false : { rejectUnauthorized: false }
});

function log(...args) {
    console.log(...args);
}

// ---- Step 1: get all distinct edition IDs from metadata ----
async function getEditionIds() {
    const sql = `
    SELECT edition_id
    FROM (
      SELECT DISTINCT edition_id
      FROM nft_core_metadata
      WHERE edition_id IS NOT NULL
    ) AS t
    ORDER BY edition_id::int
  `;
    const { rows } = await pool.query(sql);
    return rows.map((r) => r.edition_id);
}

// ---- HTML helpers ----

// Very tolerant: find "label" then first $NN.NN within ~80 chars after it.
function extractMetric(html, label) {
    const idx = html.toLowerCase().indexOf(label.toLowerCase());
    if (idx === -1) return null;

    const windowText = html.slice(idx, idx + 300); // small slice after the label

    const moneyMatch = windowText.match(/\$\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/);
    if (!moneyMatch) return null;

    const num = Number(moneyMatch[1].replace(/,/g, ""));
    return Number.isFinite(num) ? num : null;
}

// ---- Step 2: scrape a single edition page ----
async function scrapeEdition(editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;

    const res = await fetch(url, {
        headers: {
            "user-agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        },
        redirect: "follow"
    });

    if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${url}`);
    }

    const html = await res.text();

    const lowAsk = extractMetric(html, "Lowest Ask");
    const asp90d = extractMetric(html, "Avg Sale");
    const topSale = extractMetric(html, "Top Sale");

    return { editionId, lowAsk, asp90d, topSale };
}

// ---- Step 3: upsert into edition_price_stats ----
async function upsertPrice({ editionId, lowAsk, asp90d, topSale }) {
    await pool.query(
        `
    INSERT INTO edition_price_stats (
      edition_id,
      low_ask,
      asp_90d,
      top_sale,
      last_updated_at
    )
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (edition_id)
    DO UPDATE SET
      low_ask         = EXCLUDED.low_ask,
      asp_90d         = EXCLUDED.asp_90d,
      top_sale        = EXCLUDED.top_sale,
      last_updated_at = NOW()
    `,
        [editionId, lowAsk, asp90d, topSale]
    );
}

// ---- Step 4: driver with mild concurrency ----
const CONCURRENCY = 500;

async function main() {
    try {
        log("Loading edition ids from nft_core_metadata…");
        const editionIds = await getEditionIds();
        log(`Found ${editionIds.length} editions to scrape.`);

        let completed = 0;
        const queue = [...editionIds];

        async function worker(workerId) {
            while (queue.length > 0) {
                const editionId = queue.shift();
                if (!editionId) break;

                try {
                    const data = await scrapeEdition(editionId);
                    await upsertPrice(data);
                    completed++;

                    if (completed % 200 === 0) {
                        log(
                            `[${completed}/${editionIds.length}] worker ${workerId} -> edition ${editionId} ` +
                                `(L=${data.lowAsk ?? "?"}, A=${data.asp90d ?? "?"}, T=${data.topSale ?? "?"})`
                        );
                    }
                } catch (err) {
                    log(`Error scraping edition ${editionId}: ${err.message}`);
                }
            }
        }

        const workers = [];
        for (let i = 1; i <= CONCURRENCY; i++) {
            workers.push(worker(i));
        }

        await Promise.all(workers);

        log("Done scraping all editions.");
    } catch (err) {
        console.error("Fatal error in etl_refresh_edition_prices:", err);
    } finally {
        await pool.end();
    }
}

main();
