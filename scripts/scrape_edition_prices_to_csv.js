// scripts/scrape_edition_prices_to_csv.js
// Scrape Lowest Ask / Avg Sale / Top Sale for every edition
// and write them to edition_prices.csv

import fs from "fs";
import * as dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

// --- Postgres (Neon) connection ---
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

// Tiny sleep helper so we don't hammer the site
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Parse a labelled USD price like "Lowest Ask $7.00" from the HTML
function extractPrice(labelRegex, html) {
    // Look for e.g. "Lowest Ask" then anything, then a $ and a number
    const re = new RegExp(labelRegex.source + "[^$]*\\$\\s*([0-9][0-9,]*(?:\\.\\d{1,2})?)", "i");
    const m = html.match(re);
    if (!m) return null;

    const raw = m[1];
    const num = Number(String(raw).replace(/[, ]/g, ""));
    return Number.isNaN(num) ? null : num;
}

// Grab prices from a single listing page
async function fetchEditionPrices(editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;

    const res = await fetch(url, {
        headers: {
            // Fake a browser UA so they don't just 403 us
            "user-agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            accept: "text/html,application/xhtml+xml"
        }
    });

    if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${url}`);
    }

    const html = await res.text();

    const lowestAsk = extractPrice(/Lowest\s+Ask/i, html);
    const avgSale = extractPrice(/Avg(?:erage)?\s+Sale/i, html);
    const topSale = extractPrice(/Top\s+Sale/i, html);

    return { lowestAsk, avgSale, topSale };
}

// Main runner
async function main() {
    console.log("Connecting to Neon and loading edition IDs...");

    const { rows } = await pool.query(
        `SELECT edition_id
     FROM public.editions
     ORDER BY edition_id`
    );

    const editionIds = rows.map((r) => r.edition_id);
    console.log(`Found ${editionIds.length} editions.`);

    // CSV output
    const outPath = "edition_prices.csv";
    const out = fs.createWriteStream(outPath, { encoding: "utf8" });
    out.write("edition_id,lowest_ask,avg_sale,top_sale\n");

    // Concurrency control: how many pages to hit in parallel
    const CONCURRENCY = 25;
    let index = 0;
    let successCount = 0;
    let errorCount = 0;

    async function worker(workerId) {
        while (true) {
            const i = index++;
            if (i >= editionIds.length) break;

            const editionId = editionIds[i];

            try {
                const { lowestAsk, avgSale, topSale } = await fetchEditionPrices(editionId);

                out.write([editionId, lowestAsk ?? "", avgSale ?? "", topSale ?? ""].join(",") + "\n");

                successCount++;
                if ((successCount + errorCount) % 50 === 0) {
                    console.log(
                        `[${successCount + errorCount}/${editionIds.length}] ` +
                            `worker ${workerId} -> edition ${editionId} ` +
                            `(L=${lowestAsk ?? "?"}, A=${avgSale ?? "?"}, T=${topSale ?? "?"})`
                    );
                }
            } catch (err) {
                errorCount++;
                console.error(`Error scraping edition ${editionId}: ${err.message || err}`);
                // Still write a row so we keep the CSV aligned
                out.write([editionId, "", "", ""].join(",") + "\n");
            }

            // Small delay so we don't blast the site too hard
            await sleep(500);
        }
    }

    console.log(
        `Starting scrape with concurrency=${CONCURRENCY}. This will take a while for ${editionIds.length} editions.`
    );

    const workers = [];
    for (let w = 0; w < CONCURRENCY; w++) {
        workers.push(worker(w + 1));
    }

    await Promise.all(workers);

    out.end();
    await pool.end();

    console.log(`Done. Success: ${successCount}, Errors: ${errorCount}. CSV written to ${outPath}`);
}

main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
