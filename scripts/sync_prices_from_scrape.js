// scripts/sync_prices_from_scrape.js
// Scrape Lowest Ask and Avg Sale from nflallday.com listing pages
// and upsert into Render edition_price_scrape (used by wallet/leaderboards).
// Uses Playwright to render pages (pricing is client-side). If data/nflad-auth.json
// exists, it will use that storage state to stay logged in; otherwise it will try unauth.

import * as dotenv from "dotenv";
dotenv.config();

import { pgQuery } from "../db.js";
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { setTimeout as delay } from "timers/promises";

async function ensureEditionPriceTable() {
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_scrape (
      edition_id TEXT PRIMARY KEY,
      lowest_ask_usd NUMERIC,
      avg_sale_usd NUMERIC,
      top_sale_usd NUMERIC,
      scraped_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function getEditionIdsToUpdate() {
    const sql = `
    SELECT DISTINCT m.edition_id
    FROM wallet_holdings w
    JOIN nft_core_metadata_v2 m ON m.nft_id = w.nft_id
    WHERE m.edition_id IS NOT NULL
    ORDER BY m.edition_id;
  `;

    const result = await pgQuery(sql, []);
    const editionIds = [];

    for (const row of result.rows) {
        const eid = String(row.edition_id).trim();
        if (/^\d+$/.test(eid)) {
            editionIds.push(eid);
        }
    }

    console.log(`Found ${editionIds.length} distinct numeric edition_ids from wallet_holdings.`);
    return editionIds;
}

function extractPricesFromText(text) {
    const normalized = text.replace(/\s+/g, " ");
    let lowAsk = null;
    let asp = null;
    let topSale = null;

    const lowAskMatch = normalized.match(/Lowest Ask\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (lowAskMatch) {
        const cleaned = lowAskMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) {
            lowAsk = num;
        }
    }

    const aspMatch = normalized.match(/Avg Sale\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (aspMatch) {
        const cleaned = aspMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) {
            asp = num;
        }
    }

    const topMatch = normalized.match(/Top Sale\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (topMatch) {
        const cleaned = topMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) {
            topSale = num;
        }
    }

    return { lowAsk, asp90d: asp, topSale };
}

async function fetchPricesWithPlaywright(page, editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;
    try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
        // Wait a bit for client-side render
        await page.waitForTimeout(2000);

        const text = await page.content();
        const { lowAsk, asp90d } = extractPricesFromText(text);

        // Try a fallback: grab visible text
        if (lowAsk == null && asp90d == null) {
            const bodyText = await page.evaluate(() => document.body.innerText);
            const parsed = extractPricesFromText(bodyText);
            return parsed;
        }

        return { lowAsk, asp90d };
    } catch (err) {
        console.warn(`  [playwright error] edition_id=${editionId}:`, err.message);
        return { lowAsk: null, asp90d: null };
    }
}

async function upsertPriceBatch(batch) {
    if (!batch.length) return;

    const values = [];
    const params = [];
    let idx = 1;

    for (const row of batch) {
        params.push(row.edition_id, row.low_ask, row.asp_90d, row.top_sale);
        values.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++})`);
    }

    const sql = `
    INSERT INTO edition_price_scrape (edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd)
    VALUES ${values.join(", ")}
    ON CONFLICT (edition_id) DO UPDATE
    SET lowest_ask_usd = EXCLUDED.lowest_ask_usd,
        avg_sale_usd    = EXCLUDED.avg_sale_usd,
        top_sale_usd    = COALESCE(EXCLUDED.top_sale_usd, edition_price_scrape.top_sale_usd),
        scraped_at      = now();
  `;

    await pgQuery(sql, params);
}

async function main() {
    console.log("Postgres config:", {
        host: process.env.PGHOST,
        database: process.env.PGDATABASE,
        ssl: process.env.PGSSLMODE || "require"
    });

    await ensureEditionPriceTable();
    const editionIds = await getEditionIdsToUpdate();

    if (!editionIds.length) {
        console.log("No edition_ids found to update, exiting.");
        process.exit(0);
    }

    // Cap via env if desired; default to all editions found.
    const MAX_EDITIONS = process.env.PRICE_MAX_EDITIONS
        ? parseInt(process.env.PRICE_MAX_EDITIONS, 10)
        : editionIds.length;
    const limitedEditionIds = editionIds.slice(0, MAX_EDITIONS);

    console.log(`Starting price scrape for first ${limitedEditionIds.length} editions...`);

    const total = limitedEditionIds.length;
    const DB_BATCH_SIZE = 50;
    const DELAY_MS = parseInt(process.env.PRICE_DELAY_MS || "100", 10);
    const CONCURRENCY = parseInt(process.env.PRICE_CONCURRENCY || "10", 10);

    // Playwright setup
    const storagePath = path.join(process.cwd(), "data", "nflad-auth.json");
    const hasStorage = fs.existsSync(storagePath);
    const browser = await chromium.launch({ headless: true });
    const context = hasStorage
        ? await browser.newContext({ storageState: storagePath })
        : await browser.newContext();

    let cursor = 0;
    let processed = 0;
    let found = 0;

    async function getNextId() {
        if (cursor >= total) return null;
        const id = limitedEditionIds[cursor];
        cursor += 1;
        return id;
    }

    async function worker(workerId) {
        const page = await context.newPage();
        const batch = [];
        while (true) {
            const editionId = await getNextId();
            if (!editionId) break;

            const { lowAsk, asp90d, topSale } = await fetchPricesWithPlaywright(page, editionId);

            if (lowAsk != null || asp90d != null || topSale != null) {
                batch.push({
                    edition_id: editionId,
                    asp_90d: asp90d,
                    low_ask: lowAsk,
                    top_sale: topSale
                });
                found += 1;
            }

            processed += 1;

            if (batch.length >= DB_BATCH_SIZE) {
                await upsertPriceBatch(batch.splice(0, batch.length));
            }

            if (processed % 200 === 0) {
                console.log(
                    `[worker ${workerId}] progress ${processed}/${total}, found ${found} with prices`
                );
            }

            await delay(DELAY_MS);
        }

        if (batch.length) {
            await upsertPriceBatch(batch);
        }

        await page.close();
    }

    const workers = [];
    const start = Date.now();
    for (let i = 0; i < CONCURRENCY; i++) {
        workers.push(worker(i + 1));
    }
    await Promise.all(workers);

    await browser.close();

    console.log(
        `✅ Finished scraping. Processed ${processed}/${total}, wrote ~${found} priced editions in ${((Date.now() - start) / 1000).toFixed(1)
        }s`
    );
    process.exit(0);
}

main().catch((err) => {
    console.error("💥 Fatal error during price sync:", err);
    process.exit(1);
});
