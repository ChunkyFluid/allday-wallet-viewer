// scripts/sync_prices_from_scrape.js
// Scrape Lowest Ask and Avg Sale from nflallday.com listing pages
// and upsert into Neon edition_price_stats, with verbose logging.

import * as dotenv from "dotenv";
dotenv.config();

import { pgQuery } from "../db.js";
import fetch from "node-fetch";
import { setTimeout as delay } from "timers/promises";

async function ensureEditionPriceTable() {
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_stats (
      edition_id TEXT PRIMARY KEY,
      asp_90d NUMERIC,
      low_ask NUMERIC,
      last_updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function getEditionIdsToUpdate() {
    const sql = `
    SELECT DISTINCT m.edition_id
    FROM wallet_holdings w
    JOIN moments m ON m.nft_id = w.nft_id
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

function extractPricesFromHtml(html) {
    const normalized = html.replace(/\s+/g, " ");

    let lowAsk = null;
    let asp = null;

    // Examples in HTML:
    // "Lowest Ask $6.00"
    // "Avg Sale $87.95"
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

    return { lowAsk, asp90d: asp };
}

async function fetchPricesForEdition(editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;
    try {
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
            console.warn(`  [HTTP ${res.status}] edition_id=${editionId} url=${url}`);
            return { lowAsk: null, asp90d: null };
        }

        const html = await res.text();
        return extractPricesFromHtml(html);
    } catch (err) {
        console.warn(`  [fetch error] edition_id=${editionId}:`, err.message);
        return { lowAsk: null, asp90d: null };
    }
}

async function upsertPriceBatch(batch) {
    if (!batch.length) return;

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

    // DEBUG SAFETY: only do the first N editions for now
    const MAX_EDITIONS = 50; // bump this later once we see it working
    const limitedEditionIds = editionIds.slice(0, MAX_EDITIONS);

    console.log(`Starting price scrape for first ${limitedEditionIds.length} editions...`);

    const batchForDb = [];
    let processed = 0;
    const total = limitedEditionIds.length;

    const DB_BATCH_SIZE = 10;
    const DELAY_MS = 300; // a bit faster but still polite

    for (const editionId of limitedEditionIds) {
        console.log(`- [${processed + 1}/${total}] Fetching edition_id=${editionId}...`);

        const { lowAsk, asp90d } = await fetchPricesForEdition(editionId);

        console.log(`  Parsed edition_id=${editionId} => lowAsk=${lowAsk}, asp90d=${asp90d}`);

        if (lowAsk != null || asp90d != null) {
            batchForDb.push({
                edition_id: editionId,
                asp_90d: asp90d,
                low_ask: lowAsk
            });
        }

        processed += 1;

        if (batchForDb.length >= DB_BATCH_SIZE) {
            await upsertPriceBatch(batchForDb);
            console.log(
                `  ✅ Upserted ${batchForDb.length} rows into edition_price_stats (processed ${processed}/${total}).`
            );
            batchForDb.length = 0;
        }

        await delay(DELAY_MS);
    }

    if (batchForDb.length > 0) {
        await upsertPriceBatch(batchForDb);
        console.log(
            `  ✅ Upserted final ${batchForDb.length} rows into edition_price_stats (processed ${processed}/${total}).`
        );
    }

    console.log("✅ Finished scraping and syncing prices into edition_price_stats for first batch.");
    process.exit(0);
}

main().catch((err) => {
    console.error("💥 Fatal error during price sync:", err);
    process.exit(1);
});
