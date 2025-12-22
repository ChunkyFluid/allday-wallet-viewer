import { pgQuery } from "./db.js";
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import { setTimeout as delay } from "timers/promises";
import * as dotenv from "dotenv";
dotenv.config();

function extractPricesFromText(text) {
    const normalized = text.replace(/\s+/g, " ");
    let lowAsk = null;
    let asp = null;
    let topSale = null;

    const lowAskMatch = normalized.match(/Lowest Ask\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (lowAskMatch) {
        const cleaned = lowAskMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) lowAsk = num;
    }

    const aspMatch = normalized.match(/Avg Sale\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (aspMatch) {
        const cleaned = aspMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) asp = num;
    }

    const topMatch = normalized.match(/Top Sale\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)/i);
    if (topMatch) {
        const cleaned = topMatch[1].replace(/,/g, "");
        const num = Number(cleaned);
        if (!Number.isNaN(num)) topSale = num;
    }

    return { lowAsk, asp, topSale };
}

async function fetchPrices(page, editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;
    try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
        await page.waitForTimeout(3000); // Give it time to load

        const text = await page.content();
        let prices = extractPricesFromText(text);

        if (prices.lowAsk == null && prices.asp == null) {
            const bodyText = await page.evaluate(() => document.body.innerText);
            prices = extractPricesFromText(bodyText);
        }

        return prices;
    } catch (err) {
        console.warn(`  [Error] edition_id=${editionId}:`, err.message);
        return { lowAsk: null, asp: null, topSale: null };
    }
}

async function main() {
    try {
        // 1. Get editions missing prices
        const res = await pgQuery(`
      SELECT m.edition_id
      FROM wallet_holdings w
      JOIN nft_core_metadata_v2 m ON m.nft_id = w.nft_id
      LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
      WHERE m.edition_id IS NOT NULL AND eps.edition_id IS NULL
      GROUP BY m.edition_id
      ORDER BY m.edition_id::INTEGER DESC
    `);

        const editionIds = res.rows.map(r => r.edition_id);
        console.log(`Found ${editionIds.length} editions missing prices.`);

        if (editionIds.length === 0) return;

        const browser = await chromium.launch({ headless: true });
        const context = await browser.newContext();
        const page = await context.newPage();

        for (let i = 0; i < editionIds.length; i++) {
            const eid = editionIds[i];
            console.log(`[${i + 1}/${editionIds.length}] Scraping edition ${eid}...`);

            const prices = await fetchPrices(page, eid);

            if (prices.lowAsk !== null || prices.asp !== null || prices.topSale !== null) {
                console.log(`  -> Found: Low=$${prices.lowAsk}, ASP=$${prices.asp}, Top=$${prices.topSale}`);
                await pgQuery(`
                INSERT INTO edition_price_scrape (edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (edition_id) DO UPDATE SET
                    lowest_ask_usd = EXCLUDED.lowest_ask_usd,
                    avg_sale_usd = EXCLUDED.avg_sale_usd,
                    top_sale_usd = COALESCE(EXCLUDED.top_sale_usd, edition_price_scrape.top_sale_usd),
                    scraped_at = now()
            `, [eid, prices.lowAsk, prices.asp, prices.topSale]);
            } else {
                console.log(`  -> No prices found.`);
            }

            await delay(500); // Faster delay

            // Safety break
            if (i >= 200) {
                console.log("Reached safety limit of 200 editions. Stopping.");
                break;
            }
        }

        await browser.close();
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

main();
