// etl_low_asks.js - scrape low asks into edition_price_stats

import * as dotenv from "dotenv";
import fetch from "node-fetch";
import { pgQuery } from "./db.js";

dotenv.config();

const CONCURRENCY = 4; // don't go crazy or NFLAD will hate you

async function getEditionIds() {
    // editions that actually appear in your local holdings/metadata
    const res = await pgQuery(`
    SELECT DISTINCT m.edition_id
    FROM nft_core_metadata m
    JOIN wallet_holdings h ON h.nft_id = m.nft_id
    WHERE m.edition_id IS NOT NULL
  `);
    return res.rows.map((r) => r.edition_id);
}

async function scrapeLowAsk(editionId) {
    const url = `https://nflallday.com/listing/moment/${editionId}`;
    const res = await fetch(url, {
        headers: { "user-agent": "Mozilla/5.0" }
    });

    if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${url}`);
    }

    const html = await res.text();

    // very dumb but usually good enough pattern
    const m =
        html.match(/(?:lowestAsk|low(?:est)?\s*ask)[^0-9$]*\$?\s*(\d[\d,]*(?:\.\d{1,2})?)/) ||
        html.match(/\$\s*(\d[\d,]*(?:\.\d{1,2})?)/);

    if (!m) return null;

    const num = Number(String(m[1]).replace(/[^0-9.]/g, ""));
    if (Number.isNaN(num)) return null;
    return num;
}

async function run() {
    try {
        const editionIds = await getEditionIds();
        console.log("Total edition_ids to scrape:", editionIds.length);
        if (!editionIds.length) {
            console.log("No edition IDs found, exiting.");
            process.exit(0);
        }

        let idx = 0;
        let updated = 0;

        async function worker(workerId) {
            while (true) {
                const myIdx = idx++;
                if (myIdx >= editionIds.length) break;

                const editionId = editionIds[myIdx];

                try {
                    const lowAsk = await scrapeLowAsk(editionId);
                    if (lowAsk != null) {
                        await pgQuery(
                            `
              INSERT INTO edition_price_stats (
                edition_id,
                low_ask,
                low_ask_ts,
                updated_at
              ) VALUES ($1, $2, NOW(), NOW())
              ON CONFLICT (edition_id) DO UPDATE SET
                low_ask    = EXCLUDED.low_ask,
                low_ask_ts = EXCLUDED.low_ask_ts,
                updated_at = EXCLUDED.updated_at
              `,
                            [editionId, lowAsk]
                        );
                        updated++;
                        if (updated % 50 === 0) {
                            console.log(`Worker ${workerId}: updated ${updated} editions so far...`);
                        }
                    }
                } catch (err) {
                    console.warn(`Worker ${workerId}: error for edition ${editionId}:`, err.message);
                }
            }
        }

        const workers = [];
        for (let i = 0; i < CONCURRENCY; i++) {
            workers.push(worker(i + 1));
        }

        await Promise.all(workers);
        console.log("Low ask ETL complete. Editions updated:", updated);
        process.exit(0);
    } catch (err) {
        console.error("Low ask ETL FAILED:", err);
        process.exit(1);
    }
}

run();
