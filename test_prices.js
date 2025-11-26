// test_prices.js
import { pgQuery } from "./db.js";

// LEGACY test for edition_price_stats. Main UI pricing uses
// public.edition_price_scrape instead.

async function run() {
    const res = await pgQuery(`
    SELECT edition_id, asp_90d, low_ask
    FROM edition_price_stats
    ORDER BY low_ask ASC NULLS LAST
    LIMIT 20;
  `);
    console.log(res.rows);
    process.exit(0);
}

run().catch((e) => {
    console.error(e);
    process.exit(1);
});
