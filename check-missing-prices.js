import { pgQuery } from "./db.js";

async function checkMissingPrices() {
    try {
        const res = await pgQuery(`
      SELECT COUNT(DISTINCT m.edition_id) as count
      FROM wallet_holdings w
      JOIN nft_core_metadata_v2 m ON m.nft_id = w.nft_id
      LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
      WHERE m.edition_id IS NOT NULL AND eps.edition_id IS NULL
    `);
        console.log("Editions missing prices:", res.rows[0].count);

        const jb = await pgQuery("SELECT edition_id FROM nft_core_metadata_v2 WHERE nft_id = '10519438'");
        console.log("Jacoby Brissett editionId:", jb.rows[0]?.edition_id);

    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkMissingPrices();
