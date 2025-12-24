import { pgQuery } from "./db.js";

async function countMissing() {
    try {
        console.log("Querying...");
        const res = await pgQuery(`
      SELECT COUNT(DISTINCT h.nft_id) 
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON h.nft_id = m.nft_id
      WHERE m.nft_id IS NULL
    `);
        console.log("RESULT:", res.rows[0].count);
    } catch (err) {
        console.error("ERROR:", err);
    } finally {
        process.exit(0);
    }
}

countMissing();
