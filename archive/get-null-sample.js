import { pgQuery } from "./db.js";
import fs from "fs";

async function investigate() {
    try {
        // Get 50 NFTs that have NULL names in our DB
        const res = await pgQuery(`
      SELECT nft_id, set_name, team_name 
      FROM nft_core_metadata_v2 
      WHERE (first_name IS NULL OR first_name = '') 
      LIMIT 50
    `);
        fs.writeFileSync("null_names_sample.json", JSON.stringify(res.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

investigate();
