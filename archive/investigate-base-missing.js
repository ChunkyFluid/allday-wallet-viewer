import { pgQuery } from "./db.js";
import fs from "fs";

async function investigate() {
    try {
        const res = await pgQuery(`
      SELECT * 
      FROM nft_core_metadata_v2 
      WHERE set_name = 'Base' 
        AND team_name = 'Kansas City Chiefs' 
        AND (first_name IS NULL OR first_name = '') 
      LIMIT 10
    `);
        fs.writeFileSync("base_missing_sample.txt", JSON.stringify(res.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

investigate();
