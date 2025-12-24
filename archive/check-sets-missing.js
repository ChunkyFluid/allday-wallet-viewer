import { pgQuery } from "./db.js";
import fs from "fs";

async function checkSets() {
    try {
        const res = await pgQuery(`
      SELECT 
        set_name, 
        team_name, 
        COUNT(*) as missing_count
      FROM nft_core_metadata_v2
      WHERE (first_name IS NULL OR first_name = '' OR last_name IS NULL OR last_name = '')
      GROUP BY set_name, team_name
      ORDER BY missing_count DESC
      LIMIT 100
    `);
        fs.writeFileSync("sets_missing_results.txt", JSON.stringify(res.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkSets();
