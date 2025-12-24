import { pgQuery } from "./db.js";

async function checkPlayMetadata() {
    try {
        console.log("Checking if plays with missing player names have names in other records...");

        const res = await pgQuery(`
      WITH missing_names AS (
        SELECT DISTINCT play_id
        FROM nft_core_metadata_v2
        WHERE play_id IS NOT NULL 
          AND (first_name IS NULL OR first_name = '' OR last_name IS NULL OR last_name = '')
      )
      SELECT 
        m.play_id,
        m.first_name,
        m.last_name,
        COUNT(*) as record_count
      FROM nft_core_metadata_v2 m
      JOIN missing_names mn ON m.play_id = mn.play_id
      WHERE m.first_name IS NOT NULL AND m.first_name != ''
         OR m.last_name IS NOT NULL AND m.last_name != ''
      GROUP BY m.play_id, m.first_name, m.last_name
      ORDER BY record_count DESC
      LIMIT 20
    `);

        if (res.rows.length === 0) {
            console.log("No overlap found. Every single record for these plays is missing player names.");
        } else {
            console.log("Found plays where some records have names and some don't:");
            console.table(res.rows);
        }
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkPlayMetadata();
