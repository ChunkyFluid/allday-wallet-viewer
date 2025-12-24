import { pgQuery } from "./db.js";

async function checkEmpty() {
    try {
        const res = await pgQuery("SELECT COUNT(1) FROM nft_core_metadata_v2 WHERE (first_name IS NULL OR first_name = '') AND (team_name IS NULL OR team_name = '')");
        console.log("Both player and team missing:", res.rows[0].count);

        const sample = await pgQuery("SELECT * FROM nft_core_metadata_v2 WHERE (first_name IS NULL OR first_name = '') AND (team_name IS NULL OR team_name = '') LIMIT 5");
        console.log("Samples:", JSON.stringify(sample.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

checkEmpty();
