import { pgQuery } from "./db.js";

async function verifyDraft() {
    try {
        const res = await pgQuery(`
      SELECT 
        nft_id, 
        first_name, 
        last_name, 
        team_name, 
        set_name, 
        COALESCE(NULLIF(TRIM(first_name || ' ' || last_name), ''), team_name, set_name, '(unknown)') AS player_name 
      FROM nft_core_metadata_v2 
      WHERE nft_id = '9442408'
    `);
        console.log("DRAFT_VERIFICATION_RESULT:");
        console.log(JSON.stringify(res.rows[0], null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

verifyDraft();
