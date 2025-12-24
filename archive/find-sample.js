import { pgQuery } from "./db.js";

async function findSample() {
    try {
        const res = await pgQuery(`
      SELECT nft_id 
      FROM nft_core_metadata_v2 
      WHERE set_name = 'Souvenir' AND team_name = 'Los Angeles Rams' 
      LIMIT 1
    `);
        console.log(res.rows[0].nft_id);
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

findSample();
