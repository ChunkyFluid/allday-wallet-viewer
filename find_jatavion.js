// Find specific NFT ID for Ja'Tavion Sanders #31 Regal Rookies
import { pgQuery } from './db.js';

async function find() {
    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, edition_id, set_name
        FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'sanders' 
          AND serial_number = 31 
          AND LOWER(set_name) LIKE '%regal rookies%'
    `);
    console.table(res.rows);
    process.exit(0);
}
find();
