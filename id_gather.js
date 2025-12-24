// Gather definitive NFT IDs for correction
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function gather() {
    console.log('=== GATHERING Definitive NFT IDs ===\n');

    // 1. Rashid Shaheed #1096
    const rashid = await pgQuery(`
        SELECT nft_id FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'shaheed' AND serial_number = 1096
    `);
    console.log(`Rashid #1096 NFT ID: ${rashid.rows[0]?.nft_id || 'NOT FOUND'}`);

    // 2. Joe Mixon #2504
    const mixon = await pgQuery(`
        SELECT nft_id FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'mixon' AND serial_number = 2504
    `);
    console.log(`Joe Mixon #2504 NFT ID: ${mixon.rows[0]?.nft_id || 'NOT FOUND'}`);

    // 3. Shedeur Sanders (all)
    const shedeur = await pgQuery(`
        SELECT nft_id, serial_number FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'shedeur' AND LOWER(last_name) = 'sanders'
    `);
    console.log(`\nShedeur Sanders NFT IDs (${shedeur.rowCount}):`);
    console.table(shedeur.rows);

    // 4. Ja'Tavion Sanders #31
    const jatavion = await pgQuery(`
        SELECT nft_id FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'ja\\'tavion' AND LOWER(last_name) = 'sanders' AND serial_number = 31
    `);
    console.log(`\nJa'Tavion Sanders #31 NFT ID: ${jatavion.rows[0]?.nft_id || 'NOT FOUND'}`);

    process.exit(0);
}
gather();
