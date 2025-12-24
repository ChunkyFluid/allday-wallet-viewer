// Find Rashid Shaheed #1096 (from user's screenshot) and check ownership
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Find the specific NFT ID for Rashid Shaheed #1096
    const rashid = await pgQuery(`
        SELECT nft_id, serial_number, edition_id 
        FROM nft_core_metadata_v2 
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
          AND serial_number = 1096
    `);
    console.log('Rashid Shaheed #1096 NFT:');
    console.table(rashid.rows);

    if (rashid.rows.length > 0) {
        const nftId = rashid.rows[0].nft_id;

        // Check if this NFT is in holdings for the user
        const owned = await pgQuery(`
            SELECT * FROM holdings WHERE nft_id = $1
        `, [nftId]);
        console.log('Ownership in holdings table:');
        console.table(owned.rows);

        // Check wallet_holdings too
        const legacyOwned = await pgQuery(`
            SELECT * FROM wallet_holdings WHERE nft_id = $1
        `, [nftId]);
        console.log('Ownership in wallet_holdings table:');
        console.table(legacyOwned.rows);
    } else {
        console.log('Rashid Shaheed #1096 NOT FOUND in metadata!');
        console.log('This means the metadata was never synced for this serial number.');
    }

    process.exit(0);
}
check();
