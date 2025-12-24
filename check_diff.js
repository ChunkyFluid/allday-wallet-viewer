// Check: What are the newest NFTs in holdings that aren't in wallet_holdings?
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // NFTs in holdings but NOT in wallet_holdings
    const diff = await pgQuery(`
        SELECT h.nft_id, h.acquired_at, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND h.nft_id NOT IN (SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1)
        ORDER BY h.acquired_at DESC NULLS LAST
    `, [WALLET]);
    console.log('NFTs in holdings but NOT in wallet_holdings:');
    console.table(diff.rows);

    // Also check for Rashid in metadata
    const rashid = await pgQuery(`
        SELECT nft_id, serial_number, edition_id 
        FROM nft_core_metadata_v2 
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
        LIMIT 5
    `);
    console.log('Rashid Shaheed NFTs in metadata:');
    console.table(rashid.rows);

    process.exit(0);
}
check();
