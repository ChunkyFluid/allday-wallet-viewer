// Check if Rashid Shaheed #1096 from Series 2 (edition 1228) is owned by the user
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Find ALL Rashid Shaheed moments in holdings for ANY wallet
    const allRashid = await pgQuery(`
        SELECT h.nft_id, h.wallet_address, m.serial_number, m.edition_id, h.acquired_at
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
        ORDER BY h.acquired_at DESC NULLS LAST
        LIMIT 20
    `);
    console.log('Recent Rashid Shaheed ownership in holdings:');
    console.table(allRashid.rows);

    // Check if ANY Rashid Shaheed is owned by user
    const userRashid = await pgQuery(`
        SELECT h.nft_id, m.serial_number, m.edition_id, h.acquired_at
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
    `, [WALLET]);
    console.log('\\nRashid Shaheed owned by user wallet:');
    console.table(userRashid.rows);

    if (userRashid.rows.length === 0) {
        console.log('\\n*** FINDING: User does NOT have Rashid in holdings table! ***');
        console.log('This means the live event listener missed the Deposit event for this NFT.');
    }

    process.exit(0);
}
check();
