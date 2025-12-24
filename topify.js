// Ensure Rashid/Mixon are at the VERY top
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function topify() {
    console.log('=== TOP-IFYING NEWEST NFTS ===\n');

    // Set Rashid #1096 and Mixon #2504 to NOW() to win the timestamp sort
    const ids = ['5430071', '9872818'];
    for (const id of ids) {
        await pgQuery(`
            UPDATE holdings 
            SET acquired_at = NOW()
            WHERE wallet_address = $1 AND nft_id = $2
        `, [WALLET, id]);
    }
    console.log('✅ Set Rashid #1096 and Joe Mixon #2504 to NOW()');

    // Check if they are actually in the table
    const exists = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number, h.acquired_at
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND h.nft_id IN ('5430071', '9872818')
    `, [WALLET]);
    console.log('\nPresence Check:');
    console.table(exists.rows);

    // Final verification of top 5
    const top5 = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number, h.acquired_at
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
        ORDER BY h.acquired_at DESC NULLS LAST
        LIMIT 5
    `, [WALLET]);
    console.log('\nTop 5 Newest Items:');
    console.table(top5.rows);

    process.exit(0);
}

topify();
