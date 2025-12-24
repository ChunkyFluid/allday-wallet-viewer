// Final check: Are they in holdings? If not, add them NOW
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function finalFix() {
    // Check if Rashid #1096 is in holdings
    const rashid = await pgQuery(`
        SELECT h.nft_id FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid'
          AND LOWER(m.last_name) = 'shaheed'
          AND m.serial_number = 1096
    `, [WALLET]);

    console.log('Rashid #1096 in holdings:', rashid.rowCount > 0 ? 'YES ✅' : 'NO ❌');

    if (rashid.rowCount === 0) {
        await pgQuery(`
            INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
            VALUES ($1, '4099792', FALSE, CURRENT_DATE)
            ON CONFLICT (wallet_address, nft_id) DO NOTHING
        `, [WALLET]);
        console.log('✅ Added Rashid #1096 to holdings');
    }

    // Check Mixon
    const mixon = await pgQuery(`
        SELECT h.nft_id FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'joe'
          AND LOWER(m.last_name) = 'mixon'
          AND m.serial_number = 2504
    `, [WALLET]);

    console.log('Joe Mixon #2504 in holdings:', mixon.rowCount > 0 ? 'YES ✅' : 'NO ❌');

    // Final count
    const count = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`\nTotal holdings: ${count.rows[0].c}`);

    process.exit(0);
}

finalFix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
