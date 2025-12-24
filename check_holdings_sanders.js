// Check holdings table for Shedeur Sanders #31
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    console.log('=== HOLDINGS TABLE CHECK ===\n');

    const res = await pgQuery(`
        SELECT h.nft_id, h.is_locked, h.acquired_at, m.first_name, m.last_name, m.serial_number, m.set_name
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.last_name) = 'sanders'
          AND m.serial_number = 31
    `, [WALLET]);

    console.log(`Found ${res.rowCount} entries in holdings table with serial #31:`);
    console.table(res.rows);

    process.exit(0);
}
check();
