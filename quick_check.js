// Quick verification of wallet state
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    const result = await pgQuery(`
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE is_locked = TRUE) as locked,
            COUNT(*) FILTER (WHERE is_locked = FALSE OR is_locked IS NULL) as unlocked
        FROM holdings WHERE wallet_address = $1
    `, [WALLET]);
    console.log('Wallet holdings status:', result.rows[0]);
    process.exit(0);
}
check();
