import { pgQuery } from './db.js';

async function check() {
    try {
        const res = await pgQuery(`
            SELECT nft_id, TO_CHAR(acquired_at, 'YYYY-MM-DD HH24:MI:SS') as acquired_at 
            FROM holdings 
            WHERE wallet_address = '0x4edf4847ecac8a3c' 
            AND acquired_at >= '2025-12-22' 
            ORDER BY acquired_at DESC
        `);
        console.log(res.rows);
    } catch (err) {
        console.error(err);
    } finally {
        process.exit();
    }
}

check();
