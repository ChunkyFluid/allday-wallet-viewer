import { pgQuery } from './db.js';

async function checkKaladin() {
    try {
        console.log("Checking holdings for 0x93914b2bfb28d59d (Kaladin49)...");
        const res = await pgQuery(`
            SELECT 
                h.nft_id, 
                TO_CHAR(h.acquired_at, 'YYYY-MM-DD HH24:MI:SS') as acquired_at
            FROM holdings h 
            WHERE h.wallet_address = '0x93914b2bfb28d59d' 
            ORDER BY h.acquired_at DESC 
            LIMIT 50
        `);
        console.table(res.rows);
    } catch (err) {
        console.error(err);
    } finally {
        process.exit();
    }
}

checkKaladin();
