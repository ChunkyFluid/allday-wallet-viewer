// Check specific NFTs that user said have wrong dates
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Check Rashid Shaheed #1096
    const rashid = await pgQuery(`
        SELECT h.nft_id, h.acquired_at, h.is_locked, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
    `, [WALLET]);
    console.log('Rashid Shaheed NFTs:');
    console.table(rashid.rows);

    // Check NFTs with today's date (12/24/2024)
    const todayDates = await pgQuery(`
        SELECT COUNT(*) as c FROM holdings 
        WHERE wallet_address = $1 
          AND acquired_at::date = CURRENT_DATE
    `, [WALLET]);
    console.log('\nNFTs with today date:', todayDates.rows[0].c);

    // Check Myles Garrett and Ja'Tavion Sanders (from screenshot)
    const check = await pgQuery(`
        SELECT h.nft_id, h.acquired_at, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND (LOWER(m.last_name) = 'garrett' OR LOWER(m.last_name) = 'sanders')
        ORDER BY h.acquired_at DESC
        LIMIT 10
    `, [WALLET]);
    console.log('\nGarrett/Sanders NFTs:');
    console.table(check.rows);

    // Check total wallet overview
    const overview = await pgQuery(`
        SELECT COUNT(*) as total,
               COUNT(*) FILTER (WHERE is_locked) as locked,
               COUNT(*) FILTER (WHERE acquired_at::date = CURRENT_DATE) as today_date
        FROM holdings WHERE wallet_address = $1
    `, [WALLET]);
    console.log('\nWallet overview:', overview.rows[0]);

    process.exit(0);
}
check();
