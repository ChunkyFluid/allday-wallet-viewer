// Check ALL Rashid Shaheed NFTs in the database for this wallet
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Find ALL NFTs containing "Rashid" for this wallet
    const result = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number, m.team_name, m.edition_id
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name || ' ' || m.last_name) LIKE '%rashid%shaheed%'
        ORDER BY m.serial_number
    `, [WALLET]);

    console.log('Rashid Shaheed NFTs owned by user:');
    console.table(result.rows);
    console.log('Total:', result.rowCount);

    // Also check the #1096 specifically (from screenshot)
    const specific = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number, m.edition_id
        FROM nft_core_metadata_v2 m
        LEFT JOIN holdings h ON h.nft_id = m.nft_id AND h.wallet_address = $1
        WHERE LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
          AND m.serial_number = 1096
    `, [WALLET]);

    console.log('\nRashid Shaheed #1096 (from user screenshot):');
    console.table(specific.rows);

    process.exit(0);
}
check();
