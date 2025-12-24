// Deep check for Ja'Tavion Sanders across all possible tables
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function deepCheck() {
    console.log('=== DEEP CHECK FOR JA\'TAVION SANDERS ===\n');

    // Find his NFT IDs first
    const meta = await pgQuery(`
        SELECT nft_id, serial_number, edition_id
        FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'sanders' AND serial_number = 31
    `);

    if (meta.rows.length === 0) {
        console.log('No metadata found for Ja\'Tavion Sanders #31');
        return;
    }

    const nftIds = meta.rows.map(r => r.nft_id);
    const idList = nftIds.map(id => `'${id}'`).join(',');
    console.log(`Searching for NFT IDs: ${idList}`);

    const tables = ['holdings', 'wallet_holdings', 'nft_holdings', 'user_holdings'];

    for (const table of tables) {
        try {
            const res = await pgQuery(`SELECT * FROM ${table} WHERE wallet_address = $1 AND nft_id IN (${idList})`, [WALLET]);
            console.log(`Table '${table}': Found ${res.rowCount} matches`);
            if (res.rowCount > 0) {
                console.table(res.rows);
            }
        } catch (e) {
            // Table might not exist
        }
    }

    // Also check for ANY Sanders just in case
    const anySanders = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);
    console.log('\nAll Sanders in wallet_holdings:');
    console.table(anySanders.rows);

    process.exit(0);
}

deepCheck();
