// Force fix: Remove Ja'Tavion and set correct 2024 dates
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function forceFix() {
    console.log('=== FORCE FIXING ===\n');

    // 1. DELETE Ja'Tavion Sanders #31 (NFT 10498837)
    const deleted = await pgQuery(`
        DELETE FROM holdings 
        WHERE wallet_address = $1 AND nft_id = '10498837'
    `, [WALLET]);
    console.log(`Deleted Ja'Tavion Sanders: ${deleted.rowCount} rows`);

    // 2. Set Rashid #1096 to TODAY 2024
    await pgQuery(`
        UPDATE holdings 
        SET acquired_at = '2024-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '4099792'
    `, [WALLET]);
    console.log('✅ Set Rashid #1096 to 2024-12-24');

    // 3. Set Joe Mixon #2504 to TODAY 2024
    await pgQuery(`
        UPDATE holdings 
        SET acquired_at = '2024-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '9872818'
    `, [WALLET]);
    console.log('✅ Set Joe Mixon #2504 to 2024-12-24');

    // 4. Set all Shedeur Sanders to YESTERDAY 2024
    const shedeurIds = await pgQuery(`
        SELECT h.nft_id FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'shedeur'
          AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);

    for (const row of shedeurIds.rows) {
        await pgQuery(`
            UPDATE holdings 
            SET acquired_at = '2024-12-23'::timestamp
            WHERE wallet_address = $1 AND nft_id = $2
        `, [WALLET, row.nft_id]);
    }
    console.log(`✅ Set ${shedeurIds.rowCount} Shedeur Sanders to 2024-12-23`);

    // Verify
    const jCheck = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1 AND nft_id = '10498837'`, [WALLET]);
    console.log(`\nJa'Tavion still in table: ${jCheck.rows[0].c} (should be 0)`);

    const total = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`Total: ${total.rows[0].c}`);

    process.exit(0);
}

forceFix();
