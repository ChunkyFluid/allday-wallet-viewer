// Diagnostic script to check sync status for a specific wallet and NFT
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function diagnose() {
    console.log('=== SYNC DIAGNOSTIC ===\n');

    // 1. Check recent holdings for this wallet
    console.log('1. Recent holdings in holdings table for wallet:');
    const holdings = await pgQuery(
        `SELECT nft_id, is_locked, acquired_at 
         FROM holdings 
         WHERE wallet_address = $1 
         ORDER BY acquired_at DESC NULLS LAST 
         LIMIT 10`,
        [WALLET]
    );
    console.table(holdings.rows);
    console.log(`Total holdings in table: ${holdings.rowCount} shown of...`);

    const countRes = await pgQuery(
        `SELECT COUNT(*) as total FROM holdings WHERE wallet_address = $1`,
        [WALLET]
    );
    console.log(`...${countRes.rows[0].total} total\n`);

    // 2. Check for Rashid Shaheed in metadata
    console.log('2. Rashid Shaheed NFTs in metadata table:');
    const rashid = await pgQuery(
        `SELECT nft_id, serial_number, edition_id 
         FROM nft_core_metadata_v2 
         WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
         LIMIT 10`
    );
    console.table(rashid.rows);

    // 3. Check if any Rashid Shaheed is owned by this wallet
    console.log('3. Does this wallet own any Rashid Shaheed?');
    const ownedRashid = await pgQuery(
        `SELECT h.nft_id, m.serial_number, h.acquired_at
         FROM holdings h
         JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
         WHERE h.wallet_address = $1
           AND LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'`,
        [WALLET]
    );
    if (ownedRashid.rowCount > 0) {
        console.log('YES! Found:');
        console.table(ownedRashid.rows);
    } else {
        console.log('NO. No Rashid Shaheed found in holdings for this wallet.\n');
    }

    // 4. Check wallet_holdings (legacy) for comparison
    console.log('4. [Legacy] wallet_holdings table count:');
    try {
        const legacyCount = await pgQuery(
            `SELECT COUNT(*) as total FROM wallet_holdings WHERE wallet_address = $1`,
            [WALLET]
        );
        console.log(`Legacy wallet_holdings count: ${legacyCount.rows[0].total}\n`);
    } catch (e) {
        console.log(`Legacy table error: ${e.message}\n`);
    }

    // 5. Check for Joe Mixon (from screenshot - should be recent)
    console.log('5. Does this wallet own Joe Mixon (from screenshot)?');
    const mixon = await pgQuery(
        `SELECT h.nft_id, m.serial_number, h.acquired_at
         FROM holdings h
         JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
         WHERE h.wallet_address = $1
           AND LOWER(m.first_name) = 'joe' AND LOWER(m.last_name) = 'mixon'`,
        [WALLET]
    );
    if (mixon.rowCount > 0) {
        console.log('YES! Found:');
        console.table(mixon.rows);
    } else {
        console.log('NO. Joe Mixon not found.\n');
    }

    console.log('=== END DIAGNOSTIC ===');
    process.exit(0);
}

diagnose().catch(err => {
    console.error('Diagnostic failed:', err);
    process.exit(1);
});
