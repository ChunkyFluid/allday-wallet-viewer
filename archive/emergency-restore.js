import { pgQuery } from './db.js';

async function emergencyRestore() {
    try {
        console.log('=== EMERGENCY: Restoring Acquired Dates ===\n');

        const myWallet = '0x7541bafd155b683e';

        // Step 1: Check if wallet_holdings_history exists
        console.log('Step 1: Checking for backup data...');

        const tablesCheck = await pgQuery(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_name IN ('wallet_holdings_history', 'holdings')
      AND table_schema = 'public'
    `);

        console.log('Available backup tables:', tablesCheck.rows.map(r => r.table_name).join(', '));

        // Step 2: Check holdings table for older acquired_at dates
        console.log('\nStep 2: Checking holdings table for preserved dates...');

        const holdingsCheck = await pgQuery(`
      SELECT nft_id, acquired_at, created_at
      FROM holdings
      WHERE wallet_address = $1
        AND is_locked = true
      ORDER BY acquired_at DESC NULLS LAST
      LIMIT 10
    `, [myWallet]);

        console.log(`\nSample locked NFTs from holdings table:`);
        holdingsCheck.rows.forEach((row, i) => {
            console.log(`  ${i + 1}. NFT ${row.nft_id}`);
            console.log(`      Acquired: ${row.acquired_at}`);
            console.log(`      Created: ${row.created_at}`);
        });

        // Step 3: Find today's syncs that damaged dates
        console.log('\n\nStep 3: Finding NFTs with today\'s date (damaged)...');

        const damagedQuery = await pgQuery(`
      SELECT COUNT(*) as count
      FROM holdings
      WHERE wallet_address = $1
        AND is_locked = true
        AND acquired_at::date = CURRENT_DATE
    `, [myWallet]);

        console.log(`Found ${damagedQuery.rows[0].count} locked NFTs with today's date (likely damaged)`);

        // Step 4: Look for Shedeur Sanders
        console.log('\n\nStep 4: Searching for Shedeur Sanders...');

        const shedeurQuery = await pgQuery(`
      SELECT wh.nft_id, m.first_name, m.last_name, m.set_name, m.serial_number, 
             m.tier, wh.is_locked, h.acquired_at
      FROM wallet_holdings wh
      LEFT JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      LEFT JOIN holdings h ON wh.wallet_address = h.wallet_address AND wh.nft_id = h.nft_id
      WHERE wh.wallet_address = $1
        AND (m.first_name ILIKE '%shedeur%' OR m.last_name ILIKE '%sanders%')
      ORDER BY m.serial_number ASC
    `, [myWallet]);

        console.log(`\nAll Shedeur Sanders moments (${shedeurQuery.rows.length} found):`);
        shedeurQuery.rows.forEach((row, i) => {
            console.log(`  ${i + 1}. NFT ${row.nft_id}: ${row.first_name} ${row.last_name} #${row.serial_number || '?'}`);
            console.log(`      Set: ${row.set_name}, Locked: ${row.is_locked}`);
            console.log(`      Acquired: ${row.acquired_at || 'NULL'}`);
        });

        // Step 5: Query blockchain directly for the new Shedeur
        console.log('\n\nStep 5: Querying blockchain for latest NFTs...');

        const flowService = await import('./services/flow-blockchain.js');
        const unlockedIds = await flowService.getWalletNFTIds(myWallet);
        const lockedIds = await flowService.getLockedNFTIds(myWallet);

        const allIds = [...unlockedIds, ...lockedIds];
        console.log(`Blockchain shows ${allIds.length} total NFTs (${unlockedIds.length} unlocked + ${lockedIds.length} locked)`);

        // Find NFTs in blockchain but not in database
        const dbIds = new Set(shedeurQuery.rows.map(r => r.nft_id));
        const missingOnChain = allIds.filter(id => !dbIds.has(id.toString()));

        if (missingOnChain.length > 0) {
            console.log(`\n⚠️  Found ${missingOnChain.length} NFTs on blockchain not in database!`);
            console.log('Sample:', missingOnChain.slice(0, 5).join(', '));
        }

    } catch (error) {
        console.error('Error:', error.message);
        console.error(error);
    } finally {
        process.exit();
    }
}

emergencyRestore();
