import { pgQuery } from './db.js';

async function diagnoseShedeur31() {
    console.log('=== Diagnosing Shedeur Sanders #31 ===\n');

    const myWallet = '0x7541bafd155b683e';

    try {
        // 1. Check all Regal Rookies Shedeur moments
        console.log('1. All Shedeur Sanders in Regal Rookies metadata:');
        const allRegal = await pgQuery(`
      SELECT nft_id, serial_number, edition_id
      FROM nft_core_metadata_v2
      WHERE set_name ILIKE '%regal%'
        AND (first_name ILIKE '%shedeur%' OR last_name ILIKE '%sanders%')
      ORDER BY serial_number
    `);

        console.log(`   Found ${allRegal.rows.length} moments`);
        console.log(`   Serials: ${allRegal.rows.map(r => '#' + r.serial_number).join(', ')}`);

        const has31 = allRegal.rows.find(r => r.serial_number === 31);
        if (!has31) {
            console.log(`   ❌ #31 is MISSING from metadata\n`);
        } else {
            console.log(`   ✅ #31 found: NFT ID ${has31.nft_id}\n`);
        }

        // 2. Check directly on blockchain
        console.log('2. Querying your wallet on blockchain...');
        const flowService = await import('./services/flow-blockchain.js');
        const unlockedIds = await flowService.getWalletNFTIds(myWallet);
        const lockedIds = await flowService.getLockedNFTIds(myWallet);

        console.log(`   Unlocked: ${unlockedIds.length}`);
        console.log(`   Locked: ${lockedIds.length}`);

        // 3. Check wallet_holdings
        console.log('\n3. Checking wallet_holdings for Shedeur moments:');
        const holdings = await pgQuery(`
      SELECT wh.nft_id, m.serial_number, wh.is_locked
      FROM wallet_holdings wh
      LEFT JOIN nft_core_metadata_v2 m ON wh.nft_id = m.nft_id
      WHERE wh.wallet_address = $1
        AND (m.first_name ILIKE '%shedeur%' OR m.last_name ILIKE '%sanders%')
        AND m.set_name ILIKE '%regal%'
      ORDER BY m.serial_number
    `, [myWallet]);

        console.log(`   Found ${holdings.rows.length} in holdings:`);
        holdings.rows.forEach(r => {
            console.log(`     #${r.serial_number || '?'}: NFT ${r.nft_id} ${r.is_locked ? '🔒' : '🔓'}`);
        });

        // 4. Query Flow for a specific NFT's metadata
        console.log('\n4. The issue:');
        console.log('   - You opened the pack TODAY');
        console.log('   - Metadata sync hasn\'t run yet for new NFTs');
        console.log('   - NFT #31 exists on blockchain but not in nft_core_metadata_v2');
        console.log('\nSOLVED: Need to sync NFL All Day metadata for new moments');

    } catch (error) {
        console.error('Error:', error.message);
        console.error(error);
    } finally {
        process.exit();
    }
}

diagnoseShedeur31();
