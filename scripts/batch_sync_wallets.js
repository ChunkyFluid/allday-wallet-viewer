/**
 * Batch sync all wallets from blockchain
 * This rebuilds wallet_holdings and holdings tables using:
 * - Unlocked NFTs: from blockchain (live)
 * - Locked NFTs: from current database (until we can query NFTLocker)
 * 
 * Usage: node scripts/batch_sync_wallets.js [--limit=N]
 */

import { pgQuery } from "../db.js";

const NFTLOCKER_CONTRACT = '0xb6f2481eba4df97b';

async function batchSync() {
    console.log('=== Batch Wallet Sync from Blockchain ===\n');

    const args = process.argv.slice(2);
    const limitArg = args.find(a => a.startsWith('--limit='));
    const limit = limitArg ? parseInt(limitArg.split('=')[1]) : 100;

    const flowService = await import('../services/flow-blockchain.js');

    // Get all wallets with holdings (excluding NFTLocker)
    console.log(`1. Finding wallets to sync (limit: ${limit})...`);
    const wallets = await pgQuery(
        `SELECT wallet_address, COUNT(*) as count 
     FROM wallet_holdings 
     WHERE wallet_address != $1
     GROUP BY wallet_address 
     ORDER BY count DESC
     LIMIT $2`,
        [NFTLOCKER_CONTRACT, limit]
    );
    console.log(`   Found ${wallets.rows.length} wallets\n`);

    let synced = 0;
    let errors = 0;
    let totalFixed = 0;

    for (const row of wallets.rows) {
        const wallet = row.wallet_address;
        const dbCount = parseInt(row.count);

        try {
            // Get blockchain unlocked
            const unlockedIds = await flowService.getWalletNFTIds(wallet);

            // Get locked from current database
            const lockedResult = await pgQuery(
                `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
                [wallet]
            );
            const lockedIds = lockedResult.rows.map(r => r.nft_id);

            // Calculate expected
            const expectedCount = unlockedIds.length + lockedIds.length;
            const diff = dbCount - expectedCount;

            // Only sync if there's a significant discrepancy (>5%)
            const shouldSync = Math.abs(diff) > Math.max(5, dbCount * 0.05);

            if (shouldSync) {
                // Rebuild the wallet
                const allNftIds = [
                    ...unlockedIds.map(id => ({ nft_id: id.toString(), is_locked: false })),
                    ...lockedIds.map(id => ({ nft_id: id, is_locked: true }))
                ];

                // Clear and re-insert wallet_holdings
                await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1`, [wallet]);

                if (allNftIds.length > 0) {
                    const BATCH_SIZE = 500;
                    for (let i = 0; i < allNftIds.length; i += BATCH_SIZE) {
                        const batch = allNftIds.slice(i, i + BATCH_SIZE);
                        const batchValues = batch.map((_, idx) =>
                            `($1, $${idx * 2 + 2}, $${idx * 2 + 3}, NOW())`
                        ).join(', ');

                        const batchParams = [wallet];
                        batch.forEach(nft => {
                            batchParams.push(nft.nft_id, nft.is_locked);
                        });

                        await pgQuery(
                            `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
               VALUES ${batchValues}
               ON CONFLICT (wallet_address, nft_id) DO UPDATE SET 
                 is_locked = EXCLUDED.is_locked,
                 last_event_ts = NOW()`,
                            batchParams
                        );
                    }
                }

                console.log(`   ✅ ${wallet.substring(0, 10)}... ${dbCount} → ${expectedCount} (fixed ${Math.abs(diff)})`);
                totalFixed += Math.abs(diff);
            } else if (diff !== 0) {
                console.log(`   ⏭️  ${wallet.substring(0, 10)}... ${dbCount} → ${expectedCount} (diff ${diff}, skipped)`);
            }

            synced++;

            // Progress update every 10 wallets
            if (synced % 10 === 0) {
                console.log(`   Progress: ${synced}/${wallets.rows.length} wallets synced`);
            }

        } catch (err) {
            console.log(`   ❌ ${wallet.substring(0, 10)}... Error: ${err.message}`);
            errors++;
        }

        // Small delay to avoid rate limiting
        await new Promise(r => setTimeout(r, 100));
    }

    console.log(`\n=== BATCH SYNC COMPLETE ===`);
    console.log(`   Wallets processed: ${synced}`);
    console.log(`   Errors: ${errors}`);
    console.log(`   Total NFT discrepancies fixed: ${totalFixed}`);

    process.exit(0);
}

batchSync().catch(err => {
    console.error('Batch sync failed:', err);
    process.exit(1);
});
