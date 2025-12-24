/**
 * Blockchain Event Tracker - Backfill Acquisition Dates
 * 
 * Queries Flow blockchain for AllDay.Deposit events to determine
 * when each user ACTUALLY acquired their NFTs (not global transfer data)
 */

import * as fcl from "@onflow/fcl";
import { pgQuery } from '../db.js';

// Configure FCL
fcl.config()
    .put("accessNode.api", process.env.FLOW_ACCESS_NODE || "https://rest-mainnet.onflow.org")
    .put("flow.network", "mainnet");

const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";
const DEPOSIT_EVENT = `${ALLDAY_CONTRACT}.Deposit`;

/**
 * Get Deposit events for a specific NFT across all blocks
 * Returns array of {timestamp, from, to, nftId, blockHeight, txId}
 */
async function getDepositEventsForNFT(nftId, startBlock, endBlock) {
    try {
        const events = await fcl.send([
            fcl.getEventsAtBlockHeightRange(DEPOSIT_EVENT, startBlock, endBlock)
        ]).then(fcl.decode);

        // Filter for this specific NFT
        return events
            .filter(e => e.data.id?.toString() === nftId.toString())
            .map(e => ({
                timestamp: new Date(e.blockTimestamp),
                from: e.data.from || null,
                to: e.data.to,
                nftId: e.data.id.toString(),
                blockHeight: e.blockHeight,
                txId: e.transactionId
            }));
    } catch (err) {
        console.error(`Error fetching events for NFT ${nftId}:`, err.message);
        return [];
    }
}

/**
 * Find when a specific user acquired an NFT
 * Returns the FIRST Deposit event TO that user
 */
async function getUserAcquisitionDate(walletAddress, nftId, startBlock, endBlock) {
    const events = await getDepositEventsForNFT(nftId, startBlock, endBlock);

    // Find deposits TO this wallet
    const userDeposits = events
        .filter(e => e.to.toLowerCase() === walletAddress.toLowerCase())
        .sort((a, b) => a.blockHeight - b.blockHeight);

    // Return FIRST  deposit = acquisition date
    return userDeposits[0] || null;
}

/**
 * Backfill acquisition dates for all NFTs owned by a wallet
 */
async function backfillWalletAcquisitions(walletAddress, options = {}) {
    const {
        batchSize = 50,
        startBlock = 7600000, // AllDay launch block (approximate)
        endBlock = null
    } = options;

    console.log(`\n=== Backfilling Acquisition Dates for ${walletAddress} ===\n`);

    try {
        // Get current block if not specified
        const currentBlock = endBlock || (await fcl.block()).height;
        console.log(`Scanning blocks ${startBlock} to ${currentBlock}\n`);

        // Get all NFTs owned by this wallet
        const holdings = await pgQuery(`
      SELECT nft_id, acquired_at
      FROM holdings
      WHERE wallet_address = $1
      ORDER BY nft_id
    `, [walletAddress]);

        console.log(`Found ${holdings.rows.length} NFTs to process\n`);

        let processed = 0;
        let updated = 0;
        let errors = 0;

        for (const holding of holdings.rows) {
            try {
                processed++;

                if (processed % 10 === 0) {
                    console.log(`Progress: ${processed}/${holdings.rows.length} (${Math.round(processed / holdings.rows.length * 100)}%)`);
                }

                // Find acquisition event
                const acquisition = await getUserAcquisitionDate(
                    walletAddress,
                    holding.nft_id,
                    startBlock,
                    currentBlock
                );

                if (acquisition) {
                    // Update database with correct acquisition date
                    await pgQuery(`
            UPDATE holdings
            SET acquired_at = $1
            WHERE wallet_address = $2 AND nft_id = $3
          `, [acquisition.timestamp, walletAddress, holding.nft_id]);

                    // Also store in ownership_history if table exists
                    try {
                        await pgQuery(`
              INSERT INTO ownership_history 
              (nft_id, from_wallet, to_wallet, event_type, event_timestamp, block_height, transaction_id)
              VALUES ($1, $2, $3, 'DEPOSIT', $4, $5, $6)
              ON CONFLICT DO NOTHING
            `, [
                            holding.nft_id,
                            acquisition.from,
                            walletAddress,
                            acquisition.timestamp,
                            acquisition.blockHeight,
                            acquisition.txId
                        ]);
                    } catch (err) {
                        // ownership_history table might not exist yet
                    }

                    updated++;

                    if (updated % 10 === 0) {
                        console.log(`  ✅ Updated ${updated} acquisition dates so far...`);
                    }
                } else {
                    console.log(`  ⚠️  No Deposit event found for NFT ${holding.nft_id} (may be too old or locked)`);
                }

                // Rate limiting - don't overwhelm the Flow API
                await new Promise(resolve => setTimeout(resolve, 100));

            } catch (err) {
                errors++;
                console.error(`  ❌ Error processing NFT ${holding.nft_id}:`, err.message);
            }
        }

        console.log('\n' + '='.repeat(60));
        console.log('Backfill Complete!');
        console.log('='.repeat(60));
        console.log(`Processed: ${processed} NFTs`);
        console.log(`Updated: ${updated} acquisition dates`);
        console.log(`Errors: ${errors}`);
        console.log(`Skipped: ${processed - updated - errors}`);
        console.log('='.repeat(60));

    } catch (error) {
        console.error('Fatal error in backfill:', error);
        throw error;
    }
}

/**
 * Repair mode: Only backfill NFTs with NULL or today's date (corrupted or missing)
 */
async function repairMissingDates(walletAddress) {
    console.log(`\n=== Repairing Missing/Corrupted Dates for ${walletAddress} ===\n`);

    try {
        // Find NFTs with today's date (corrupted) OR NULL date (missing)
        const toFix = await pgQuery(`
      SELECT nft_id, acquired_at
      FROM holdings
      WHERE wallet_address = $1
        AND (acquired_at IS NULL OR acquired_at::date = CURRENT_DATE)
      ORDER BY nft_id
    `, [walletAddress]);

        console.log(`Found ${toFix.rows.length} NFTs with missing or today's dates\n`);

        if (toFix.rows.length === 0) {
            console.log('✅ No dates need repair!');
            return;
        }

        // Process these specifically
        await backfillWalletAcquisitions(walletAddress, {
            nftIds: toFix.rows.map(r => r.nft_id)
        });

    } catch (error) {
        console.error('Error in repair:', error);
        throw error;
    }
}

// CLI Usage
const args = process.argv.slice(2);
const wallet = args[0];
const mode = args[1] || 'repair';

if (!wallet) {
    console.log('Usage: node backfill-acquisition-dates.js <wallet_address> [mode]');
    console.log('');
    console.log('Modes:');
    console.log('  repair  - Fix NFTs with NULL or today\'s dates (default)');
    console.log('  full    - Backfill all NFTs (slow, comprehensive)');
    console.log('');
    console.log('Example:');
    console.log('  node scripts/backfill-acquisition-dates.js 0x7541bafd155b683e repair');
    process.exit(1);
}

console.log('🔍 Blockchain Event Tracker');
console.log(`Wallet: ${wallet}`);
console.log(`Mode: ${mode}\n`);

if (mode === 'repair') {
    repairMissingDates(wallet)
        .then(() => process.exit(0))
        .catch(err => {
            console.error(err);
            process.exit(1);
        });
} else {
    backfillWalletAcquisitions(wallet)
        .then(() => process.exit(0))
        .catch(err => {
            console.error(err);
            process.exit(1);
        });
}

export {
    getDepositEventsForNFT,
    getUserAcquisitionDate,
    backfillWalletAcquisitions,
    repairMissingDates
};
