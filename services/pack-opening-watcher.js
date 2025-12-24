/**
 * Pack Opening Event Watcher
 * Listens for AllDay.Deposit events from pack contracts to detect pack openings
 * Automatically adds new NFTs to database when packs are opened
 */

import * as fcl from "@onflow/fcl";
import { pgQuery } from "../db.js";

// Configure FCL
fcl.config()
    .put("accessNode.api", process.env.FLOW_ACCESS_NODE || "https://rest-mainnet.onflow.org")
    .put("flow.network", "mainnet");

const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";
const PACK_CONTRACTS = [
    "0xe4cf4bdc1751c65d", // AllDay contract itself (for pack drops)
    "0x0000000000000000", // Zero address indicates mint from pack
];

let eventCount = 0;
let packOpeningCount = 0;

/**
 * Handle AllDay.Deposit events
 * These occur when NFTs are deposited into a wallet (from packs, gifts, or transfers)
 */
async function handleDepositEvent(event) {
    try {
        const eventData = event.data || {};
        const nftId = eventData.id?.toString();
        const toWallet = eventData.to?.toLowerCase();
        const fromAddress = eventData.from?.toLowerCase() || null;

        if (!nftId || !toWallet) {
            console.log('[Pack Watcher] Invalid event data, skipping');
            return;
        }

        eventCount++;

        // Determine if this is a pack opening vs marketplace transfer
        // Pack openings typically have from=null or from=pack_contract
        const isPackOpening = !fromAddress || fromAddress === '0x0000000000000000';

        if (isPackOpening) {
            packOpeningCount++;
            console.log(`\n📦 [Pack Opening #${packOpeningCount}] NFT ${nftId} → ${toWallet}`);
        } else {
            console.log(`\n📬 [Transfer] NFT ${nftId}: ${fromAddress} → ${toWallet}`);
        }

        // 1. Add to wallet_holdings
        await pgQuery(`
      INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
      VALUES ($1, $2, false, NOW())
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked = false,
        last_event_ts = NOW()
    `, [toWallet, nftId]);

        // 2. Add to holdings (with acquired_at preserved)
        await pgQuery(`
      INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
      VALUES ($1, $2, false, NOW())
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked = false
    `, [toWallet, nftId]);

        // 3. Check if metadata exists
        const metadataCheck = await pgQuery(`
      SELECT 1 FROM nft_core_metadata_v2 WHERE nft_id = $1
    `, [nftId]);

        if (metadataCheck.rows.length === 0) {
            console.log(`   ⚠️  Metadata not found for NFT ${nftId} - will be synced later`);
            // Queue for metadata fetch (this can be async)
            queueMetadataFetch(nftId);
        } else {
            console.log(`   ✅ NFT ${nftId} added to wallet ${toWallet.substring(0, 10)}...`);
        }

        // 4. If new tables exist, add there too
        try {
            await pgQuery(`
        INSERT INTO ownership (wallet_address, nft_id, is_locked, first_acquired_at)
        VALUES ($1, $2, false, NOW())
        ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
          is_locked = false,
          last_synced_at = NOW()
      `, [toWallet, nftId]);

            // Log to ownership_history
            await pgQuery(`
        INSERT INTO ownership_history (nft_id, from_wallet, to_wallet, event_type, event_timestamp)
        VALUES ($1, $2, $3, $4, NOW())
      `, [nftId, fromAddress, toWallet, isPackOpening ? 'MINT' : 'TRANSFER']);
        } catch (err) {
            // Tables might not exist yet, that's okay
        }

    } catch (error) {
        console.error('[Pack Watcher] Error processing deposit event:', error.message);
    }
}

/**
 * Queue metadata fetch for NFTs without metadata
 */
const metadataQueue = new Set();
let metadataFetchTimer = null;

function queueMetadataFetch(nftId) {
    metadataQueue.add(nftId);

    // Batch fetch after 5 seconds
    if (metadataFetchTimer) clearTimeout(metadataFetchTimer);
    metadataFetchTimer = setTimeout(async () => {
        if (metadataQueue.size > 0) {
            console.log(`\n🔍 Fetching metadata for ${metadataQueue.size} NFTs...`);
            // This will be implemented in the metadata sync script
            // For now, just log
            metadataQueue.clear();
        }
    }, 5000);
}

/**
 * Start watching for pack opening events
 */
export async function watchPackOpenings() {
    console.log('\n=== Pack Opening Watcher Started ===');
    console.log('Listening for AllDay.Deposit events...');
    console.log('Contract:', ALLDAY_CONTRACT);
    console.log('Watching for pack openings and transfers\n');

    try {
        // Subscribe to Deposit events
        const eventName = `${ALLDAY_CONTRACT}.Deposit`;

        // Note: fcl.events() requires block height ranges
        // For real-time monitoring, we'll need to poll recent blocks
        // Let's implement a polling approach instead

        let lastCheckedBlock = await getCurrentBlockHeight();
        console.log(`Starting from block: ${lastCheckedBlock}\n`);

        // Poll every 10 seconds
        setInterval(async () => {
            try {
                const currentBlock = await getCurrentBlockHeight();

                if (currentBlock > lastCheckedBlock) {
                    // Check events in range
                    const events = await getEventsInRange(
                        eventName,
                        lastCheckedBlock + 1,
                        currentBlock
                    );

                    if (events.length > 0) {
                        console.log(`\n📡 Found ${events.length} deposit event(s) in blocks ${lastCheckedBlock + 1}-${currentBlock}`);

                        for (const event of events) {
                            await handleDepositEvent(event);
                        }
                    }

                    lastCheckedBlock = currentBlock;
                }
            } catch (err) {
                console.error('[Pack Watcher] Error in polling loop:', err.message);
            }
        }, 10000); // Check every 10 seconds

        console.log('✅ Pack watcher polling started (checking every 10 seconds)');

    } catch (error) {
        console.error('[Pack Watcher] Failed to start:', error);
        throw error;
    }
}

/**
 * Get current block height
 */
async function getCurrentBlockHeight() {
    try {
        const block = await fcl.block();
        return block.height;
    } catch (err) {
        console.error('[Pack Watcher] Error getting block height:', err.message);
        throw err;
    }
}

/**
 * Get events in a block range
 */
async function getEventsInRange(eventType, startBlock, endBlock) {
    try {
        // Limit range to prevent overwhelming queries
        if (endBlock - startBlock > 100) {
            endBlock = startBlock + 100;
        }

        const events = await fcl.send([
            fcl.getEventsAtBlockHeightRange(eventType, startBlock, endBlock)
        ]).then(fcl.decode);

        return events;
    } catch (err) {
        console.error('[Pack Watcher] Error fetching events:', err.message);
        return [];
    }
}

/**
 * Stop watching (for graceful shutdown)
 */
export function stopWatching() {
    console.log('\n[Pack Watcher] Shutting down...');
    if (metadataFetchTimer) {
        clearTimeout(metadataFetchTimer);
    }
}

// Stats reporting every minute
setInterval(() => {
    if (eventCount > 0) {
        console.log(`\n📊 [Stats] Total events: ${eventCount}, Pack openings: ${packOpeningCount}`);
    }
}, 60000);

export default {
    watchPackOpenings,
    stopWatching
};
