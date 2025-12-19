/**
 * Backfill locked NFT events from Flow blockchain
 * Queries NFTLocked/NFTUnlocked events and stores them in database
 * 
 * Time estimates (at ~5 requests/second):
 * - 1 year  = ~31.5M blocks = ~126K requests = ~7 hours
 * - 2 years = ~63M blocks   = ~252K requests = ~14 hours
 * 
 * Usage: 
 *   node scripts/backfill_locked_events.js --years=1
 *   node scripts/backfill_locked_events.js --days=30  (for testing)
 */

import { pgQuery } from "../db.js";

const ACCESS_NODE = process.env.FLOW_ACCESS_NODE || 'https://rest-mainnet.onflow.org';
const NFTLOCKER_LOCKED_EVENT = 'A.b6f2481eba4df97b.NFTLocker.NFTLocked';
const NFTLOCKER_UNLOCKED_EVENT = 'A.b6f2481eba4df97b.NFTLocker.NFTUnlocked';

const BLOCKS_PER_REQUEST = 250;
const BLOCKS_PER_SECOND = 1; // Flow produces ~1 block per second
const REQUESTS_DELAY_MS = 200; // 5 requests per second to avoid rate limiting

async function getCurrentBlockHeight() {
    const response = await fetch(`${ACCESS_NODE}/v1/blocks?height=sealed`);
    const data = await response.json();
    return parseInt(data[0].header.height);
}

async function queryFlowEvents(eventType, startHeight, endHeight) {
    const url = `${ACCESS_NODE}/v1/events?type=${encodeURIComponent(eventType)}&start_height=${startHeight}&end_height=${endHeight}`;

    const response = await fetch(url);
    if (!response.ok) {
        if (response.status === 429) {
            // Rate limited - wait and retry
            await sleep(5000);
            return queryFlowEvents(eventType, startHeight, endHeight);
        }
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();

    // Flow API returns { block_events: [{ block_id, block_height, events: [...] }] }
    // We need to flatten all events from all blocks
    const allEvents = [];
    if (data.block_events && Array.isArray(data.block_events)) {
        for (const block of data.block_events) {
            if (block.events && Array.isArray(block.events)) {
                for (const event of block.events) {
                    allEvents.push({
                        ...event,
                        block_height: block.block_height,
                        block_timestamp: block.block_timestamp
                    });
                }
            }
        }
    }
    return allEvents;
}

function parseEventPayload(event) {
    try {
        const payload = event.payload;
        if (!payload) return null;

        const decoded = Buffer.from(payload, 'base64').toString();
        const json = JSON.parse(decoded);

        const fields = json.value?.fields || [];
        const result = {};

        for (const field of fields) {
            result[field.name] = field.value?.value;
        }

        return {
            nft_id: result.id?.toString(),
            wallet_address: (result.to || result.from)?.toLowerCase(),
            block_height: event.block_height,
            event_type: event.type.includes('NFTLocked') ? 'locked' : 'unlocked'
        };
    } catch (err) {
        return null;
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTime(seconds) {
    const hours = Math.floor(seconds / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${mins}m`;
}

async function backfillLockedEvents(lookbackDays) {
    console.log('=== Locked Events Backfill ===\n');

    const currentHeight = await getCurrentBlockHeight();
    const blocksToQuery = lookbackDays * 24 * 60 * 60 * BLOCKS_PER_SECOND;
    const startHeight = currentHeight - blocksToQuery;
    const totalRequests = Math.ceil(blocksToQuery / BLOCKS_PER_REQUEST);
    const estimatedSeconds = totalRequests * (REQUESTS_DELAY_MS / 1000);

    console.log(`Current block: ${currentHeight}`);
    console.log(`Start block:   ${startHeight}`);
    console.log(`Blocks to query: ${blocksToQuery.toLocaleString()}`);
    console.log(`Total requests:  ${totalRequests.toLocaleString()}`);
    console.log(`Estimated time:  ${formatTime(estimatedSeconds)}`);
    console.log('');

    let lockedCount = 0;
    let unlockedCount = 0;
    let requestCount = 0;
    const startTime = Date.now();

    // Process in batches
    for (let height = startHeight; height < currentHeight; height += BLOCKS_PER_REQUEST) {
        const endHeight = Math.min(height + BLOCKS_PER_REQUEST - 1, currentHeight);

        try {
            // Query locked events
            const lockedEvents = await queryFlowEvents(NFTLOCKER_LOCKED_EVENT, height, endHeight);
            for (const event of lockedEvents) {
                const parsed = parseEventPayload(event);
                if (parsed && parsed.nft_id && parsed.wallet_address) {
                    // Insert into database
                    await pgQuery(`
            INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
            VALUES ($1, $2, true, NOW())
            ON CONFLICT (wallet_address, nft_id) DO UPDATE SET is_locked = true
          `, [parsed.wallet_address, parsed.nft_id]);
                    lockedCount++;
                }
            }

            // Query unlocked events
            const unlockedEvents = await queryFlowEvents(NFTLOCKER_UNLOCKED_EVENT, height, endHeight);
            for (const event of unlockedEvents) {
                const parsed = parseEventPayload(event);
                if (parsed && parsed.nft_id && parsed.wallet_address) {
                    // Update to unlocked
                    await pgQuery(`
            UPDATE wallet_holdings SET is_locked = false WHERE nft_id = $1
          `, [parsed.nft_id]);
                    unlockedCount++;
                }
            }

            requestCount += 2; // 2 requests per iteration (locked + unlocked)

            // Progress update every 100 requests
            if (requestCount % 100 === 0) {
                const elapsed = (Date.now() - startTime) / 1000;
                const remaining = ((totalRequests * 2) - requestCount) * (elapsed / requestCount);
                const percent = ((requestCount / (totalRequests * 2)) * 100).toFixed(1);
                console.log(`Progress: ${percent}% | Locked: ${lockedCount} | Unlocked: ${unlockedCount} | ETA: ${formatTime(remaining)}`);
            }

            await sleep(REQUESTS_DELAY_MS);

        } catch (err) {
            console.error(`Error at block ${height}: ${err.message}`);
            await sleep(1000); // Wait on error
        }
    }

    const totalTime = (Date.now() - startTime) / 1000;
    console.log('\n=== BACKFILL COMPLETE ===');
    console.log(`Total time: ${formatTime(totalTime)}`);
    console.log(`Locked events processed: ${lockedCount}`);
    console.log(`Unlocked events processed: ${unlockedCount}`);

    return { lockedCount, unlockedCount };
}

// Parse command line args
const args = process.argv.slice(2);
const yearsArg = args.find(a => a.startsWith('--years='));
const daysArg = args.find(a => a.startsWith('--days='));

let lookbackDays = 365; // Default 1 year

if (yearsArg) {
    lookbackDays = parseInt(yearsArg.split('=')[1]) * 365;
} else if (daysArg) {
    lookbackDays = parseInt(daysArg.split('=')[1]);
}

console.log(`Backfilling ${lookbackDays} days of locked events...\n`);

backfillLockedEvents(lookbackDays).then(() => {
    process.exit(0);
}).catch(err => {
    console.error('Backfill failed:', err);
    process.exit(1);
});
