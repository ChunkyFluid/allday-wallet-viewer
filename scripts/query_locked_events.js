/**
 * Query Flow blockchain for NFTLocked/NFTUnlocked events
 * This replicates what Snowflake FACT_EVENTS does - querying the blockchain events directly
 * 
 * Flow Access API: GET /v1/events?type={event_type}&start_height={start}&end_height={end}
 */

const ACCESS_NODE = process.env.FLOW_ACCESS_NODE || 'https://rest-mainnet.onflow.org';
const NFTLOCKER_LOCKED_EVENT = 'A.b6f2481eba4df97b.NFTLocker.NFTLocked';
const NFTLOCKER_UNLOCKED_EVENT = 'A.b6f2481eba4df97b.NFTLocker.NFTUnlocked';

/**
 * Get the current block height from Flow
 */
async function getCurrentBlockHeight() {
    const response = await fetch(`${ACCESS_NODE}/v1/blocks?height=sealed`);
    const data = await response.json();
    return parseInt(data[0].header.height);
}

/**
 * Query events from Flow Access API
 * @param {string} eventType - Full event type (e.g. A.b6f2481eba4df97b.NFTLocker.NFTLocked)
 * @param {number} startHeight - Start block height
 * @param {number} endHeight - End block height (max 250 blocks per request)
 */
async function queryFlowEvents(eventType, startHeight, endHeight) {
    const url = `${ACCESS_NODE}/v1/events?type=${encodeURIComponent(eventType)}&start_height=${startHeight}&end_height=${endHeight}`;

    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = await response.json();
        return data.events || [];
    } catch (err) {
        console.error(`Error querying events: ${err.message}`);
        return [];
    }
}

/**
 * Parse event payload to extract NFT ID and wallet address
 */
function parseEventPayload(event) {
    try {
        const payload = event.payload;
        if (!payload) return null;

        // Payload is base64 encoded JSON-CDC
        const decoded = Buffer.from(payload, 'base64').toString();
        const json = JSON.parse(decoded);

        const fields = json.value?.fields || [];
        const result = {};

        for (const field of fields) {
            const name = field.name;
            const value = field.value?.value;
            result[name] = value;
        }

        return {
            nft_id: result.id?.toString(),
            wallet_address: (result.to || result.from)?.toLowerCase(),
            block_height: event.block_height,
            block_timestamp: event.block_timestamp,
            event_type: event.type
        };
    } catch (err) {
        return null;
    }
}

/**
 * Get locked NFTs for a specific wallet by querying historical events
 * This is expensive - only use for initial sync or verification
 */
async function getLockedNFTsFromEvents(walletAddress, lookbackBlocks = 10000000) {
    console.log(`\n=== Querying Flow Events for ${walletAddress} ===\n`);

    const wallet = walletAddress.toLowerCase();
    const currentHeight = await getCurrentBlockHeight();
    console.log(`Current block height: ${currentHeight}`);

    // Flow events API has a limit of 250 blocks per request
    // For a full historical query, this would require millions of requests
    // Instead, let's just demonstrate with recent events

    const BLOCKS_PER_REQUEST = 250;
    const startHeight = Math.max(1, currentHeight - lookbackBlocks);

    console.log(`Querying from block ${startHeight} to ${currentHeight}`);
    console.log(`This is ${currentHeight - startHeight} blocks (~${Math.ceil((currentHeight - startHeight) / BLOCKS_PER_REQUEST)} requests)`);

    // For demonstration, let's just do a small sample
    const sampleStart = currentHeight - 1000; // Last ~1000 blocks (~10 hours)
    console.log(`\nSampling last 1000 blocks (${sampleStart} to ${currentHeight})...\n`);

    const lockedEvents = [];
    const unlockedEvents = [];

    for (let height = sampleStart; height < currentHeight; height += BLOCKS_PER_REQUEST) {
        const endHeight = Math.min(height + BLOCKS_PER_REQUEST - 1, currentHeight);

        // Query locked events
        const locked = await queryFlowEvents(NFTLOCKER_LOCKED_EVENT, height, endHeight);
        for (const event of locked) {
            const parsed = parseEventPayload(event);
            if (parsed && parsed.wallet_address === wallet) {
                lockedEvents.push(parsed);
            }
        }

        // Query unlocked events
        const unlocked = await queryFlowEvents(NFTLOCKER_UNLOCKED_EVENT, height, endHeight);
        for (const event of unlocked) {
            const parsed = parseEventPayload(event);
            if (parsed && parsed.wallet_address === wallet) {
                unlockedEvents.push(parsed);
            }
        }

        process.stdout.write(`\rProcessed blocks ${height} - ${endHeight} (${lockedEvents.length} locked, ${unlockedEvents.length} unlocked)`);
    }

    console.log(`\n\nResults:`);
    console.log(`- Locked events found: ${lockedEvents.length}`);
    console.log(`- Unlocked events found: ${unlockedEvents.length}`);

    return { lockedEvents, unlockedEvents };
}

// Test with JungleRules
const JUNGLE_RULES = '0xcfd9bad75352b43b';
getLockedNFTsFromEvents(JUNGLE_RULES, 1000).then(result => {
    console.log('\nSample locked events:', result.lockedEvents.slice(0, 3));
    process.exit(0);
}).catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
