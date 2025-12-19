/**
 * Simple debug - just print raw API data to see what's happening
 */

const ACCESS_NODE = 'https://rest-mainnet.onflow.org';

async function debug() {
    // Get current block
    const blockResp = await fetch(`${ACCESS_NODE}/v1/blocks?height=sealed`);
    const blockData = await blockResp.json();
    const currentHeight = parseInt(blockData[0].header.height);

    console.log('Current block:', currentHeight);

    // Test different time ranges
    const ranges = [
        { days: 7, label: '7 days ago' },
        { days: 30, label: '30 days ago' },
        { days: 60, label: '60 days ago' },
        { days: 90, label: '90 days ago' },
        { days: 180, label: '180 days ago' },
    ];

    for (const range of ranges) {
        const blocksAgo = range.days * 24 * 60 * 60; // blocks per day
        const startHeight = currentHeight - blocksAgo;
        const endHeight = startHeight + 250;

        const url = `${ACCESS_NODE}/v1/events?type=A.b6f2481eba4df97b.NFTLocker.NFTLocked&start_height=${startHeight}&end_height=${endHeight}`;

        try {
            const resp = await fetch(url);
            const data = await resp.json();

            let eventCount = 0;
            if (data.block_events) {
                for (const block of data.block_events) {
                    if (block.events) {
                        eventCount += block.events.length;
                    }
                }
            }

            console.log(`\n${range.label} (blocks ${startHeight}-${endHeight}): ${eventCount} events`);

            if (eventCount > 0) {
                const block = data.block_events.find(b => b.events && b.events.length > 0);
                const event = block.events[0];
                console.log('  Sample event type:', event.type);
                console.log('  Sample payload preview:', event.payload?.substring(0, 100));

                // Try to decode
                const decoded = Buffer.from(event.payload, 'base64').toString();
                const json = JSON.parse(decoded);
                console.log('  Decoded fields:', json.value?.fields?.map(f => f.name));

                // Show the to/from fields
                for (const field of json.value?.fields || []) {
                    console.log(`    ${field.name}: ${field.value?.value}`);
                }
            }
        } catch (err) {
            console.log(`${range.label}: Error - ${err.message}`);
        }
    }
}

debug();
