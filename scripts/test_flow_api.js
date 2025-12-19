/**
 * Simple test to see raw API response for NFTLocked events
 */

const ACCESS_NODE = 'https://rest-mainnet.onflow.org';

async function test() {
    // Get current block
    const blockResp = await fetch(`${ACCESS_NODE}/v1/blocks?height=sealed`);
    const blockData = await blockResp.json();
    const currentHeight = parseInt(blockData[0].header.height);

    console.log('Current block:', currentHeight);

    // Query events - note the API returns "block_events" not "events"
    const url = `${ACCESS_NODE}/v1/events?type=A.b6f2481eba4df97b.NFTLocker.NFTLocked&start_height=${currentHeight - 250}&end_height=${currentHeight}`;
    console.log('\nURL:', url);

    const resp = await fetch(url);
    const data = await resp.json();

    console.log('\nResponse keys:', Object.keys(data));
    console.log('block_events count:', data.block_events?.length || 0);

    // The structure is likely: { block_events: [ { block_id, block_height, events: [...] } ] }
    if (data.block_events && data.block_events.length > 0) {
        let totalEvents = 0;
        for (const block of data.block_events) {
            if (block.events && block.events.length > 0) {
                totalEvents += block.events.length;
                console.log(`\nBlock ${block.block_height}: ${block.events.length} events`);
                console.log('First event:', JSON.stringify(block.events[0]).substring(0, 300));
            }
        }
        console.log('\nTotal events across all blocks:', totalEvents);
    } else {
        console.log('\nNo block_events in response');
        console.log('Raw response:', JSON.stringify(data).substring(0, 500));
    }
}

test();
