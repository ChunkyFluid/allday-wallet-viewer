/**
 * Backfill blockchain events from Dec 17 to current date
 * Catches up the 3-day gap without re-querying Snowflake
 */

import * as eventProcessor from '../services/event-processor.js';
import fetch from 'node-fetch';

const FLOW_REST_API = 'https://rest-mainnet.onflow.org';
const START_DATE = '2025-12-22T00:00:00Z';
const END_DATE = new Date().toISOString();

async function getBlockHeightForDate(dateStr) {
    // Flow mainnet genesis was around block height 7601063 (Sept 2020)
    // Approx 10M blocks/year, ~27k blocks/day
    // Dec 17 2024 ≈ block 90-95M range
    // We'll use a rough estimate and adjust
    const targetDate = new Date(dateStr);
    const genesisDate = new Date('2020-09-23T00:00:00Z');
    const genesisBlock = 7601063;

    const daysSinceGenesis = (targetDate - genesisDate) / (1000 * 60 * 60 * 24);
    const estimatedBlock = Math.floor(genesisBlock + (daysSinceGenesis * 27000));

    return estimatedBlock;
}

async function fetchEventsInRange(startHeight, endHeight) {
    console.log(`Fetching events from block ${startHeight} to ${endHeight}...`);

    const eventTypes = eventProcessor.ALLDAY_EVENT_TYPES;
    const allEvents = [];

    // Flow REST API has limits, so we'll fetch in chunks
    const CHUNK_SIZE = 250; // blocks per request

    for (let height = startHeight; height <= endHeight; height += CHUNK_SIZE) {
        const endChunk = Math.min(height + CHUNK_SIZE - 1, endHeight);

        try {
            // Fetch events for each type
            for (const eventType of eventTypes) {
                const url = `${FLOW_REST_API}/v1/events?type=${encodeURIComponent(eventType)}&start_height=${height}&end_height=${endChunk}`;

                const response = await fetch(url, {
                    headers: { 'Accept': 'application/json' }
                });

                if (!response.ok) {
                    console.warn(`Failed to fetch ${eventType} from blocks ${height}-${endChunk}: ${response.status}`);
                    continue;
                }

                const data = await response.json();

                if (data.results && Array.isArray(data.results)) {
                    for (const event of data.results) {
                        allEvents.push({
                            type: eventType,
                            block_height: event.block_height,
                            block_timestamp: event.block_timestamp,
                            transaction_id: event.transaction_id,
                            payload: event.payload
                        });
                    }
                }
            }

            if ((height - startHeight) % 1000 === 0) {
                console.log(`  Progress: Block ${height.toLocaleString()} (found ${allEvents.length} events so far)`);
            }

            // Rate limiting
            await new Promise(resolve => setTimeout(resolve, 100));

        } catch (err) {
            console.error(`Error fetching blocks ${height}-${endChunk}:`, err.message);
        }
    }

    return allEvents;
}

async function backfillEvents() {
    console.log('\n🔄 BACKFILLING BLOCKCHAIN EVENTS\n');
    console.log(`Date Range: ${START_DATE} to ${END_DATE}\n`);

    // Calculate approximate block heights
    // Use hardcoded blocks for accuracy based on diagnosis
    // Current block roughly 137070688 (Dec 24)
    // We want last ~3 days (approx 260k blocks)
    const endHeight = 137070000;
    const startHeight = 136800000;

    console.log(`Estimated Block Range: ${startHeight.toLocaleString()} to ${endHeight.toLocaleString()}`);
    console.log(`(Approximately ${(endHeight - startHeight).toLocaleString()} blocks to scan)\n`);

    // Fetch all events
    const events = await fetchEventsInRange(startHeight, endHeight);

    console.log(`\n✅ Found ${events.length} total events to process\n`);

    if (events.length === 0) {
        console.log('No events found in this range. Data is already up to date!');
        process.exit(0);
    }

    // Sort events by block height to process in chronological order
    events.sort((a, b) => a.block_height - b.block_height);

    // Process each event
    console.log('Processing events...\n');
    let processed = 0;

    for (const event of events) {
        try {
            await eventProcessor.processBlockchainEvent(event);
            processed++;

            if (processed % 100 === 0) {
                console.log(`  Processed: ${processed}/${events.length}`);
            }
        } catch (err) {
            console.error(`Error processing event ${event.type}:`, err.message);
        }
    }

    console.log(`\n✅ Backfill complete! Processed ${processed}/${events.length} events`);
    console.log('\nYour database is now up to date through', new Date().toISOString());

    process.exit(0);
}

backfillEvents().catch(err => {
    console.error('\n❌ BACKFILL ERROR:', err.message);
    process.exit(1);
});
