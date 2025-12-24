import { createSnowflakeConnection, executeSnowflakeWithRetry } from "./scripts/snowflake-utils.js";
import * as dotenv from "dotenv";
dotenv.config();

const WALLET = '0xcfd9bad75352b43b';
// These are the "Locked" IDs from our DB that are NOT in the 3 found on Marketplace.
// I'll query for *all* locked IDs for this wallet from Snowflake 
// and see if I can find the ones that have a later Unlock event that was missed.

async function checkGhostEvents() {
    console.log(`Checking Locked Event History for ${WALLET}...`);
    try {
        const conn = await createSnowflakeConnection();

        // 1. Get ALL Locked Events for this wallet
        // This is the same logic as the sync script but I want to see the RAW data
        // to find where the mismatch is.
        const sql = `
            WITH locked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    BLOCK_TIMESTAMP as LOCKED_AT,
                    TX_ID as LOCK_TX
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND LOWER(EVENT_DATA:to::STRING) = LOWER('${WALLET}') 
                AND EVENT_TYPE = 'NFTLocked' 
                AND TX_SUCCEEDED = true
            ),
            unlocked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    BLOCK_TIMESTAMP as UNLOCKED_AT,
                    TX_ID as UNLOCK_TX
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND LOWER(EVENT_DATA:from::STRING) = LOWER('${WALLET}') 
                AND EVENT_TYPE = 'NFTUnlocked' 
                AND TX_SUCCEEDED = true
            )
            SELECT 
                l.NFT_ID, 
                l.LOCKED_AT, 
                u.UNLOCKED_AT,
                CASE WHEN u.UNLOCKED_AT IS NULL THEN 'LOCKED_FOREVER'
                     WHEN u.UNLOCKED_AT > l.LOCKED_AT THEN 'UNLOCKED_OK'
                     ELSE 'UNLOCKED_BEFORE_LOCK_WEIRD'
                END as STATUS
            FROM locked_events l
            LEFT JOIN unlocked_events u ON l.NFT_ID = u.NFT_ID
            ORDER BY l.LOCKED_AT DESC
        `;

        console.log("Running Snowflake query...");
        const rows = await executeSnowflakeWithRetry(conn, sql, { maxRetries: 2 });

        console.log(`Total Rows: ${rows.length}`);

        // Count statuses
        const counts = {};
        rows.forEach(r => {
            counts[r.STATUS] = (counts[r.STATUS] || 0) + 1;
        });
        console.log("Status Counts:", counts);

        // Find potential ghosts: 
        // These would be items that ARE 'LOCKED_FOREVER' (according to this query)
        // BUT don't exist in the "True Count" list from NFLAD?
        // Wait, if Snowflake says they are locked, and our DB says they are locked, 
        // then they are "VALIDLY" locked according to the event log.

        // But NFL All Day has 41 FEWER moments.
        // This means NFL All Day knows about an Unlock event (or a Burn/Transfer) that WE don't see.
        // OR filtering logic is different.

        // Is it possible they were burned directly from the Locker?
        // Or transferred FROM the locker to someone else (not the owner)?
        // The un-lock query checks `from = wallet`. What if `from` is the locker address?

    } catch (err) {
        console.error("Error:", err.message);
    }
}

checkGhostEvents();
