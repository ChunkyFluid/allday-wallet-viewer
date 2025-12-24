// Surgical fix: Remove locked moments that aren't in Snowflake
import { pgQuery } from './db.js';
import { createSnowflakeConnection, executeSnowflakeWithRetry } from './scripts/snowflake-utils.js';
import * as dotenv from 'dotenv';
dotenv.config();

const WALLET = '0xcfd9bad75352b43b';

async function fixLockedGhosts() {
    console.log(`\n=== FIXING LOCKED GHOSTS FOR ${WALLET} ===\n`);

    try {
        // 1. Get locked moments from Snowflake (source of truth)
        console.log('Step 1: Querying Snowflake for TRUE locked moments...');
        const conn = await createSnowflakeConnection();

        const snowflakeSql = `
            WITH my_locked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    BLOCK_TIMESTAMP
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND LOWER(EVENT_DATA:to::STRING) = LOWER('${WALLET}') 
                AND EVENT_TYPE = 'NFTLocked' 
                AND TX_SUCCEEDED = true
                QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
            ),
            my_unlocked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    BLOCK_TIMESTAMP
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND LOWER(EVENT_DATA:from::STRING) = LOWER('${WALLET}') 
                AND EVENT_TYPE = 'NFTUnlocked' 
                AND TX_SUCCEEDED = true
                QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
            )
            SELECT l.NFT_ID
            FROM my_locked_events l
            LEFT JOIN my_unlocked_events u ON l.NFT_ID = u.NFT_ID 
                AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
            WHERE u.NFT_ID IS NULL
        `;

        const snowflakeRows = await executeSnowflakeWithRetry(conn, snowflakeSql, { maxRetries: 2 });
        const snowflakeLockedIds = new Set(snowflakeRows.map(r => r.NFT_ID));
        console.log(`  Snowflake says: ${snowflakeLockedIds.size} locked moments`);

        // 2. Get locked moments from database
        console.log('\nStep 2: Querying database for current locked moments...');
        const dbResult = await pgQuery(
            `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
            [WALLET]
        );
        const dbLockedIds = dbResult.rows.map(r => r.nft_id);
        console.log(`  Database has: ${dbLockedIds.length} locked moments`);

        // 3. Find the extras (in DB but not in Snowflake)
        const ghostIds = dbLockedIds.filter(id => !snowflakeLockedIds.has(id));
        console.log(`\n=== FOUND ${ghostIds.length} GHOST LOCKED MOMENTS ===`);

        if (ghostIds.length === 0) {
            console.log('Nothing to fix!');
            process.exit(0);
        }

        console.log('Sample ghost IDs:', ghostIds.slice(0, 10));
        console.log('\nRemoving ghosts from database...');

        // 4. Remove the ghosts
        const deleteResult = await pgQuery(
            `DELETE FROM wallet_holdings 
             WHERE wallet_address = $1 
             AND is_locked = true 
             AND nft_id = ANY($2::text[])`,
            [WALLET, ghostIds]
        );

        console.log(`✅ Removed ${deleteResult.rowCount} ghost locked moments`);

        // 5. Verify final counts
        console.log('\n=== FINAL VERIFICATION ===');
        const finalCount = await pgQuery(
            `SELECT 
                COUNT(*) FILTER (WHERE is_locked = false) as unlocked,
                COUNT(*) FILTER (WHERE is_locked = true) as locked,
                COUNT(*) as total
             FROM wallet_holdings 
             WHERE wallet_address = $1`,
            [WALLET]
        );

        console.log('Database now has:');
        console.log('  Unlocked:', finalCount.rows[0].unlocked);
        console.log('  Locked:', finalCount.rows[0].locked);
        console.log('  Total:', finalCount.rows[0].total);
        console.log('\nExpected (NFL All Day): 2638 total');

    } catch (err) {
        console.error('Error:', err.message);
        console.error(err.stack);
    }

    process.exit(0);
}

fixLockedGhosts();
