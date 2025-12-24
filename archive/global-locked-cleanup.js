// Global cleanup: Remove ghost locked moments for ALL wallets
import { pgQuery } from './db.js';
import { createSnowflakeConnection, executeSnowflakeWithRetry } from './scripts/snowflake-utils.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function globalLockedCleanup() {
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('  GLOBAL LOCKED GHOST CLEANUP');
    console.log('═══════════════════════════════════════════════════════════\n');

    try {
        // 1. Get TRUE locked moments from Snowflake for ALL wallets
        console.log('Step 1: Querying Snowflake for ALL currently locked moments...');
        const conn = await createSnowflakeConnection();

        const snowflakeSql = `
            WITH my_locked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    LOWER(EVENT_DATA:to::STRING) as WALLET,
                    BLOCK_TIMESTAMP,
                    BLOCK_HEIGHT
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND EVENT_TYPE = 'NFTLocked' 
                AND TX_SUCCEEDED = true
                QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING, LOWER(EVENT_DATA:to::STRING) ORDER BY BLOCK_HEIGHT DESC) = 1
            ),
            my_unlocked_events AS (
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID,
                    LOWER(EVENT_DATA:from::STRING) as WALLET,
                    BLOCK_TIMESTAMP,
                    BLOCK_HEIGHT
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
                AND EVENT_TYPE = 'NFTUnlocked' 
                AND TX_SUCCEEDED = true
                QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING, LOWER(EVENT_DATA:from::STRING) ORDER BY BLOCK_HEIGHT DESC) = 1
            )
            SELECT l.WALLET, l.NFT_ID
            FROM my_locked_events l
            LEFT JOIN my_unlocked_events u 
                ON l.NFT_ID = u.NFT_ID 
                AND l.WALLET = u.WALLET
                AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
            WHERE u.NFT_ID IS NULL
        `;

        console.log('   Executing Snowflake query (this may take a few minutes)...');
        const snowflakeRows = await executeSnowflakeWithRetry(conn, snowflakeSql, { maxRetries: 2 });

        // Build a map: wallet -> Set of locked NFT IDs
        const trueLocked = new Map();
        for (const row of snowflakeRows) {
            const wallet = row.WALLET;
            const nftId = row.NFT_ID;
            if (!trueLocked.has(wallet)) {
                trueLocked.set(wallet, new Set());
            }
            trueLocked.get(wallet).add(nftId);
        }

        console.log(`   ✅ Snowflake returned ${snowflakeRows.length} locked moments across ${trueLocked.size} wallets\n`);

        // 2. Get all wallets with locked moments in our database
        console.log('Step 2: Querying database for wallets with locked moments...');
        const walletsResult = await pgQuery(`
            SELECT DISTINCT wallet_address
            FROM wallet_holdings
            WHERE is_locked = true
        `);
        const wallets = walletsResult.rows.map(r => r.wallet_address);
        console.log(`   Found ${wallets.length} wallets with locked moments in database\n`);

        // 3. For each wallet, find and remove ghosts
        console.log('Step 3: Finding and removing ghost locked moments...\n');
        let totalGhostsRemoved = 0;
        let walletsProcessed = 0;
        let walletsWithGhosts = 0;

        for (const wallet of wallets) {
            // Get database locked moments for this wallet
            const dbResult = await pgQuery(
                `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
                [wallet]
            );
            const dbLockedIds = dbResult.rows.map(r => r.nft_id);

            // Get Snowflake locked moments for this wallet
            const snowflakeLockedIds = trueLocked.get(wallet) || new Set();

            // Find ghosts (in DB but not in Snowflake)
            const ghostIds = dbLockedIds.filter(id => !snowflakeLockedIds.has(id));

            if (ghostIds.length > 0) {
                // Remove the ghosts
                const deleteResult = await pgQuery(
                    `DELETE FROM wallet_holdings 
                     WHERE wallet_address = $1 
                     AND is_locked = true 
                     AND nft_id = ANY($2::text[])`,
                    [wallet, ghostIds]
                );

                totalGhostsRemoved += deleteResult.rowCount;
                walletsWithGhosts++;

                if (walletsWithGhosts <= 10) {
                    console.log(`   ${wallet.substring(0, 10)}... removed ${deleteResult.rowCount} ghosts`);
                }
            }

            walletsProcessed++;
            if (walletsProcessed % 100 === 0) {
                console.log(`   Progress: ${walletsProcessed}/${wallets.length} wallets processed...`);
            }
        }

        console.log(`\n═══════════════════════════════════════════════════════════`);
        console.log('  CLEANUP COMPLETE');
        console.log('═══════════════════════════════════════════════════════════');
        console.log(`Total wallets processed: ${walletsProcessed}`);
        console.log(`Wallets with ghosts: ${walletsWithGhosts}`);
        console.log(`Total ghost moments removed: ${totalGhostsRemoved}`);
        console.log('═══════════════════════════════════════════════════════════\n');

    } catch (err) {
        console.error('\n❌ Error:', err.message);
        console.error(err.stack);
    }

    process.exit(0);
}

globalLockedCleanup();
