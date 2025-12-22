/**
 * Incremental Locked Sync - No User Disruption
 * 
 * This script:
 * 1. Queries Snowflake for currently locked NFTs
 * 2. Compares with database
 * 3. Only updates what changed (no full reset)
 * 
 * Usage: node scripts/incremental_locked_sync.js
 */

import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';

const sfConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database: process.env.SNOWFLAKE_DATABASE || 'FLOW_ONCHAIN_CORE_DATA',
    schema: process.env.SNOWFLAKE_SCHEMA || 'CORE',
    role: process.env.SNOWFLAKE_ROLE || 'ACCOUNTADMIN'
};

function createSnowflakeConnection() {
    return new Promise((resolve, reject) => {
        const connection = snowflake.createConnection(sfConfig);
        connection.connect((err, conn) => {
            if (err) reject(err);
            else resolve(conn);
        });
    });
}

function executeSnowflakeQuery(connection, sql) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => {
                if (err) reject(err);
                else resolve(rows);
            }
        });
    });
}

async function incrementalSync() {
    console.log('=== Incremental Locked NFT Sync (Zero Downtime) ===\n');
    const startTime = Date.now();

    // Connect to Snowflake
    console.log('1. Connecting to Snowflake...');
    let sfConnection;
    try {
        sfConnection = await createSnowflakeConnection();
        console.log('   ✅ Connected\n');
    } catch (err) {
        console.error('   ❌ Failed:', err.message);
        process.exit(1);
    }

    // Query currently locked NFTs from Snowflake
    console.log('2. Querying locked NFTs from Snowflake...');
    const lockedQuery = `
    WITH burned_nfts AS (
      SELECT EVENT_DATA:id::STRING as NFT_ID
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_TYPE='MomentNFTBurned' AND TX_SUCCEEDED = true AND BLOCK_TIMESTAMP >= '2021-01-01'
    ),
    locked_events AS (
      SELECT 
        EVENT_DATA:id::STRING as NFT_ID, 
        BLOCK_HEIGHT
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE 
        EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
        EVENT_TYPE = 'NFTLocked' AND
        TX_SUCCEEDED = true AND
        BLOCK_TIMESTAMP >= '2021-01-01'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
    ),
    unlocked_events AS (
      SELECT 
        EVENT_DATA:id::STRING as NFT_ID,
        BLOCK_HEIGHT
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE 
        EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
        EVENT_TYPE = 'NFTUnlocked' AND
        TX_SUCCEEDED = true AND
        BLOCK_TIMESTAMP >= '2021-01-01'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
    ),
    currently_locked AS (
      SELECT l.NFT_ID
      FROM locked_events l
      LEFT JOIN unlocked_events u ON l.NFT_ID = u.NFT_ID AND u.BLOCK_HEIGHT > l.BLOCK_HEIGHT
      WHERE u.NFT_ID IS NULL
        AND l.NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
    )
    SELECT NFT_ID FROM currently_locked
  `;

    let snowflakeLocked;
    try {
        snowflakeLocked = await executeSnowflakeQuery(sfConnection, lockedQuery);
        console.log(`   ✅ Found ${snowflakeLocked.length.toLocaleString()} locked NFTs in Snowflake\n`);
    } catch (err) {
        console.error('   ❌ Query failed:', err.message);
        process.exit(1);
    }
    sfConnection.destroy();

    // Get currently locked NFTs from database
    console.log('3. Querying locked NFTs from database...');
    const dbResult = await pgQuery(`
    SELECT DISTINCT nft_id 
    FROM wallet_holdings 
    WHERE is_locked = true
  `);
    const dbLocked = new Set(dbResult.rows.map(r => r.nft_id));
    console.log(`   ✅ Found ${dbLocked.size.toLocaleString()} locked NFTs in database\n`);

    // Calculate differences
    const snowflakeSet = new Set(snowflakeLocked.map(r => r.NFT_ID));
    const toUnlock = Array.from(dbLocked).filter(id => !snowflakeSet.has(id));
    const toLock = snowflakeLocked.filter(r => !dbLocked.has(r.NFT_ID)).map(r => r.NFT_ID);

    console.log('4. Calculating changes...');
    console.log(`   Newly locked: ${toLock.length.toLocaleString()}`);
    console.log(`   Newly unlocked: ${toUnlock.length.toLocaleString()}\n`);

    // Apply changes in batches
    const BATCH_SIZE = 1000;

    if (toLock.length > 0) {
        console.log('5. Applying lock updates...');
        for (let i = 0; i < toLock.length; i += BATCH_SIZE) {
            const batch = toLock.slice(i, i + BATCH_SIZE);
            await pgQuery(`UPDATE wallet_holdings SET is_locked = true WHERE nft_id = ANY($1)`, [batch]);
            await pgQuery(`UPDATE holdings SET is_locked = true WHERE nft_id = ANY($1)`, [batch]);
            console.log(`   Progress: ${Math.min(i + BATCH_SIZE, toLock.length).toLocaleString()} / ${toLock.length.toLocaleString()}`);
        }
        console.log('   ✅ Lock updates complete\n');
    }

    if (toUnlock.length > 0) {
        console.log('6. Applying unlock updates...');
        for (let i = 0; i < toUnlock.length; i += BATCH_SIZE) {
            const batch = toUnlock.slice(i, i + BATCH_SIZE);
            await pgQuery(`UPDATE wallet_holdings SET is_locked = false WHERE nft_id = ANY($1)`, [batch]);
            await pgQuery(`UPDATE holdings SET is_locked = false WHERE nft_id = ANY($1)`, [batch]);
            console.log(`   Progress: ${Math.min(i + BATCH_SIZE, toUnlock.length).toLocaleString()} / ${toUnlock.length.toLocaleString()}`);
        }
        console.log('   ✅ Unlock updates complete\n');
    }

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.log(`=== SYNC COMPLETE in ${Math.floor(elapsed / 60)}m ${elapsed % 60}s ===`);
    console.log(`Total changes: ${toLock.length + toUnlock.length} NFTs updated`);

    process.exit(0);
}

incrementalSync().catch(err => {
    console.error('Sync failed:', err);
    process.exit(1);
});
