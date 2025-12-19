/**
 * Final Snowflake Sync - Locked NFT Status (OPTIMIZED)
 * 
 * Uses batch UPDATE queries instead of individual row updates.
 * Should complete in ~10 minutes instead of 55 hours.
 * 
 * Usage: node scripts/sync_locked_from_snowflake.js
 */

import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';

// Snowflake connection config from environment
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

async function syncLockedNFTs() {
  console.log('=== Final Snowflake Sync - Locked NFT Status (OPTIMIZED) ===\n');
  const startTime = Date.now();

  // Connect to Snowflake
  console.log('1. Connecting to Snowflake...');
  let sfConnection;
  try {
    sfConnection = await createSnowflakeConnection();
    console.log('   ✅ Connected to Snowflake\n');
  } catch (err) {
    console.error('   ❌ Failed to connect:', err.message);
    process.exit(1);
  }

  // Query for all currently locked NFTs
  console.log('2. Querying locked NFTs from Snowflake...');

  const lockedNFTsQuery = `
    WITH burned_nfts AS (
      SELECT EVENT_DATA:id::STRING as NFT_ID
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_TYPE='MomentNFTBurned' AND TX_SUCCEEDED = true AND BLOCK_TIMESTAMP >= '2021-01-01'
    ),
    
    locked_events AS (
      SELECT 
        EVENT_DATA:id::STRING as NFT_ID, 
        EVENT_DATA:to::STRING as WALLET_ADDRESS,
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
      SELECT 
        l.NFT_ID,
        LOWER(l.WALLET_ADDRESS) as WALLET_ADDRESS
      FROM locked_events l
      LEFT JOIN unlocked_events u ON l.NFT_ID = u.NFT_ID AND u.BLOCK_HEIGHT > l.BLOCK_HEIGHT
      WHERE u.NFT_ID IS NULL
        AND l.NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
    )
    
    SELECT NFT_ID, WALLET_ADDRESS
    FROM currently_locked
  `;

  let lockedNFTs;
  try {
    lockedNFTs = await executeSnowflakeQuery(sfConnection, lockedNFTsQuery);
    console.log(`   ✅ Found ${lockedNFTs.length.toLocaleString()} locked NFTs\n`);
  } catch (err) {
    console.error('   ❌ Snowflake query failed:', err.message);
    process.exit(1);
  }

  // Close Snowflake connection early
  sfConnection.destroy();

  // Reset ALL locked status to false first (single query)
  console.log('3. Resetting all locked status in PostgreSQL...');
  await pgQuery(`UPDATE wallet_holdings SET is_locked = false`);
  await pgQuery(`UPDATE holdings SET is_locked = false`);
  console.log('   ✅ Reset complete\n');

  // Build map of wallet -> nft_ids for efficient updates
  console.log('4. Preparing batch updates...');
  const walletToNfts = new Map();
  const allNftIds = [];

  for (const row of lockedNFTs) {
    const wallet = row.WALLET_ADDRESS;
    const nftId = row.NFT_ID;
    allNftIds.push(nftId);

    if (!walletToNfts.has(wallet)) {
      walletToNfts.set(wallet, []);
    }
    walletToNfts.get(wallet).push(nftId);
  }
  console.log(`   ${walletToNfts.size.toLocaleString()} unique wallets with locked NFTs\n`);

  // Batch update all locked NFTs (by nft_id)
  console.log('5. Batch updating locked status...');
  const BATCH_SIZE = 1000;
  let updated = 0;

  for (let i = 0; i < allNftIds.length; i += BATCH_SIZE) {
    const batch = allNftIds.slice(i, i + BATCH_SIZE);

    // Update wallet_holdings - set is_locked = true for these NFT IDs
    await pgQuery(`
      UPDATE wallet_holdings SET is_locked = true 
      WHERE nft_id = ANY($1)
    `, [batch]);

    // Update holdings table too
    await pgQuery(`
      UPDATE holdings SET is_locked = true 
      WHERE nft_id = ANY($1)
    `, [batch]);

    updated += batch.length;
    console.log(`   Progress: ${updated.toLocaleString()} / ${allNftIds.length.toLocaleString()} NFTs`);
  }

  console.log(`   ✅ Updated ${updated.toLocaleString()} locked NFTs\n`);

  // Now ensure wallet_holdings has entries for locked NFTs with correct owners
  console.log('6. Ensuring locked NFTs have correct wallet ownership...');
  let ensured = 0;

  for (const [wallet, nftIds] of walletToNfts) {
    for (let i = 0; i < nftIds.length; i += BATCH_SIZE) {
      const batch = nftIds.slice(i, i + BATCH_SIZE);

      // Upsert: ensure this wallet owns these locked NFTs
      for (const nftId of batch) {
        await pgQuery(`
          INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
          VALUES ($1, $2, true, NOW())
          ON CONFLICT (wallet_address, nft_id) DO UPDATE SET is_locked = true
        `, [wallet, nftId]);
      }

      ensured += batch.length;
    }

    if (ensured % 10000 === 0) {
      console.log(`   Progress: ${ensured.toLocaleString()} / ${allNftIds.length.toLocaleString()} ownership entries`);
    }
  }

  console.log(`   ✅ Ensured ${ensured.toLocaleString()} wallet ownership entries\n`);

  // Verify JungleRules
  console.log('7. Verifying JungleRules wallet...');
  const JUNGLE_RULES = '0xcfd9bad75352b43b';

  const jrCount = await pgQuery(`
    SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE is_locked) as locked
    FROM wallet_holdings WHERE wallet_address = $1
  `, [JUNGLE_RULES]);

  console.log(`   JungleRules: ${jrCount.rows[0].total} total, ${jrCount.rows[0].locked} locked`);
  console.log('   NFL All Day: 2589 total');

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log(`\n=== SYNC COMPLETE in ${Math.floor(elapsed / 60)}m ${elapsed % 60}s ===`);

  process.exit(0);
}

syncLockedNFTs().catch(err => {
  console.error('Sync failed:', err);
  process.exit(1);
});
