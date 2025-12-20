/**
 * Quick fix: Restore locked NFTs for a specific wallet from Snowflake
 * Then we'll disable Snowflake forever
 */

import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';

const WALLET = process.argv[2];
if (!WALLET) {
    console.error('Usage: node scripts/restore_locked_for_wallet.js 0x...');
    process.exit(1);
}

const sfConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database: process.env.SNOWFLAKE_DATABASE || 'FLOW_ONCHAIN_CORE_DATA',
    schema: process.env.SNOWFLAKE_SCHEMA || 'CORE',
    role: process.env.SNOWFLAKE_ROLE || 'ACCOUNTADMIN'
};

function createConnection() {
    return new Promise((resolve, reject) => {
        const conn = snowflake.createConnection(sfConfig);
        conn.connect((err, c) => err ? reject(err) : resolve(c));
    });
}

function executeQuery(connection, sql) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => err ? reject(err) : resolve(rows)
        });
    });
}

async function restore() {
    console.log(`\n=== Restoring Locked NFTs for ${WALLET} ===\n`);

    const conn = await createConnection();
    console.log('✅ Connected to Snowflake\n');

    const query = `
    WITH burned AS (
      SELECT EVENT_DATA:id::STRING as id
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_TYPE='MomentNFTBurned' AND TX_SUCCEEDED = true
    ),
    locked_events AS (
      SELECT 
        EVENT_DATA:id::STRING as id,
        LOWER(EVENT_DATA:to::STRING) as wallet,
        BLOCK_HEIGHT
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE 
        EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
        EVENT_TYPE = 'NFTLocked' AND
        TX_SUCCEEDED = true
      QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
    ),
    unlocked_events AS (
      SELECT 
        EVENT_DATA:id::STRING as id,
        BLOCK_HEIGHT
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE 
        EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
        EVENT_TYPE = 'NFTUnlocked' AND
        TX_SUCCEEDED = true
      QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
    )
    SELECT l.id as NFT_ID
    FROM locked_events l
    LEFT JOIN unlocked_events u ON l.id = u.id AND u.BLOCK_HEIGHT > l.BLOCK_HEIGHT
    WHERE u.id IS NULL 
      AND l.id NOT IN (SELECT id FROM burned)
      AND l.wallet = LOWER('${WALLET}')
  `;

    console.log('Querying Snowflake for locked NFTs...');
    const lockedNFTs = await executeQuery(conn, query);
    conn.destroy();

    console.log(`✅ Found ${lockedNFTs.length} locked NFTs\n`);

    if (lockedNFTs.length === 0) {
        console.log('No locked NFTs to update. Done.');
        process.exit(0);
    }

    // Update database
    console.log('Updating database...');
    const nftIds = lockedNFTs.map(r => r.NFT_ID);

    // Set these specific NFTs to locked
    await pgQuery(`
    UPDATE wallet_holdings 
    SET is_locked = true 
    WHERE wallet_address = $1 AND nft_id = ANY($2)
  `, [WALLET.toLowerCase(), nftIds]);

    await pgQuery(`
    UPDATE holdings 
    SET is_locked = true 
    WHERE wallet_address = $1 AND nft_id = ANY($2)
  `, [WALLET.toLowerCase(), nftIds]);

    console.log(`✅ Updated ${nftIds.length} NFTs to locked\n`);

    // Verify
    const check = await pgQuery(`
    SELECT COUNT(*) FILTER (WHERE is_locked) as locked
    FROM wallet_holdings 
    WHERE wallet_address = $1
  `, [WALLET.toLowerCase()]);

    console.log(`Verification: ${check.rows[0].locked} locked NFTs in database\n`);
    console.log('=== COMPLETE ===');
    process.exit(0);
}

restore().catch(err => {
    console.error('Failed:', err);
    process.exit(1);
});
