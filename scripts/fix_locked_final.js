/**
 * Fix Locked NFTs - Final restoration with progress
 */

import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';

const WALLET = '0x7541bafd155b683e';

const sfConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database: process.env.SNOWFLAKE_DATABASE || 'FLOW_ONCHAIN_CORE_DATA',
    schema: process.env.SNOWFLAKE_SCHEMA || 'CORE',
    role: process.env.SNOWFLAKE_ROLE || 'ACCOUNTADMIN'
};

function connect() {
    return new Promise((resolve, reject) => {
        const conn = snowflake.createConnection(sfConfig);
        conn.connect((err, c) => err ? reject(err) : resolve(c));
    });
}

function query(conn, sql) {
    return new Promise((resolve, reject) => {
        conn.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => err ? reject(err) : resolve(rows)
        });
    });
}

async function fix() {
    console.log('\n=== RESTORING LOCKED NFTS FOR', WALLET, '===\n');
    console.log('[1/5] Connecting to Snowflake...');
    const conn = await connect();
    console.log('      ✅ Connected\n');

    console.log('[2/5] Querying locked NFTs (this takes ~2-3 minutes for large dataset)...');
    const start = Date.now();

    const sql = `
    WITH locked_events AS (
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
    SELECT l.id
    FROM locked_events l
    LEFT JOIN unlocked_events u ON l.id = u.id AND u.BLOCK_HEIGHT > l.BLOCK_HEIGHT
    WHERE u.id IS NULL AND l.wallet = '${WALLET.toLowerCase()}'
  `;

    const locked = await query(conn, sql);
    conn.destroy();

    const elapsed = Math.round((Date.now() - start) / 1000);
    console.log(`      ✅ Found ${locked.length} locked NFTs (${elapsed}s)\n`);

    if (locked.length === 0) {
        console.log('      ⚠️  No locked NFTs found. Exiting.');
        process.exit(0);
    }

    const ids = locked.map(r => r.ID);

    console.log('[3/5] Updating wallet_holdings...');
    await pgQuery(`UPDATE wallet_holdings SET is_locked = true WHERE wallet_address = $1 AND nft_id = ANY($2)`, [WALLET.toLowerCase(), ids]);
    console.log('      ✅ Updated\n');

    console.log('[4/5] Updating holdings...');
    await pgQuery(`UPDATE holdings SET is_locked = true WHERE wallet_address = $1 AND nft_id = ANY($2)`, [WALLET.toLowerCase(), ids]);
    console.log('      ✅ Updated\n');

    console.log('[5/5] Verifying...');
    const check = await pgQuery(`SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM wallet_holdings WHERE wallet_address = $1`, [WALLET.toLowerCase()]);
    console.log(`      ✅ Database shows ${check.rows[0].locked} locked NFTs\n`);

    console.log('=== COMPLETE ===\n');
    process.exit(0);
}

fix().catch(err => {
    console.error('\n❌ ERROR:', err.message);
    process.exit(1);
});
