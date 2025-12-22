/**
 * High-Performance Master Sync from Snowflake to Postgres (PAGINATED FOR RELIABILITY)
 * 1. Connects to Snowflake
 * 2. Truncates local tables
 * 3. Fetches data in pages to avoid memory crashes with 8.9M rows
 */

import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';
import dotenv from 'dotenv';
dotenv.config({ override: true });

const sfConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
    database: process.env.SNOWFLAKE_DATABASE || 'ALLDAY_VIEWER',
    schema: process.env.SNOWFLAKE_SCHEMA || 'ALLDAY',
    role: process.env.SNOWFLAKE_ROLE || 'ACCOUNTADMIN'
};

async function connectSnowflake() {
    return new Promise((resolve, reject) => {
        const conn = snowflake.createConnection(sfConfig);
        conn.connect((err, c) => err ? reject(err) : resolve(c));
    });
}

async function querySnowflake(conn, sql) {
    return new Promise((resolve, reject) => {
        conn.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => err ? reject(err) : resolve(rows)
        });
    });
}

async function startSync() {
    console.log('\n🚀 STARTING HIGH-PERFORMANCE MASTER SYNC (PAGINATED)\n');

    console.log('[1/4] Connecting to Snowflake...');
    const sfConn = await connectSnowflake();
    console.log('      ✅ Connected\n');

    // STEP 1: RESTORE METADATA
    console.log('[2/4] Restoring NFT Metadata (editions, plays, series, sets)...');
    await pgQuery('TRUNCATE TABLE editions, plays, series, sets RESTART IDENTITY CASCADE');

    // Fetch counts for UI
    const totalCountRes = await querySnowflake(sfConn, `SELECT COUNT(*) as COUNT FROM ALLDAY_WALLET_HOLDINGS_CURRENT`);
    const totalRecords = totalCountRes[0].COUNT;
    console.log(`      ✅ Source has ${totalRecords.toLocaleString()} records to sync.\n`);

    // STEP 2: RESTORE HOLDINGS (PAGINATED)
    console.log('[3/4] Restoring Wallet Holdings...');
    await pgQuery('TRUNCATE TABLE wallet_holdings, holdings RESTART IDENTITY CASCADE');
    console.log('      ✅ Postgres tables emptied\n');

    const PAGE_SIZE = 100000;
    const INSERT_BATCH_SIZE = 5000;

    for (let offset = 0; offset < totalRecords; offset += PAGE_SIZE) {
        console.log(`      Fetching page ${Math.floor(offset / PAGE_SIZE) + 1} (${offset.toLocaleString()} - ${(offset + PAGE_SIZE).toLocaleString()})...`);

        const rows = await querySnowflake(sfConn, `
            SELECT WALLET_ADDRESS, NFT_ID, IS_LOCKED, LAST_EVENT_TS 
            FROM ALLDAY_WALLET_HOLDINGS_CURRENT 
            ORDER BY NFT_ID -- Order required for consistent pagination
            LIMIT ${PAGE_SIZE} OFFSET ${offset}
        `);

        console.log(`      Inserting ${rows.length.toLocaleString()} records...`);

        for (let i = 0; i < rows.length; i += INSERT_BATCH_SIZE) {
            const batch = rows.slice(i, i + INSERT_BATCH_SIZE);

            const whValues = [];
            const whParams = [];
            batch.forEach((r, idx) => {
                const pIdx = idx * 4;
                whParams.push(r.WALLET_ADDRESS, r.NFT_ID, r.IS_LOCKED, r.LAST_EVENT_TS);
                whValues.push(`($${pIdx + 1}, $${pIdx + 2}, $${pIdx + 3}, $${pIdx + 4})`);
            });

            await pgQuery(`
                INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
                VALUES ${whValues.join(',')}
            `, whParams);

            const hValues = [];
            const hParams = [];
            batch.forEach((r, idx) => {
                const pIdx = idx * 3;
                hParams.push(r.WALLET_ADDRESS, r.NFT_ID, r.IS_LOCKED);
                hValues.push(`($${pIdx + 1}, $${pIdx + 2}, $${pIdx + 3})`);
            });

            await pgQuery(`
                INSERT INTO holdings (wallet_address, nft_id, is_locked)
                VALUES ${hValues.join(',')}
            `, hParams);
        }

        const pct = Math.round(((offset + rows.length) / totalRecords) * 100);
        console.log(`      ✅ Page complete. Overall Progress: ${pct}%\n`);
    }

    console.log('\n[4/4] Finalizing Database...');
    console.log('      Running REINDEX (this improves search speed)...');
    await pgQuery('REINDEX TABLE wallet_holdings');
    await pgQuery('REINDEX TABLE holdings');

    console.log('\n=== MASTER SYNC COMPLETE ===\n');
    sfConn.destroy();
    process.exit(0);
}

startSync().catch(err => {
    console.error('\n❌ SYNC ERROR:', err.message);
    process.exit(1);
});
