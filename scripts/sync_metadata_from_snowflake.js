/**
 * Sync NFT Metadata from Snowflake to Postgres
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

async function syncMetadata() {
    console.log('\n🔄 SYNCING METADATA FROM SNOWFLAKE\n');

    const sfConn = await connectSnowflake();
    console.log('✅ Connected to Snowflake\n');

    const PAGE_SIZE = 100000;
    const BATCH_SIZE = 500;  // 15 params * 500 = 7500 params (well under 65535 limit)

    const countRes = await querySnowflake(sfConn, `SELECT COUNT(*) as COUNT FROM ALLDAY_CORE_NFT_METADATA`);
    const total = countRes[0].COUNT;
    console.log(`Found ${total.toLocaleString()} metadata records to sync...\n`);

    for (let offset = 0; offset < total; offset += PAGE_SIZE) {
        console.log(`Fetching page ${Math.floor(offset / PAGE_SIZE) + 1} (${offset.toLocaleString()} - ${(offset + PAGE_SIZE).toLocaleString()})...`);

        const rows = await querySnowflake(sfConn, `
            SELECT NFT_ID, EDITION_ID, PLAY_ID, SERIES_ID, SET_ID, TIER, 
                   SERIAL_NUMBER, MAX_MINT_SIZE, FIRST_NAME, LAST_NAME, 
                   TEAM_NAME, POSITION, JERSEY_NUMBER, SERIES_NAME, SET_NAME
            FROM ALLDAY_CORE_NFT_METADATA 
            ORDER BY NFT_ID
            LIMIT ${PAGE_SIZE} OFFSET ${offset}
        `);

        console.log(`Inserting ${rows.length.toLocaleString()} records...`);

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);

            const values = [];
            const params = [];

            // Helper: convert empty string integers to null
            const toInt = (val) => {
                if (val === '' || val === null || val === undefined) return null;
                const num = Number(val);
                return isNaN(num) ? null : num;
            };

            batch.forEach((r, idx) => {
                const pIdx = idx * 15;
                params.push(
                    r.NFT_ID, r.EDITION_ID, r.PLAY_ID, r.SERIES_ID, r.SET_ID,
                    r.TIER, toInt(r.SERIAL_NUMBER), toInt(r.MAX_MINT_SIZE), r.FIRST_NAME,
                    r.LAST_NAME, r.TEAM_NAME, r.POSITION, toInt(r.JERSEY_NUMBER),
                    r.SERIES_NAME, r.SET_NAME
                );
                values.push(`($${pIdx + 1}, $${pIdx + 2}, $${pIdx + 3}, $${pIdx + 4}, $${pIdx + 5}, $${pIdx + 6}, $${pIdx + 7}, $${pIdx + 8}, $${pIdx + 9}, $${pIdx + 10}, $${pIdx + 11}, $${pIdx + 12}, $${pIdx + 13}, $${pIdx + 14}, $${pIdx + 15})`);
            });

            await pgQuery(`
                INSERT INTO nft_core_metadata_v2 (
                    nft_id, edition_id, play_id, series_id, set_id, tier,
                    serial_number, max_mint_size, first_name, last_name,
                    team_name, position, jersey_number, series_name, set_name
                ) VALUES ${values.join(',')}
                ON CONFLICT (nft_id) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name
            `, params);
        }

        const pct = Math.round(((offset + rows.length) / total) * 100);
        console.log(`✅ Page complete. Progress: ${pct}%\n`);
    }

    console.log('\n=== METADATA SYNC COMPLETE ===\n');
    sfConn.destroy();
    process.exit(0);
}

syncMetadata().catch(err => {
    console.error('\n❌ SYNC ERROR:', err.message);
    process.exit(1);
});
