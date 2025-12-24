import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';
import dotenv from 'dotenv';
dotenv.config();

async function investigate() {
    console.log("🕵️‍♂️ Investigating Kaladin's Wallet Issues...");
    const wallet = '0x93914b2bfb28d59d';

    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    connection.connect(async (err, conn) => {
        if (err) { console.error('SF Connect Error', err); process.exit(1); }

        // 1. Check Truth for NFT 8229357 (Showing Dec 23 in DB)
        console.log('\n--- Checking NFT 8229357 (Suspicious Date) ---');
        const dateCheckSql = `
            SELECT BLOCK_TIMESTAMP, EVENT_DATA:to::STRING as OWNER 
            FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
            WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' 
              AND EVENT_TYPE = 'Deposit' 
              AND EVENT_DATA:id::STRING = '8229357'
            ORDER BY BLOCK_TIMESTAMP DESC
            LIMIT 1
        `;
        conn.execute({
            sqlText: dateCheckSql,
            complete: (err, stmt, rows) => {
                if (err) console.error(err);
                else {
                    const row = rows[0];
                    console.log(`Snowflake Truth: ${row?.BLOCK_TIMESTAMP} (Owner: ${row?.OWNER})`);
                    if (row?.OWNER?.toLowerCase() === wallet) {
                        console.log('Owner matches. Date comparison:');
                        console.log('SF: ' + row.BLOCK_TIMESTAMP);
                        console.log('DB: 2025-12-23 (approx)');
                    }
                }
            }
        });

        // 2. Find Missing "Drake Maye" (Recent Deposits)
        console.log('\n--- Searching for Missing Recent Deposits ---');
        const depositSql = `
            SELECT 
                EVENT_DATA:id::STRING as NFT_ID, 
                BLOCK_TIMESTAMP
            FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
            WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' 
              AND EVENT_TYPE = 'Deposit' 
              AND LOWER(EVENT_DATA:to::STRING) = '${wallet}'
              AND BLOCK_TIMESTAMP >= '2025-12-01' -- Look back 3 weeks
            ORDER BY BLOCK_TIMESTAMP DESC
        `;

        conn.execute({
            sqlText: depositSql,
            complete: async (err, stmt, rows) => {
                if (err) console.error(err);
                else {
                    console.log(`Found ${rows.length} deposits since Dec 1st.`);

                    // Cross reference with DB
                    const nftIds = rows.map(r => r.NFT_ID);
                    if (nftIds.length === 0) return;

                    const dbRes = await pgQuery(`
                        SELECT nft_id FROM holdings WHERE nft_id = ANY($1)
                    `, [nftIds]);
                    const dbIds = new Set(dbRes.rows.map(r => r.nft_id));

                    const missing = rows.filter(r => !dbIds.has(r.NFT_ID));
                    console.log(`\nMISSING NFTs (${missing.length}):`);
                    missing.forEach(r => console.log(`- ID: ${r.NFT_ID} (Date: ${r.BLOCK_TIMESTAMP})`));

                    // Note: We'd need metadata to know which one is Drake Maye, 
                    // but listing the ID allows us to fix it.
                }

                // Cleanup
                // conn.destroy(); // Wait for parallel query? SF driver might close.
                // Just exit after specific timeout or nesting. Use nesting for safety.
                conn.destroy();
            }
        });
    });
}

investigate();
