import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

async function debugKaladin() {
    // 1. Get Wallet Address (Hardcoded from screenshot)
    const wallet = '0x93914b2bfb28d59d';
    console.log(`Checking Wallet: ${wallet}`);

    // 2. Get Current Holdings Dates in PG
    const current = await pgQuery(`
        SELECT nft_id, acquired_at 
        FROM holdings 
        WHERE wallet_address = $1 
        ORDER BY acquired_at DESC
        LIMIT 5
    `, [wallet]);

    console.log('\n--- Current Postgres Dates ---');
    current.rows.forEach(r => console.log(`NFT ${r.nft_id}: ${r.acquired_at}`));

    // 3. Check Snowflake for Truth
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
        if (err) { console.error('SF Connect Error'); return; }

        console.log('\n--- Snowflake Truth Check ---');
        // Pick one NFT from PG result to check
        if (current.rows.length > 0) {
            const sampleNft = current.rows[0].nft_id;
            console.log(`Checking history for NFT ${sampleNft}...`);

            const sql = `
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID, 
                    LOWER(EVENT_DATA:to::STRING) as WALLET_ADDRESS,
                    BLOCK_TIMESTAMP
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' 
                  AND EVENT_TYPE = 'Deposit' 
                  AND EVENT_DATA:id::STRING = '${sampleNft}'
                ORDER BY BLOCK_TIMESTAMP DESC
            `;

            conn.execute({
                sqlText: sql,
                complete: (err, stmt, rows) => {
                    if (err) console.error(err);
                    else {
                        console.log('Snowflake Deposit Events for this NFT:');
                        console.table(rows);

                        // Check why the script might have skipped it
                        // script logic: WHERE h.wallet_address = v.wallet_owner
                        const latest = rows[0];
                        if (latest && latest.WALLET_ADDRESS === wallet.toLowerCase()) {
                            console.log(`\nMatch! Owner matches.`);
                            console.log(`True Date: ${latest.BLOCK_TIMESTAMP}`);
                            console.log(`PG Date:   ${current.rows[0].acquired_at}`);
                        } else {
                            console.log(`\nMismatch! Latest SF owner (${latest?.WALLET_ADDRESS}) != PG owner (${wallet})`);
                        }
                    }
                    conn.destroy();
                }
            });
        } else {
            conn.destroy();
        }
    });

}

debugKaladin();
