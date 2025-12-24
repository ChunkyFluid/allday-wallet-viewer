import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';
import dotenv from 'dotenv';

dotenv.config();

async function restoreAllAcquisitionDates(walletAddress) {
    console.log(`\n=== 🚀 Full Historical Date Restoration from Snowflake for ${walletAddress} ===\n`);

    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    try {
        await new Promise((resolve, reject) => {
            connection.connect((err, conn) => {
                if (err) reject(err);
                else resolve(conn);
            });
        });
        console.log('✅ Connected to Snowflake');

        // Optimized Query: Pull ALL events for this wallet to find the TRUE acquisition date
        const sql = `
            WITH all_wallet_events AS (
                -- Deposits into the wallet (unlocked)
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID, 
                    BLOCK_TIMESTAMP,
                    'DEPOSIT' as EVENT_TYPE
                FROM 
                    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE 
                    EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
                    EVENT_DATA:to::STRING = '${walletAddress.toLowerCase()}' AND
                    EVENT_TYPE = 'Deposit' AND
                    TX_SUCCEEDED = true
                
                UNION ALL

                -- Locking events (locked)
                SELECT 
                    EVENT_DATA:id::STRING as NFT_ID, 
                    BLOCK_TIMESTAMP,
                    'NFTLOCK' as EVENT_TYPE
                FROM 
                    FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE 
                    EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
                    EVENT_DATA:to::STRING = '${walletAddress.toLowerCase()}' AND
                    EVENT_TYPE = 'NFTLocked' AND
                    TX_SUCCEEDED = true
            )
            SELECT 
                NFT_ID, 
                MIN(BLOCK_TIMESTAMP) as ACQUIRED_AT
            FROM all_wallet_events
            GROUP BY NFT_ID;
        `;

        console.log('Running full Snowflake event scan (this may take a minute)...');

        const results = await new Promise((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                complete: (err, stmt, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            });
        });

        console.log(`\nFound ${results.length} NFTs with historical acquisition events.`);

        let updated = 0;
        let batchCount = 0;
        const total = results.length;

        for (const row of results) {
            const nftId = row.NFT_ID;
            const acquiredAt = row.ACQUIRED_AT;

            // Use UPSERT/UPDATE in Postgres
            // We update the holdings table which is our source of truth for acquisition dates
            const result = await pgQuery(`
                UPDATE holdings
                SET acquired_at = $1
                WHERE wallet_address = $2 AND nft_id = $3
            `, [acquiredAt, walletAddress.toLowerCase(), nftId]);

            if (result.rowCount > 0) {
                updated++;
            }

            batchCount++;
            if (batchCount % 100 === 0) {
                console.log(`Progress: ${batchCount}/${total} (${Math.round(batchCount / total * 100)}%)`);
            }
        }

        console.log(`\n✅ Successfully restored ${updated} acquisition dates in Postgres.`);

    } catch (err) {
        console.error('❌ Error during restoration:', err.message);
    } finally {
        connection.destroy();
        process.exit();
    }
}

const wallet = process.argv[2] || '0x7541bafd155b683e';
restoreAllAcquisitionDates(wallet);
