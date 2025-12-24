import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';
import dotenv from 'dotenv';

dotenv.config();

async function globalDateFix() {
    console.log(`\n=== 🌍 Global Acquisition Date Repair (Since Dec 22, 2025) ===\n`);

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

        // Query ALL Deposit events since the outage started
        const sql = `
            SELECT 
                EVENT_DATA:id::STRING as NFT_ID, 
                LOWER(EVENT_DATA:to::STRING) as WALLET_ADDRESS,
                BLOCK_TIMESTAMP as ACQUIRED_AT
            FROM 
                FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
            WHERE 
                EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
                EVENT_TYPE = 'Deposit' AND
                TX_SUCCEEDED = true AND
                BLOCK_TIMESTAMP >= '2025-12-22T00:00:00Z'
            ORDER BY BLOCK_TIMESTAMP ASC;
        `;

        console.log('Running Snowflake query for recent deposits (Dec 22+)...');

        const results = await new Promise((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                complete: (err, stmt, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            });
        });

        console.log(`\nFound ${results.length} recent deposit events.`);

        let updated = 0;
        let batchCount = 0;
        const total = results.length;

        // Process in chunks to be nice to DB
        // Since we are updating specific rows, we can just loop
        for (const row of results) {
            const nftId = row.NFT_ID;
            const walletAddress = row.WALLET_ADDRESS;
            const acquiredAt = row.ACQUIRED_AT;

            // Update holdings if this wallet currently holds this NFT
            // We use 'acquired_at > ...' check or just overwrite? 
            // Just overwrite to be safe, assuming the event timestamp is the truth.
            // But wait, what if the user had it, sold it, bought it back?
            // The query returns all deposits ordered by time.
            // So if bought back later, the later timestamp will overwrite the earlier one (correct).

            const result = await pgQuery(`
                UPDATE holdings
                SET acquired_at = $1
                WHERE wallet_address = $2 AND nft_id = $3
            `, [acquiredAt, walletAddress, nftId]);

            // Also update legacy table just in case
            await pgQuery(`
                UPDATE wallet_holdings
                SET last_event_ts = $1
                WHERE wallet_address = $2 AND nft_id = $3
            `, [acquiredAt, walletAddress, nftId]);

            if (result.rowCount > 0) {
                updated++;
            }

            batchCount++;
            if (batchCount % 500 === 0) {
                process.stdout.write(`\rProgress: ${batchCount}/${total} events processed. Updated: ${updated}`);
            }
        }

        console.log(`\n\n✅ Repair complete!`);
        console.log(`   - Processed Events: ${total}`);
        console.log(`   - Holdings Updated: ${updated}`);
        console.log(`   - (Reason for difference: Many events are for NFTs that were subsequently sold/moved)`);

    } catch (err) {
        console.error('❌ Error during global repair:', err.message);
    } finally {
        connection.destroy();
        process.exit();
    }
}

globalDateFix();
