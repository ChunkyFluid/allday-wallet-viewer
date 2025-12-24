import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

async function globalDateRestoreFull() {
    console.log(`\n=== 🦖 GLOBAL HISTORICAL DATE RESTORATION (FULL HISTORY) ===\n`);
    console.log(`Target: Fixing 'all today' dates by checking original deposit times.`);

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

        // Query: Find the LATEST successful deposit event for EVERY NFT.
        // This gives us the current owner and when they got it.
        const sql = `
            SELECT 
                EVENT_DATA:id::STRING as NFT_ID, 
                LOWER(EVENT_DATA:to::STRING) as WALLET_ADDRESS,
                BLOCK_TIMESTAMP as TRUE_ACQUIRED_AT
            FROM 
                FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
            WHERE 
                EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
                EVENT_TYPE = 'Deposit' AND
                TX_SUCCEEDED = true
            QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_TIMESTAMP DESC) = 1
        `;

        console.log('Running massive Snowflake query (fetching 9M+ rows) in STREAMING mode...');
        const startTime = Date.now();

        // EXECUTE WITH STREAMING
        const statement = connection.execute({
            sqlText: sql,
            streamResult: true, // Enable streaming
            complete: function (err, stmt, rows) {
                if (err) {
                    console.error('Snowflake query failed:', err.message);
                }
            }
        });

        const stream = statement.streamRows();

        console.log('\nStarted Database Updates (Streaming)...');
        let updatedCount = 0;
        let processedCount = 0;
        let errorCount = 0;
        let batch = [];
        const BATCH_SIZE = 2000;

        for await (const row of stream) {
            batch.push(row);

            if (batch.length >= BATCH_SIZE) {
                await processBatch(batch);
                batch = [];
            }

            processedCount++;
            if (processedCount % 10000 === 0) {
                process.stdout.write(`\rProgress: ${processedCount} scanned. Repaired: ${updatedCount}`);
            }
        }

        // Process final remaining batch
        if (batch.length > 0) {
            await processBatch(batch);
        }

        const elapsed = (Date.now() - startTime) / 1000;
        console.log(`\n\n✅ RESTORATION COMPLETE in ${elapsed}s!`);
        console.log(`   - Total NFTs Scanned: ${processedCount}`);
        console.log(`   - Holdings Repaired: ${updatedCount}`);
        console.log(`   - Errors: ${errorCount}`);

        // Helper to process a batch
        async function processBatch(batchData) {
            const placeholders = [];
            const params = [];
            let pIdx = 1;

            for (const row of batchData) {
                placeholders.push(`($${pIdx}, $${pIdx + 1}, $${pIdx + 2}::timestamptz)`);
                params.push(row.NFT_ID, row.WALLET_ADDRESS, row.TRUE_ACQUIRED_AT);
                pIdx += 3;
            }

            if (placeholders.length === 0) return;

            const updateSql = `
                UPDATE holdings AS h
                SET acquired_at = v.true_date
                FROM (VALUES ${placeholders.join(',')}) AS v(nft_id, wallet_owner, true_date)
                WHERE h.nft_id = v.nft_id 
                  AND h.wallet_address = v.wallet_owner
                  AND (h.acquired_at >= '2025-12-20'::timestamptz OR h.acquired_at IS NULL) -- Fix recent OR null dates
                  AND v.true_date < '2025-12-20'::timestamptz    -- Sanity check: don't overwrite if true date is also recent
            `;

            try {
                const res = await pgQuery(updateSql, params);
                updatedCount += res.rowCount;
            } catch (err) {
                console.error('Batch error:', err.message);
                errorCount++;
            }
        }

    } catch (err) {
        console.error('❌ Error during full restoration:', err.message);
    } finally {
        if (connection) {
            connection.destroy();
        }
        process.exit();
    }
}

globalDateRestoreFull();
