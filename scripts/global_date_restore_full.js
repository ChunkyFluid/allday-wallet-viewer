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

        console.log('Runing massive Snowflake query (fetching 9M+ rows)... this will take a while...');
        const startTime = Date.now();

        // Using streaming to avoid 9M rows in memory at once? 
        // Snowflake NodeJS driver 'execute' buffers all rows by default.
        // For 9M rows with 3 columns, it might be ~1-2GB RAM. Node can handle it if we have 8GB.

        const results = await new Promise((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                complete: (err, stmt, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            });
        });

        const elapsed = (Date.now() - startTime) / 1000;
        console.log(`\n✅ Snowflake query complete in ${elapsed}s.`);
        console.log(`Found ${results.length} total NFTs with history.`);

        console.log('\nStarted Database Updates...');
        let updatedCount = 0;
        let processedCount = 0;
        let errorCount = 0;
        const total = results.length;

        // We'll process in chunks to essentially pipeline the DB updates
        // But doing one-by-one is too slow for 9M rows.
        // We need a way to bulk update? 
        // Postgres UPDATE ... FROM VALUES is fast.

        // Let's do batches of 1000.
        const BATCH_SIZE = 2000;

        for (let i = 0; i < total; i += BATCH_SIZE) {
            const batch = results.slice(i, i + BATCH_SIZE);
            const values = [];

            // Prepare batch logic
            // We want to update `holdings`
            // But only if current date is recent (suspicious)?
            // Actually, we can just enforce the condition in WHERE:
            // acquired_at >= '2025-12-20' (The start of the bad syncs)

            // Construct a giant generic query or multiple updates?
            // "UPDATE holdings as h SET acquired_at = v.date FROM (VALUES ...) as v(nft_id, wallet, date) WHERE h.nft_id = v.nft_id AND h.wallet_address = v.wallet AND h.acquired_at > '2025-12-20'"

            // Create VALUES string: ($1, $2, $3), ($4, $5, $6)...
            const placeholders = [];
            const params = [];
            let pIdx = 1;

            for (const row of batch) {
                placeholders.push(`($${pIdx}, $${pIdx + 1}, $${pIdx + 2}::timestamptz)`);
                params.push(row.NFT_ID, row.WALLET_ADDRESS, row.TRUE_ACQUIRED_AT);
                pIdx += 3;
            }

            if (placeholders.length === 0) continue;

            const updateSql = `
                UPDATE holdings AS h
                SET acquired_at = v.true_date
                FROM (VALUES ${placeholders.join(',')}) AS v(nft_id, wallet_owner, true_date)
                WHERE h.nft_id = v.nft_id 
                  AND h.wallet_address = v.wallet_owner
                  AND h.acquired_at >= '2025-12-20'::timestamptz -- Only fix recent "bad" dates
                  AND v.true_date < '2025-12-20'::timestamptz    -- Sanity check: don't overwrite if true date is also recent
            `;

            try {
                const res = await pgQuery(updateSql, params);
                updatedCount += res.rowCount;
            } catch (err) {
                console.error('Batch error:', err.message);
                errorCount++;
            }

            processedCount += batch.length;
            if (processedCount % 10000 === 0 || processedCount === total) {
                process.stdout.write(`\rProgress: ${processedCount}/${total} (100% scanned). Repaired: ${updatedCount}`);
            }
        }

        console.log(`\n\n✅ RESTORATION COMPLETE!`);
        console.log(`   - Total NFTs Scanned: ${total}`);
        console.log(`   - Holdings Repaired: ${updatedCount}`);
        console.log(`   - Errors: ${errorCount}`);

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
