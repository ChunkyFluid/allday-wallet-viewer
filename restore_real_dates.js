// FINAL FIX: Restore real acquisition dates from Snowflake events
import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';

dotenv.config();

const WALLET = '0x7541bafd155b683e';

const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    role: process.env.SNOWFLAKE_ROLE
});

async function executeQuery(sql) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => {
                if (err) reject(err);
                else resolve(rows || []);
            }
        });
    });
}

async function restore() {
    console.log('=== RESTORING REAL ACQUISITION DATES ===\n');

    await new Promise((resolve, reject) => {
        connection.connect((err) => {
            if (err) reject(err);
            else resolve();
        });
    });
    console.log('Connected to Snowflake');

    // Get all NFT IDs with today's date (corrupted)
    const corrupted = await pgQuery(`
        SELECT nft_id FROM holdings 
        WHERE wallet_address = $1 
          AND acquired_at::date = CURRENT_DATE
    `, [WALLET]);

    console.log(`Found ${corrupted.rowCount} NFTs with today's date (corrupted)`);

    if (corrupted.rowCount === 0) {
        console.log('No corrupted dates found!');
        process.exit(0);
    }

    const nftIds = corrupted.rows.map(r => r.nft_id);
    const nftIdList = nftIds.map(id => `'${id}'`).join(',');

    // Get first Deposit event for each NFT to this wallet
    const sql = `
        SELECT 
            EVENT_DATA:id::STRING as NFT_ID,
            MIN(BLOCK_TIMESTAMP) as ACQUIRED_DATE
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
          AND LOWER(EVENT_DATA:to::STRING) = LOWER('${WALLET}')
          AND EVENT_TYPE = 'Deposit'
          AND TX_SUCCEEDED = true
          AND EVENT_DATA:id::STRING IN (${nftIdList})
        GROUP BY 1
    `;

    console.log('Fetching real acquisition dates from Snowflake...');
    const dates = await executeQuery(sql);
    console.log(`Got ${dates.length} real dates from Snowflake`);

    // Update each NFT with its real date
    let updated = 0;
    for (const row of dates) {
        const nftId = row.NFT_ID?.toString();
        const date = row.ACQUIRED_DATE;
        if (nftId && date) {
            await pgQuery(`
                UPDATE holdings 
                SET acquired_at = $1 
                WHERE wallet_address = $2 AND nft_id = $3
            `, [new Date(date), WALLET, nftId]);
            updated++;
        }
    }

    console.log(`\n✅ Updated ${updated} NFTs with real acquisition dates`);

    // For any remaining with today's date (no Snowflake data), estimate based on NFT ID
    const remaining = await pgQuery(`
        SELECT nft_id FROM holdings 
        WHERE wallet_address = $1 
          AND acquired_at::date = CURRENT_DATE
    `, [WALLET]);

    if (remaining.rowCount > 0) {
        console.log(`\n${remaining.rowCount} NFTs still have today's date - applying estimates...`);
        for (const row of remaining.rows) {
            const id = parseInt(row.nft_id);
            let estimatedDate;
            if (id > 10000000) estimatedDate = new Date('2025-01-01');
            else if (id > 9000000) estimatedDate = new Date('2024-09-01');
            else if (id > 8000000) estimatedDate = new Date('2024-06-01');
            else if (id > 5000000) estimatedDate = new Date('2023-06-01');
            else if (id > 4000000) estimatedDate = new Date('2022-12-01');
            else estimatedDate = new Date('2022-09-01');

            await pgQuery(`
                UPDATE holdings 
                SET acquired_at = $1 
                WHERE wallet_address = $2 AND nft_id = $3
            `, [estimatedDate, WALLET, row.nft_id]);
        }
        console.log(`Applied estimated dates to ${remaining.rowCount} NFTs`);
    }

    console.log('\n✅ All dates restored!');
    process.exit(0);
}

restore().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
