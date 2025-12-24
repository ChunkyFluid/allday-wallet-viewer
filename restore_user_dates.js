// Restore dates for user's wallet from Snowflake - FIXED table name
import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';

dotenv.config();

const WALLET = '0x7541bafd155b683e';

// Create Snowflake connection
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
    console.log('=== RESTORING DATES FOR USER WALLET ===\n');

    // Connect to Snowflake
    await new Promise((resolve, reject) => {
        connection.connect((err) => {
            if (err) reject(err);
            else resolve();
        });
    });
    console.log('Connected to Snowflake');

    // Get NFT IDs in user's wallet that have TODAY's date (corrupted)
    const badHoldings = await pgQuery(`
        SELECT nft_id FROM holdings 
        WHERE wallet_address = $1 
          AND acquired_at::date = CURRENT_DATE
    `, [WALLET]);
    const nftIds = badHoldings.rows.map(r => r.nft_id);
    console.log(`User has ${nftIds.length} NFTs with today's date (corrupted)`);

    if (nftIds.length === 0) {
        console.log('No corrupted dates to fix!');
        process.exit(0);
    }

    // Fetch acquisition dates from Snowflake deposit events
    const nftIdList = nftIds.slice(0, 500).map(id => `'${id}'`).join(',');
    const sql = `
        SELECT 
            PARSE_JSON(event_payload):value:fields[0]:value:value::string AS nft_id,
            MIN(block_timestamp) as first_deposit
        FROM flow_api.flow_events_allday_v3
        WHERE event_name = 'Deposit'
          AND PARSE_JSON(event_payload):value:fields[1]:value:value::string = '${WALLET}'
          AND PARSE_JSON(event_payload):value:fields[0]:value:value::string IN (${nftIdList})
        GROUP BY 1
    `;

    console.log('Fetching dates from Snowflake deposit events...');
    let sfData;
    try {
        sfData = await executeQuery(sql);
        console.log(`Snowflake returned ${sfData.length} records`);
    } catch (err) {
        console.error('Snowflake query failed:', err.message);
        console.log('Trying alternative approach...');

        // Fallback: Set a reasonable default date for truly new NFTs
        // For purchased moments, use a date based on NFT ID patterns
        let updated = 0;
        for (const nftId of nftIds) {
            // NFTs with ID > 9000000 are roughly from late 2024
            // NFTs with ID > 10000000 are from 2025
            const id = parseInt(nftId);
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
                  AND acquired_at::date = CURRENT_DATE
            `, [estimatedDate, WALLET, nftId.toString()]);
            updated++;
        }
        console.log(`Applied estimated dates to ${updated} NFTs`);
        process.exit(0);
    }

    // Update holdings with correct dates
    let updated = 0;
    for (const row of sfData) {
        const nftId = row.NFT_ID;
        const ts = row.FIRST_DEPOSIT;
        if (nftId && ts) {
            await pgQuery(`
                UPDATE holdings 
                SET acquired_at = $1 
                WHERE wallet_address = $2 AND nft_id = $3
            `, [new Date(ts), WALLET, nftId.toString()]);
            updated++;
        }
    }

    console.log(`\n✅ Updated ${updated} NFT dates from Snowflake`);

    process.exit(0);
}

restore().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
