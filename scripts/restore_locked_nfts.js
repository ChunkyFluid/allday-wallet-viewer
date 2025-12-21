/**
 * Restore locked NFT status from Snowflake event data
 * 
 * This script queries NFTLocked and NFTUnlocked events from Snowflake
 * and updates the wallet_holdings table with the correct is_locked status.
 */

import snowflake from 'snowflake-sdk';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

// PostgreSQL connection
const pgPool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Create Snowflake connection
function createSnowflakeConnection() {
    return new Promise((resolve, reject) => {
        const conn = snowflake.createConnection({
            account: process.env.SNOWFLAKE_ACCOUNT,
            username: process.env.SNOWFLAKE_USERNAME,
            password: process.env.SNOWFLAKE_PASSWORD,
            database: 'FLOW_ONCHAIN_CORE_DATA',
            schema: 'CORE',
            warehouse: process.env.SNOWFLAKE_WAREHOUSE
        });

        conn.connect((err) => {
            if (err) reject(err);
            else resolve(conn);
        });
    });
}

// Execute Snowflake query
function executeQuery(conn, sql) {
    return new Promise((resolve, reject) => {
        conn.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => {
                if (err) reject(err);
                else resolve(rows);
            }
        });
    });
}

async function main() {
    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  RESTORE LOCKED NFT STATUS FROM SNOWFLAKE EVENTS');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');

    // Connect to Snowflake
    console.log('[Restore] Connecting to Snowflake...');
    const sfConn = await createSnowflakeConnection();
    console.log('[Restore] ✅ Connected to Snowflake');

    try {
        // Query to get all currently locked NFTs (AllDay NFTs only)
        // This mirrors the logic in NFLAllDayWalletGrab.sql
        const sql = `
      WITH locked_events AS (
        SELECT 
          EVENT_DATA:id::STRING as NFT_ID, 
          EVENT_DATA:to::STRING as WALLET_ADDRESS,
          BLOCK_HEIGHT,
          BLOCK_TIMESTAMP
        FROM 
          FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE 
          EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
          EVENT_TYPE = 'NFTLocked' AND
          TX_SUCCEEDED = true AND
          BLOCK_TIMESTAMP >= '2021-01-01'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
      ),
      
      unlocked_events AS (
        SELECT 
          EVENT_DATA:id::STRING as NFT_ID,
          BLOCK_HEIGHT,
          BLOCK_TIMESTAMP
        FROM 
          FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE 
          EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND
          EVENT_TYPE = 'NFTUnlocked' AND
          TX_SUCCEEDED = true AND
          BLOCK_TIMESTAMP >= '2021-01-01'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
      ),
      
      burned_nfts AS (
        SELECT
          EVENT_DATA:id::STRING as NFT_ID
        FROM 
          FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE 
          EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND
          EVENT_TYPE = 'MomentNFTBurned' AND
          TX_SUCCEEDED = true
      )
      
      SELECT
        l.NFT_ID,
        LOWER(l.WALLET_ADDRESS) as WALLET_ADDRESS,
        l.BLOCK_TIMESTAMP as LOCKED_AT
      FROM
        locked_events l
        LEFT JOIN unlocked_events u ON l.NFT_ID = u.NFT_ID AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
      WHERE
        u.NFT_ID IS NULL AND
        l.NFT_ID NOT IN (SELECT NFT_ID FROM burned_nfts)
      ORDER BY l.NFT_ID
    `;

        console.log('[Restore] Querying locked NFTs from Snowflake...');
        console.log('[Restore] This may take a minute...');
        const startTime = Date.now();

        const lockedNFTs = await executeQuery(sfConn, sql);

        const queryTime = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`[Restore] ✅ Found ${lockedNFTs.length} locked NFTs in ${queryTime}s`);

        if (lockedNFTs.length === 0) {
            console.log('[Restore] No locked NFTs found in Snowflake');
            return;
        }

        // Show sample
        console.log('\n[Restore] Sample locked NFTs:');
        lockedNFTs.slice(0, 5).forEach(r => {
            console.log(`  NFT ${r.NFT_ID} locked by ${r.WALLET_ADDRESS}`);
        });

        // Group by wallet
        const byWallet = new Map();
        for (const row of lockedNFTs) {
            const wallet = row.WALLET_ADDRESS;
            if (!byWallet.has(wallet)) {
                byWallet.set(wallet, []);
            }
            byWallet.get(wallet).push(row.NFT_ID);
        }

        console.log(`\n[Restore] Locked NFTs span ${byWallet.size} wallets`);

        // Update PostgreSQL in batches
        console.log('\n[Restore] Updating wallet_holdings in PostgreSQL...');

        let totalUpdated = 0;
        const BATCH_SIZE = 1000;
        const nftIds = lockedNFTs.map(r => r.NFT_ID);

        for (let i = 0; i < nftIds.length; i += BATCH_SIZE) {
            const batch = nftIds.slice(i, i + BATCH_SIZE);

            const result = await pgPool.query(
                `UPDATE wallet_holdings 
         SET is_locked = TRUE 
         WHERE nft_id = ANY($1::text[]) AND is_locked = FALSE`,
                [batch]
            );

            totalUpdated += result.rowCount;

            if ((i + BATCH_SIZE) % 10000 === 0 || i + BATCH_SIZE >= nftIds.length) {
                console.log(`[Restore] Progress: ${Math.min(i + BATCH_SIZE, nftIds.length)}/${nftIds.length} NFTs processed`);
            }
        }

        console.log(`\n[Restore] ✅ Updated ${totalUpdated} holdings to is_locked=TRUE`);

        // Verify the update
        const verifyResult = await pgPool.query(
            `SELECT COUNT(*) as locked FROM wallet_holdings WHERE is_locked = TRUE`
        );
        console.log(`[Restore] ✅ Total locked holdings in database: ${verifyResult.rows[0].locked}`);

    } finally {
        sfConn.destroy(() => console.log('[Restore] Snowflake connection closed'));
        await pgPool.end();
    }

    console.log('\n[Restore] ✅ Complete!');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
