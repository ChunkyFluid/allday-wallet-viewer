// Restore locked moments for user's wallet from Snowflake
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
    console.log('=== RESTORING LOCKED MOMENTS ===\n');

    // Connect
    await new Promise((resolve, reject) => {
        connection.connect((err) => {
            if (err) reject(err);
            else resolve();
        });
    });
    console.log('Connected to Snowflake');

    // Get locked NFTs from Snowflake
    const sql = `
        SELECT DISTINCT NFT_ID
        FROM ALLDAY_CORE_LOCKED_NFTS
        WHERE LOWER(OWNER) = '${WALLET}'
    `;

    console.log('Fetching locked NFTs from Snowflake...');
    const lockedNfts = await executeQuery(sql);
    console.log(`Found ${lockedNfts.length} locked NFTs`);

    // Add/update them in holdings
    let added = 0;
    let updated = 0;
    for (const row of lockedNfts) {
        const nftId = row.NFT_ID?.toString();
        if (!nftId) continue;

        // Check if exists
        const existing = await pgQuery(`SELECT is_locked FROM holdings WHERE wallet_address = $1 AND nft_id = $2`, [WALLET, nftId]);

        if (existing.rowCount === 0) {
            // Add new locked holding
            await pgQuery(`
                INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
                VALUES ($1, $2, TRUE, NOW())
            `, [WALLET, nftId]);
            added++;
        } else if (!existing.rows[0].is_locked) {
            // Update to locked
            await pgQuery(`UPDATE holdings SET is_locked = TRUE WHERE wallet_address = $1 AND nft_id = $2`, [WALLET, nftId]);
            updated++;
        }
    }

    console.log(`\n✅ Added ${added} locked NFTs, updated ${updated} to locked status`);

    // Verify
    const finalCount = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1 AND is_locked = TRUE`, [WALLET]);
    console.log(`Total locked NFTs now: ${finalCount.rows[0].c}`);

    process.exit(0);
}

restore().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
