/**
 * Find holdings missing acquired dates
 */
import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config();

const { Pool } = pg;
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    console.log('\nScanning for holdings missing acquired dates...\n');

    const result = await pool.query(`
    SELECT wallet_address, nft_id 
    FROM wallet_holdings 
    WHERE last_event_ts IS NULL
    LIMIT 30000
  `);

    console.log(`Found ${result.rows.length} total records missing dates.`);

    if (result.rows.length > 0) {
        // Group by wallet to see which accounts are most affected
        const byWallet = {};
        result.rows.forEach(r => {
            byWallet[r.wallet_address] = (byWallet[r.wallet_address] || 0) + 1;
        });

        const sortedWallets = Object.entries(byWallet)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 10);

        console.log('\nTop 10 wallets missing dates:');
        sortedWallets.forEach(([wallet, count]) => {
            console.log(`  - ${wallet}: ${count} moments`);
        });

        // Save the NFT IDs to a file so we can use them in a Snowflake query
        const nftIds = result.rows.map(r => `'${r.nft_id}'`).join(',');
        fs.writeFileSync('missing_nft_ids.txt', nftIds);
        console.log('\nSaved missing NFT IDs to missing_nft_ids.txt');

        console.log('\n--- RECOMMENDATION ---');
        console.log('To fix these, run this query in Snowflake to get their original Deposit dates:');
        console.log(`
      SELECT 
        LOWER(EVENT_DATA:to::STRING) as WALLET_ADDRESS,
        EVENT_DATA:id::STRING as NFT_ID,
        MIN(BLOCK_TIMESTAMP) as ACQUIRED_AT
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
        AND EVENT_TYPE = 'Deposit'
        AND TX_SUCCEEDED = true
        AND NFT_ID IN (SELECT column1 FROM VALUES ${result.rows.slice(0, 1000).map(r => `('${r.nft_id}')`).join(',')})
      GROUP BY 1, 2;
    `);
        console.log('(Note: The above sample query only covers the first 1000 items due to SQL length limits)');
    }

    await pool.end();
}

main().catch(console.error);
