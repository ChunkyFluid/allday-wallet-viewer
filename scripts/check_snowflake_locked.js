import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';

dotenv.config();

const conn = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE
});

conn.connect((err) => {
    if (err) {
        console.error('Connection error:', err.message);
        process.exit(1);
    }

    console.log('Connected to Snowflake, checking your wallet...');

    // Check the specific wallet
    conn.execute({
        sqlText: `SELECT WALLET_ADDRESS, NFT_ID, IS_LOCKED 
              FROM ALLDAY_WALLET_HOLDINGS_CURRENT 
              WHERE LOWER(WALLET_ADDRESS) = '0x7541bafd155b683e'
              LIMIT 20`,
        complete: (err, stmt, rows) => {
            if (err) {
                console.error('Query error:', err.message);
                conn.destroy(() => process.exit(1));
                return;
            }

            console.log(`Found ${rows.length} rows for your wallet:`);
            const locked = rows.filter(r => r.IS_LOCKED === true);
            const unlocked = rows.filter(r => r.IS_LOCKED !== true);
            console.log(`  Locked: ${locked.length}`);
            console.log(`  Unlocked: ${unlocked.length}`);

            if (rows.length > 0) {
                console.log('\nSample:');
                rows.slice(0, 5).forEach(r => {
                    console.log(`  NFT ${r.NFT_ID} - locked: ${r.IS_LOCKED}`);
                });
            }

            conn.destroy(() => process.exit(0));
        }
    });
});
