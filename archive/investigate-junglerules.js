import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function investigate() {
    console.log(`Investigating counts for ${ADDRESS}...`);

    // 1. Local Database Check
    console.log('--- LOCAL DB START ---');
    const walletHoldings = await pgQuery(`
    SELECT is_locked, count(*) 
    FROM wallet_holdings 
    WHERE wallet_address = $1 
    GROUP BY is_locked
  `, [ADDRESS]);
    console.log('Local wallet_holdings counts:', JSON.stringify(walletHoldings.rows));

    const holdings = await pgQuery(`
    SELECT count(*) FROM holdings WHERE wallet_address = $1
  `, [ADDRESS]);
    console.log('Local holdings count:', holdings.rows[0].count);
    console.log('--- LOCAL DB END ---');

    // 2. Snowflake Check
    const cfg = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    };

    const conn = snowflake.createConnection(cfg);
    await new Promise((resolve, reject) => {
        conn.connect((err) => err ? reject(err) : resolve());
    });

    const sfRes = await new Promise((resolve, reject) => {
        conn.execute({
            sqlText: `
        SELECT count(*) as count
        FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT
        WHERE WALLET_ADDRESS = '${ADDRESS}'
      `,
            complete(err, stmt, rows) {
                if (err) return reject(err);
                resolve(rows);
            }
        });
    });
    console.log('\nSnowflake Active (Unlocked) count:', sfRes[0].COUNT);

    conn.destroy(() => { });
    process.exit(0);
}

investigate();
