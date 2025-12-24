import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function auditJunglerules() {
    console.log(`Auditing Junglerules Unlocked Ghosts...`);

    // 1. Fetch our local unlocked IDs
    const localRes = await pgQuery(`
    SELECT nft_id FROM wallet_holdings 
    WHERE wallet_address = $1 AND is_locked = false
  `, [ADDRESS]);
    const localIds = new Set(localRes.rows.map(r => r.nft_id));
    console.log(`Local Unlocked: ${localIds.size}`);

    // 2. Fetch Snowflake active IDs
    const cfg = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: 'ALLDAY',
        role: process.env.SNOWFLAKE_ROLE
    };

    const conn = snowflake.createConnection(cfg);
    await new Promise((resolve, reject) => {
        conn.connect((err) => err ? reject(err) : resolve());
    });

    const sfRows = await new Promise((resolve, reject) => {
        conn.execute({
            sqlText: `SELECT NFT_ID FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT WHERE WALLET_ADDRESS = '${ADDRESS}'`,
            complete(err, stmt, rows) {
                if (err) return reject(err);
                resolve(rows);
            }
        });
    });
    const sfIds = new Set(sfRows.map(r => r.NFT_ID));
    console.log(`Snowflake Unlocked: ${sfIds.size}`);

    const suspects = [...localIds].filter(id => !sfIds.has(id));
    console.log(`Ghost Suspects: ${suspects.length}`);
    if (suspects.length > 0) {
        console.log(`Sample suspect: ${suspects[0]}`);
        console.log(`SUSPECT_IDS: ${JSON.stringify(suspects)}`);
    }

    conn.destroy(() => { });
    process.exit(0);
}

auditJunglerules();
