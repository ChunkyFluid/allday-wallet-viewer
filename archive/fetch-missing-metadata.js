import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function fetchMissingMetadata() {
    const localRes = await pgQuery(`
    SELECT wh.nft_id 
    FROM wallet_holdings wh 
    LEFT JOIN nft_core_metadata_v2 ncm ON ncm.nft_id = wh.nft_id 
    WHERE wh.wallet_address = $1 AND ncm.nft_id IS NULL
  `, [ADDRESS]);

    const ids = localRes.rows.map(r => r.nft_id);
    console.log(`Missing metadata for ${ids.length} IDs: ${ids.join(', ')}`);

    if (ids.length === 0) return;

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

    const idsSql = ids.map(id => `'${id}'`).join(',');
    const sfRows = await new Promise((resolve, reject) => {
        conn.execute({
            sqlText: `SELECT * FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA WHERE NFT_ID IN (${idsSql})`,
            complete(err, stmt, rows) {
                if (err) return reject(err);
                resolve(rows);
            }
        });
    });

    console.log(`Found ${sfRows.length} rows in Snowflake.`);
    if (sfRows.length > 0) {
        console.log(`Sample metadata:`, JSON.stringify(sfRows[0]));
    }

    conn.destroy(() => { });
    process.exit(0);
}

fetchMissingMetadata();
