const snowflake = require('snowflake-sdk');
require('dotenv').config({ path: '.env' });
const fs = require('fs');

const cfg = {
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    role: process.env.SNOWFLAKE_ROLE
};

function exec(conn, sql) {
    return new Promise((resolve, reject) => {
        conn.execute({
            sqlText: sql, complete(err, stmt, rows) {
                if (err) return reject(err);
                resolve(rows);
            }
        });
    });
}

(async () => {
    const conn = snowflake.createConnection(cfg);
    await new Promise((resolve, reject) => {
        conn.connect((err) => err ? reject(err) : resolve());
    });

    console.log('Searching for "unknown" in Snowflake...');
    const sql = `
    SELECT COUNT(*) AS c
    FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
    WHERE LOWER(FIRST_NAME) = 'unknown' 
       OR LOWER(LAST_NAME) = 'unknown'
  `;

    const unknownCount = await exec(conn, sql);
    console.log("Snowflake records with 'unknown': " + unknownCount[0].C);

    // Check how many have NULL or EMPTY
    const sql2 = `
    SELECT COUNT(*) AS c
    FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
    WHERE FIRST_NAME IS NULL OR FIRST_NAME = ''
       OR LAST_NAME IS NULL OR LAST_NAME = ''
  `;
    const nullCount = await exec(conn, sql2);
    console.log("Snowflake records with NULL/EMPTY: " + nullCount[0].C);

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
