const snowflake = require('snowflake-sdk');
require('dotenv').config({ path: '.env' });

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

    console.log('Checking Jacoby Brissett (10519438) in Snowflake...');
    const sql = `
    SELECT *
    FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
    WHERE NFT_ID = '10519438'
  `;

    const rows = await exec(conn, sql);
    console.log("SNOWFLAKE_RESULT:");
    console.log(JSON.stringify(rows, null, 2));

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
