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

    const ids = ['9443518', '9443519', '938885', '938886'];
    const sql = `
    SELECT NFT_ID, FIRST_NAME, LAST_NAME, TEAM_NAME, SET_NAME 
    FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
    WHERE NFT_ID IN (${ids.map(id => "'" + id + "'").join(',')})
  `;

    const rows = await exec(conn, sql);
    let output = JSON.stringify(rows, null, 2);
    fs.writeFileSync("snowflake_check_results.txt", output);

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
