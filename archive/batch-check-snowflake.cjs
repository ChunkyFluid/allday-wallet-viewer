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
    const sample = JSON.parse(fs.readFileSync('null_names_sample.json', 'utf8'));
    const ids = sample.map(s => s.nft_id);

    const conn = snowflake.createConnection(cfg);
    await new Promise((resolve, reject) => {
        conn.connect((err) => err ? reject(err) : resolve());
    });

    console.log(`Checking ${ids.length} IDs in Snowflake...`);
    const sql = `
    SELECT NFT_ID, FIRST_NAME, LAST_NAME
    FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
    WHERE NFT_ID IN (${ids.map(id => "'" + id + "'").join(',')})
  `;

    const rows = await exec(conn, sql);

    // Find how many have actual names in Snowflake but not in local DB
    const withNames = rows.filter(r => r.FIRST_NAME && r.FIRST_NAME !== '');
    console.log(`Snowflake has names for ${withNames.length} out of ${rows.length} checked metadata rows.`);

    fs.writeFileSync('snowflake_batch_check.json', JSON.stringify(rows, null, 2));

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
