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

    let output = "COLUMNS:\n";
    const sql = `DESCRIBE TABLE ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA`;
    const columns = await exec(conn, sql);
    output += JSON.stringify(columns.map(c => ({ name: c.name, type: c.type })), null, 2) + "\n\nTABLES:\n";

    const sql2 = `SHOW TABLES IN ALLDAY_VIEWER.ALLDAY`;
    const tables = await exec(conn, sql2);
    output += JSON.stringify(tables.map(t => t.name), null, 2);

    fs.writeFileSync("snowflake_schema_results.txt", output);

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
