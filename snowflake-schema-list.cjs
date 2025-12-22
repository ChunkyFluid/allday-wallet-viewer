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

    let output = "SCHEMAS:\n";
    const sql = `SHOW SCHEMAS IN DATABASE ALLDAY_VIEWER`;
    const schemas = await exec(conn, sql);
    output += JSON.stringify(schemas.map(s => s.name), null, 2) + "\n\n";

    fs.writeFileSync("snowflake_schemas_results.txt", output);

    conn.destroy(() => { });
})().catch(err => { console.error(err); process.exit(1); });
