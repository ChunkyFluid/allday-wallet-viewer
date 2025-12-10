require('dotenv').config({ path: '.env' });
const snowflake = require('snowflake-sdk');

const cfg = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE
};

function exec(conn, sql){
  return new Promise((resolve,reject)=>{
    conn.execute({sqlText: sql, complete(err, stmt, rows){
      if(err) return reject(err);
      resolve(rows);
    }});
  });
}

(async() => {
  const conn = snowflake.createConnection(cfg);
  await new Promise((resolve,reject)=>{
    conn.connect((err)=> err?reject(err):resolve());
  });
  console.log('Connected to Snowflake');
  const meta = await exec(conn, 'SELECT COUNT(*) AS c FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA');
  const hold = await exec(conn, 'SELECT COUNT(*) AS c FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT');
  console.log({ metadata_rows: meta[0].C, holdings_rows: hold[0].C });
  conn.destroy(()=>{});
})().catch(err=>{ console.error(err); process.exit(1); });
