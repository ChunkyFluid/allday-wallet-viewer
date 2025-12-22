import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

async function findGhostOwnerInSnowflake(nftIds) {
    let conn;
    try {
        const cfg = {
            account: process.env.SNOWFLAKE_ACCOUNT,
            username: process.env.SNOWFLAKE_USERNAME,
            password: process.env.SNOWFLAKE_PASSWORD,
            warehouse: process.env.SNOWFLAKE_WAREHOUSE,
            database: process.env.SNOWFLAKE_DATABASE,
            schema: process.env.SNOWFLAKE_SCHEMA,
            role: process.env.SNOWFLAKE_ROLE
        };

        conn = snowflake.createConnection(cfg);
        await new Promise((resolve, reject) => {
            conn.connect((err) => err ? reject(err) : resolve());
        });
        console.log("Connected to Snowflake.");

        const idString = nftIds.map(id => `'${id}'`).join(',');
        const sql = `
            SELECT * 
            FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT 
            WHERE NFT_ID IN (${idString})
        `;

        const rows = await new Promise((resolve, reject) => {
            conn.execute({
                sqlText: sql,
                complete(err, stmt, rows) {
                    if (err) return reject(err);
                    resolve(rows);
                }
            });
        });

        console.log("Snowflake Holdings for these IDs:");
        console.log(rows);

    } catch (err) {
        console.error("Snowflake check failed:", err);
    } finally {
        if (conn) {
            conn.destroy(() => { });
        }
        process.exit(0);
    }
}

findGhostOwnerInSnowflake(['6063904', '6049871']);
