import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '.env') });

async function findGhostSample() {
    let conn;
    try {
        // 1. Get 1,000 sample NFT IDs from our local DB (faster method)
        console.log("Fetching 1,000 sample NFT IDs from local DB...");
        const countRes = await pgQuery('SELECT count(*) FROM wallet_holdings');
        const count = parseInt(countRes.rows[0].count);
        const randomOffset = Math.max(0, Math.floor(Math.random() * (count - 1000)));
        const localRes = await pgQuery('SELECT nft_id FROM wallet_holdings LIMIT 1000 OFFSET $1', [randomOffset]);
        const localIds = localRes.rows.map(r => r.nft_id);

        // 2. Connect to Snowflake
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

        // 3. Check which of these exist in Snowflake's holdings
        const idString = localIds.map(id => `'${id}'`).join(',');
        const sql = `
            SELECT NFT_ID 
            FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT 
            WHERE NFT_ID IN (${idString})
        `;

        const snowflakeRows = await new Promise((resolve, reject) => {
            conn.execute({
                sqlText: sql,
                complete(err, stmt, rows) {
                    if (err) return reject(err);
                    resolve(rows);
                }
            });
        });

        const snowflakeIds = new Set(snowflakeRows.map(r => r.NFT_ID));

        // 4. Find ghosts
        const ghosts = localIds.filter(id => !snowflakeIds.has(id));

        console.log(`Sample results:`);
        console.log(`- Checked: ${localIds.length}`);
        console.log(`- Found in Snowflake: ${snowflakeIds.size}`);
        console.log(`- Ghosts found: ${ghosts.length}`);

        if (ghosts.length > 0) {
            console.log("First 10 ghosts:", ghosts.slice(0, 10));
        }

    } catch (err) {
        console.error("Ghost sample check failed:", err);
    } finally {
        if (conn) {
            conn.destroy(() => { });
        }
        process.exit(0);
    }
}

findGhostSample();
