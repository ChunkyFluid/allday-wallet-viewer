import { pgQuery } from "./db.js";
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

async function fixSpecificNft(nftId) {
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

        const conn = snowflake.createConnection(cfg);
        await new Promise((resolve, reject) => {
            conn.connect((err) => err ? reject(err) : resolve());
        });

        console.log(`Fetching metadata for ${nftId} from Snowflake...`);
        const sql = `
      SELECT *
      FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
      WHERE NFT_ID = '${nftId}'
      LIMIT 1
    `;

        const rows = await new Promise((resolve, reject) => {
            conn.execute({
                sqlText: sql, complete(err, stmt, rows) {
                    if (err) return reject(err);
                    resolve(rows);
                }
            });
        });

        if (rows && rows.length > 0) {
            const row = rows[0];
            console.log(`Found: ${row.FIRST_NAME} ${row.LAST_NAME}`);

            await pgQuery(`
        INSERT INTO nft_core_metadata_v2 (
          nft_id, edition_id, play_id, series_id, set_id, tier,
          serial_number, max_mint_size, first_name, last_name,
          team_name, position, jersey_number, series_name, set_name
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (nft_id) DO UPDATE SET
          edition_id = EXCLUDED.edition_id,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          team_name = EXCLUDED.team_name,
          set_name = EXCLUDED.set_name
      `, [
                nftId,
                row.EDITION_ID,
                row.PLAY_ID,
                row.SERIES_ID,
                row.SET_ID,
                row.TIER,
                row.SERIAL_NUMBER,
                row.MAX_MINT_SIZE,
                row.FIRST_NAME,
                row.LAST_NAME,
                row.TEAM_NAME,
                row.POSITION,
                row.JERSEY_NUMBER,
                row.SERIES_NAME,
                row.SET_NAME
            ]);
            console.log(`✅ Fixed NFT ${nftId}`);
        } else {
            console.log(`❌ Not found in Snowflake`);
        }

        conn.destroy(() => { });
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

fixSpecificNft('10519438');
