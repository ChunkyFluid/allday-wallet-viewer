import { pgQuery } from "../db.js";
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load env from root
dotenv.config({ path: path.join(__dirname, '..', '.env') });

async function backfillMetadata() {
    let conn;
    try {
        // 1. Find NFTs in wallet_holdings that are missing from nft_core_metadata_v2
        console.log("Searching for NFTs with missing metadata...");
        const missingRes = await pgQuery(`
      SELECT DISTINCT h.nft_id 
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON h.nft_id = m.nft_id
      WHERE m.nft_id IS NULL
    `);

        const missingIds = missingRes.rows.map(r => r.nft_id);
        console.log(`Found ${missingIds.length} NFTs missing metadata.`);

        if (missingIds.length === 0) {
            console.log("No missing metadata found. Exiting.");
            return;
        }

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

        // 3. Process in batches
        const BATCH_SIZE = 1000;
        for (let i = 0; i < missingIds.length; i += BATCH_SIZE) {
            const batch = missingIds.slice(i, i + BATCH_SIZE);
            console.log(`Processing batch ${i / BATCH_SIZE + 1} (${batch.length} IDs)...`);

            const idString = batch.map(id => `'${id}'`).join(',');
            const sql = `
        SELECT *
        FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_CORE_NFT_METADATA 
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

            console.log(`Found metadata for ${rows.length} NFTs in Snowflake.`);

            if (rows.length > 0) {
                // Build upsert query
                // We use a multi-row insert for efficiency
                for (const row of rows) {
                    try {
                        // Sanitize integer fields
                        const sanitizeInt = (val) => {
                            if (val === null || val === undefined || val === '' || val === 'null') return null;
                            const parsed = parseInt(val, 10);
                            return isNaN(parsed) ? null : parsed;
                        };

                        await pgQuery(`
              INSERT INTO nft_core_metadata_v2 (
                nft_id, edition_id, play_id, series_id, set_id, tier,
                serial_number, max_mint_size, first_name, last_name,
                team_name, position, jersey_number, series_name, set_name
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
              ON CONFLICT (nft_id) DO UPDATE SET
                edition_id = EXCLUDED.edition_id,
                play_id = EXCLUDED.play_id,
                series_id = EXCLUDED.series_id,
                set_id = EXCLUDED.set_id,
                tier = EXCLUDED.tier,
                serial_number = EXCLUDED.serial_number,
                max_mint_size = EXCLUDED.max_mint_size,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                team_name = EXCLUDED.team_name,
                position = EXCLUDED.position,
                jersey_number = EXCLUDED.jersey_number,
                series_name = EXCLUDED.series_name,
                set_name = EXCLUDED.set_name
            `, [
                            row.NFT_ID,
                            row.EDITION_ID,
                            row.PLAY_ID,
                            row.SERIES_ID,
                            row.SET_ID,
                            row.TIER,
                            sanitizeInt(row.SERIAL_NUMBER),
                            sanitizeInt(row.MAX_MINT_SIZE),
                            row.FIRST_NAME,
                            row.LAST_NAME,
                            row.TEAM_NAME,
                            row.POSITION,
                            sanitizeInt(row.JERSEY_NUMBER),
                            row.SERIES_NAME,
                            row.SET_NAME
                        ]);
                    } catch (insertErr) {
                        console.error(`Error inserting NFT ${row.NFT_ID}:`, insertErr.message);
                    }
                }
            }
        }

        console.log("Backfill complete.");

    } catch (err) {
        console.error("Backfill failed:", err);
    } finally {
        if (conn) {
            conn.destroy(() => { });
        }
        process.exit(0);
    }
}

backfillMetadata();
