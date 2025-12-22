import snowflake from 'snowflake-sdk';
import { pgQuery } from '../db.js';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const DRY_RUN = process.env.DRY_RUN !== 'false';

async function globalGhostCleanup() {
    let conn;
    try {
        console.log(`Starting Global Ghost Cleanup (DRY_RUN=${DRY_RUN})`);

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

        // 1. Fetch ALL valid NFT IDs from Snowflake via Stream
        console.log("Fetching active/unlocked NFT IDs from Snowflake via stream...");
        const activeIds = new Set();
        let firstRowReceived = false;

        const sql = `SELECT NFT_ID FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT`;

        const statement = conn.execute({ sqlText: sql });
        const stream = statement.streamRows();

        await new Promise((resolve, reject) => {
            stream.on('data', (row) => {
                if (!firstRowReceived) {
                    console.log("First row received! Streaming in progress...");
                    firstRowReceived = true;
                }
                activeIds.add(row.NFT_ID);
                if (activeIds.size % 250000 === 0) {
                    console.log(`Accumulated ${activeIds.size.toLocaleString()} active IDs...`);
                }
            });
            stream.on('error', (err) => {
                console.error("Snowflake stream error:", err);
                reject(err);
            });
            stream.on('end', () => {
                console.log("Snowflake stream ended.");
                resolve();
            });
        });

        console.log(`Finished fetching ${activeIds.size.toLocaleString()} active NFTs from Snowflake.`);

        // 2. Batch check our local wallet_holdings
        console.log("Scanning local database for suspect unlocked ghosts...");
        const BATCH_SIZE = 50000;
        let processed = 0;
        let suspectCount = 0;

        const countRes = await pgQuery('SELECT count(*) FROM wallet_holdings WHERE is_locked = false');
        const totalToProcess = parseInt(countRes.rows[0].count);
        console.log(`Total unlocked records in DB to scan: ${totalToProcess.toLocaleString()}`);

        while (true) {
            const localRes = await pgQuery(`
                SELECT nft_id FROM wallet_holdings 
                WHERE is_locked = false
                AND last_synced_at < NOW() - INTERVAL '4 hours'
                ORDER BY nft_id
                LIMIT ${BATCH_SIZE} OFFSET ${processed}
            `);

            if (localRes.rows.length === 0) break;

            const suspects = localRes.rows
                .map(r => r.nft_id)
                .filter(id => !activeIds.has(id));

            if (suspects.length > 0) {
                suspectCount += suspects.length;

                if (!DRY_RUN) {
                    console.log(`  Deleting ${suspects.length} verified ghosts...`);
                    await pgQuery(`DELETE FROM wallet_holdings WHERE nft_id = ANY($1::text[])`, [suspects]);
                    await pgQuery(`DELETE FROM holdings WHERE nft_id = ANY($1::text[])`, [suspects]);
                } else if (suspectCount <= 10) {
                    console.log(`  [DRY_RUN] Suspect found: ${suspects[0]}`);
                }
            }

            processed += localRes.rows.length;
            if (processed % 100000 === 0 || localRes.rows.length < BATCH_SIZE) {
                console.log(`Progress: Checked ${processed.toLocaleString()} records... Found ${suspectCount.toLocaleString()} suspects so far.`);
            }
            if (localRes.rows.length < BATCH_SIZE) break;
        }

        console.log('\n--- Cleanup Summary ---');
        console.log(`Mode:           ${DRY_RUN ? 'DRY RUN (No changes made)' : 'LIVE CLEANUP'}`);
        console.log(`Total Scanned:  ${processed.toLocaleString()} (All Unlocked)`);
        console.log(`Suspect Ghosts: ${suspectCount.toLocaleString()}`);

    } catch (err) {
        console.error("Global cleanup failed:", err);
    } finally {
        if (conn) {
            conn.destroy(() => { });
        }
        process.exit(0);
    }
}

globalGhostCleanup();
