import { pgQuery } from './db.js';
import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function identifyLockedGhostsViaSnowflake() {
    console.log(`Fetching 1131 'locked' IDs from local DB...`);
    const res = await pgQuery(`
        SELECT nft_id FROM wallet_holdings 
        WHERE wallet_address = $1 AND is_locked = true
    `, [ADDRESS]);
    const localLockedIds = res.rows.map(r => r.nft_id);
    console.log(`Found ${localLockedIds.length} locally locked IDs.`);

    if (localLockedIds.length === 0) {
        console.log("No locked IDs to check.");
        process.exit(0);
    }

    // Query Snowflake for current owner of these IDs
    const cfg = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: 'ALLDAY',
        role: process.env.SNOWFLAKE_ROLE
    };

    const conn = snowflake.createConnection(cfg);
    await new Promise((resolve, reject) => {
        conn.connect((err) => err ? reject(err) : resolve());
    });

    console.log("Querying Snowflake for ownership...");
    // We can use ALLDAY_WALLET_HOLDINGS_CURRENT?
    // Wait, that table only has UNLOCKED moments?
    // Is there a table for ALL holdings or Metadata with Owner?
    // User mentioned `ALLDAY_CORE_NFT_METADATA`. Does it have owner?
    // Usually metadata tables don't have current owner.

    // If Snowflake ONLY tracks Unlocked, then checking Snowflake won't help for Locked.
    // UNLESS the moments were sold and are now Unlocked in *someone else's* wallet.
    // In that case, they will appear in `ALLDAY_WALLET_HOLDINGS_CURRENT` with a DIFFERENT wallet address.

    const idsList = localLockedIds.map(id => `'${id}'`).join(',');

    const sfRows = await new Promise((resolve, reject) => {
        conn.execute({
            sqlText: `
                SELECT NFT_ID, WALLET_ADDRESS 
                FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT 
                WHERE NFT_ID IN (${idsList})
            `,
            complete(err, stmt, rows) {
                if (err) return reject(err);
                resolve(rows);
            }
        });
    });

    console.log(`Snowflake returned ${sfRows.length} rows for the checked IDs.`);

    const ghosts = [];
    const movedToUnlocked = [];
    const ghostOwnership = {}; // who owns them now?

    for (const row of sfRows) {
        if (row.WALLET_ADDRESS === ADDRESS) {
            movedToUnlocked.push(row.NFT_ID);
        } else {
            ghosts.push(row.NFT_ID);
            ghostOwnership[row.NFT_ID] = row.WALLET_ADDRESS;
        }
    }

    // IDs that are NOT in Snowflake at all:
    // This implies they are either:
    // 1. Locked (in anyone's wallet) - if Snowflake doesn't track locked.
    // 2. Burned.

    // We assume the 1090 valid locked ones are NOT in Snowflake (as they are locked).
    // So if an ID IS in Snowflake (and not owned by Junglerules), it is DEFINITELY a ghost (sold).
    // If an ID IS in Snowflake (and owned by Junglerules), it is Unlocked (my "9" IDs).

    console.log(`\nAnalysis:`);
    console.log(`- ${movedToUnlocked.length} moments moved to Unlocked (Junglerules).`);
    console.log(`- ${ghosts.length} moments are owned by OTHERS (Confirmed Ghosts).`);

    // But what if the ghost count isn't 41?
    // 1131 - 1090 = 41. (Expected ghosts).
    // Any ID NOT in Snowflake is assumed to remain Locked (Valid).

    if (ghosts.length > 0) {
        console.log(`\nConfirmed Ghosts (Owned by others):`);
        console.log(JSON.stringify(ghosts));
    }

    conn.destroy(() => { });
    process.exit(0);
}

identifyLockedGhostsViaSnowflake();
