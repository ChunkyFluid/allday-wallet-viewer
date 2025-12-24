import { createSnowflakeConnection, executeSnowflakeWithRetry } from "./scripts/snowflake-utils.js";
import { pgQuery } from "./db.js";
import * as dotenv from 'dotenv';
dotenv.config();

const WALLET = '0xcfd9bad75352b43b';

async function checkGhostEventsSmart() {
    console.log(`Checking Locked Event History for ${WALLET} (Smart Mode - Split queries)...`);
    try {
        const dbRes = await pgQuery(
            `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
            [WALLET.toLowerCase()]
        );
        const localLockedIds = dbRes.rows.map(r => r.nft_id);
        console.log(`Local DB has ${localLockedIds.length} locked moments.`);

        if (localLockedIds.length === 0) process.exit(0);

        const conn = await createSnowflakeConnection();
        const BATCH = 500;
        let potentialGhosts = [];

        for (let i = 0; i < localLockedIds.length; i += BATCH) {
            const batchIds = localLockedIds.slice(i, i + BATCH);
            const idList = batchIds.map(id => `'${id}'`).join(',');

            console.log(`Checking batch ${i} to ${i + batchIds.length}...`);

            // 1. Check Unlocked
            console.log("  > Checking Unlocked...");
            const unlockSql = `
                SELECT EVENT_DATA:id::STRING as NFT_ID, MAX(BLOCK_TIMESTAMP) as EVENT_DATE
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND EVENT_TYPE = 'NFTUnlocked'
                AND EVENT_DATA:id::STRING IN (${idList}) AND TX_SUCCEEDED = true GROUP BY 1
            `;
            const unlockRows = await executeSnowflakeWithRetry(conn, unlockSql, { maxRetries: 1 });
            unlockRows.forEach(r => potentialGhosts.push({ id: r.NFT_ID, type: 'NFTUnlocked', date: r.EVENT_DATE }));

            // 2. Check Withdraw from Locker
            console.log("  > Checking Withdraw...");
            const withdrawSql = `
                SELECT EVENT_DATA:id::STRING as NFT_ID, MAX(BLOCK_TIMESTAMP) as EVENT_DATE
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' AND EVENT_TYPE = 'Withdraw'
                AND EVENT_DATA:id::STRING IN (${idList}) AND TX_SUCCEEDED = true GROUP BY 1
            `;
            const withdrawRows = await executeSnowflakeWithRetry(conn, withdrawSql, { maxRetries: 1 });
            withdrawRows.forEach(r => potentialGhosts.push({ id: r.NFT_ID, type: 'Withdraw', date: r.EVENT_DATE }));

            // 3. Check Burned
            console.log("  > Checking Burned...");
            const burnSql = `
                SELECT EVENT_DATA:id::STRING as NFT_ID, MAX(BLOCK_TIMESTAMP) as EVENT_DATE
                FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
                WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' AND EVENT_TYPE = 'MomentNFTBurned'
                AND EVENT_DATA:id::STRING IN (${idList}) AND TX_SUCCEEDED = true GROUP BY 1
            `;
            const burnRows = await executeSnowflakeWithRetry(conn, burnSql, { maxRetries: 1 });
            burnRows.forEach(r => potentialGhosts.push({ id: r.NFT_ID, type: 'Burned', date: r.EVENT_DATE }));
        }

        console.log(`Found ${potentialGhosts.length} events affecting Locked moments.`);
        if (potentialGhosts.length > 0) {
            console.log("Details:", JSON.stringify(potentialGhosts, null, 2));
        }

    } catch (err) {
        console.error("Error:", err.message);
    }
    process.exit(0);
}

checkGhostEventsSmart();
