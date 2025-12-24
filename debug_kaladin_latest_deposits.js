import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';
import dotenv from 'dotenv';
dotenv.config();

async function findTopMoments() {
    console.log("🕵️‍♂️ Finding 'Top 4' Moments for Kaladin49...");
    const wallet = '0x93914b2bfb28d59d';

    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    connection.connect(async (err, conn) => {
        if (err) { console.error('SF Connect Error'); return; }

        // 1. Get last 10 deposits
        console.log('\n--- 1. Fetching Last 10 Deposits from Snowflake ---');
        const depositSql = `
            SELECT EVENT_DATA:id::STRING as NFT_ID, BLOCK_TIMESTAMP
            FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
            WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay' 
              AND EVENT_TYPE = 'Deposit' 
              AND LOWER(EVENT_DATA:to::STRING) = '${wallet}'
            ORDER BY BLOCK_TIMESTAMP DESC
            LIMIT 10
        `;

        conn.execute({
            sqlText: depositSql,
            complete: async (err, stmt, rows) => {
                if (err) { console.error(err); return; }
                const deposits = rows;
                console.table(deposits);

                const nftIds = deposits.map(r => r.NFT_ID);
                if (nftIds.length === 0) { conn.destroy(); return; }

                // 2. For these IDs, find Serial Number from Mint Event
                console.log('\n--- 2. Fetching Serial Numbers (Mint Events) ---');
                // Note: Values in IN clause must be quoted
                const idList = nftIds.map(id => `'${id}'`).join(',');
                const mintSql = `
                    SELECT EVENT_DATA:id::STRING as NFT_ID, EVENT_DATA:serialNumber::STRING as SERIAL
                    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
                    WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
                      AND EVENT_TYPE = 'MomentNFTMinted'
                      AND EVENT_DATA:id::STRING IN (${idList})
                `;

                conn.execute({
                    sqlText: mintSql,
                    complete: async (errMint, stmtMint, rowsMint) => {
                        if (errMint) console.error(errMint);
                        const serialMap = {};
                        rowsMint.forEach(r => serialMap[r.NFT_ID] = r.SERIAL);

                        // 3. Check Postgres Status
                        const pgRes = await pgQuery(`
                            SELECT h.nft_id, m.first_name, m.last_name, m.tier, m.serial_number 
                            FROM holdings h
                            LEFT JOIN nft_core_metadata_v2 m ON h.nft_id = m.nft_id
                            WHERE h.nft_id = ANY($1)
                        `, [nftIds]);
                        const pgMap = {};
                        pgRes.rows.forEach(r => pgMap[r.nft_id] = r);

                        console.log('\n--- ANALYSIS ---');
                        console.log('Target Serials: Josh Allen #82, Drake Maye #4, Trevor #7092, Zack #3229');
                        console.log('NOTE: If serial is missing via Mint event, it might be in Dim table, but this is a good start.');

                        deposits.forEach(d => {
                            const serial = serialMap[d.NFT_ID] || 'Unknown';
                            const pg = pgMap[d.NFT_ID];
                            const name = pg ? `${pg.first_name} ${pg.last_name}` : 'Unknown';
                            const pgStatus = pg ? `In DB (${name}, #${pg.serial_number}, ${pg.tier})` : 'MISSING from DB';

                            console.log(`NFT ${d.NFT_ID} (Date: ${d.BLOCK_TIMESTAMP})`);
                            console.log(`   -> Serial: ${serial}`);
                            console.log(`   -> Status: ${pgStatus}`);

                            // Highlight matches
                            if (serial === '82') console.log('   MATCH: Josh Allen #82 Candidate!');
                            if (serial === '4') console.log('   MATCH: Drake Maye #4 Candidate!');
                            if (serial === '7092') console.log('   MATCH: Trevor Lawrence #7092 Candidate!');
                            if (serial === '3229') console.log('   MATCH: Zack Baun #3229 Candidate!');
                            console.log('');
                        });

                        conn.destroy();
                    }
                });
            }
        });
    });
}

findTopMoments();
