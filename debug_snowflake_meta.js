import snowflake from 'snowflake-sdk';
import dotenv from 'dotenv';
dotenv.config();

async function checkSnowflakeMeta() {
    const nftId = '10576026';
    console.log(`Checking Snowflake metadata for NFT ${nftId}...`);

    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    connection.connect((err, conn) => {
        if (err) { console.error('SF Connect Error'); return; }

        // Try to find it in DIM_ALLDAY_NFT_METADATA (or similar?)
        // Actually, we usually sync from a specific table. 
        // Let's check FLOW_ONCHAIN_CORE_DATA.CORE.DIM_ALLDAY_METADATA or sim.
        // Wait, I don't know the exact source table name for metadata in SF.
        // I'll check `scripts/sync_leaderboards.js` or `sync_metadata.js` (if exists) -> No.
        // I recall `nft_core_metadata_v2` is the PG table.
        // Let's try guessing the SF view: `FLOW.CORE.DIM_ALLDAY_NFT_METADATA`?
        // Or just query the `EVENT_DATA` from the `MomentMinted` event!

        const sql = `
            SELECT EVENT_DATA 
            FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
            WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
              AND EVENT_TYPE = 'MomentNFTMinted'
              AND EVENT_DATA:id::STRING = '${nftId}'
        `;

        conn.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => {
                if (err) console.error(err);
                else {
                    if (rows.length > 0) {
                        console.log('Found MomentMinted event! Data:');
                        console.log(JSON.stringify(rows[0].EVENT_DATA, null, 2));
                        // The Minted event usually has editionID and serialNumber.
                        // But maybe not full player metadata (that's on the Edition).
                    } else {
                        console.log('No MomentMinted event found in Snowflake yet.');
                    }
                }
                conn.destroy();
            }
        });
    });
}

checkSnowflakeMeta();
