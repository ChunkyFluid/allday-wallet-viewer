// scripts/snowflake_final_dump.js
// One-time script to extract ALL data from Snowflake into normalized PostgreSQL tables
// This creates a complete local copy of all NFL All Day blockchain data

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { executeSnowflakeWithRetry, delay, createSnowflakeConnection } from "./snowflake-utils.js";

dotenv.config();

const BATCH_SIZE = 50000;
const BATCH_DELAY_MS = 500;

// Helper to escape SQL literals
function escapeLiteral(str) {
    if (str == null) return null;
    return String(str).replace(/'/g, "''");
}

// Create normalized tables
async function createNormalizedTables() {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  CREATING NORMALIZED TABLES");
    console.log("══════════════════════════════════════════════════════════════\n");

    // Series table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS series (
      series_id TEXT PRIMARY KEY,
      series_name TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
    console.log("✓ Created/verified series table");

    // Sets table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS sets (
      set_id TEXT PRIMARY KEY,
      set_name TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
    console.log("✓ Created/verified sets table");

    // Plays table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS plays (
      play_id TEXT PRIMARY KEY,
      first_name TEXT,
      last_name TEXT,
      team_name TEXT,
      position TEXT,
      jersey_number INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
    console.log("✓ Created/verified plays table");

    // Editions table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS editions (
      edition_id TEXT PRIMARY KEY,
      play_id TEXT,
      series_id TEXT,
      set_id TEXT,
      tier TEXT,
      max_mint_size INTEGER,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
    console.log("✓ Created/verified editions table");

    // NFTs table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS nfts (
      nft_id TEXT PRIMARY KEY,
      edition_id TEXT,
      serial_number INTEGER,
      minted_at TIMESTAMPTZ,
      burned_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
    console.log("✓ Created/verified nfts table");

    // Holdings table
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS holdings (
      id SERIAL PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      nft_id TEXT NOT NULL,
      is_locked BOOLEAN DEFAULT FALSE,
      acquired_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(wallet_address, nft_id)
    );
  `);
    console.log("✓ Created/verified holdings table");

    // Create indexes
    await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_wallet ON holdings(wallet_address);`);
    await pgQuery(`CREATE INDEX IF NOT EXISTS idx_nfts_edition ON nfts(edition_id);`);
    await pgQuery(`CREATE INDEX IF NOT EXISTS idx_editions_play ON editions(play_id);`);
    await pgQuery(`CREATE INDEX IF NOT EXISTS idx_editions_set ON editions(set_id);`);
    console.log("✓ Created/verified indexes");

    // Create backward-compatibility view
    await pgQuery(`
    CREATE OR REPLACE VIEW nft_core_metadata_v2 AS
    SELECT
      n.nft_id,
      n.edition_id,
      e.play_id,
      e.series_id,
      e.set_id,
      e.tier,
      n.serial_number,
      e.max_mint_size,
      p.first_name,
      p.last_name,
      p.team_name,
      p.position,
      p.jersey_number,
      sr.series_name,
      st.set_name
    FROM nfts n
    LEFT JOIN editions e ON n.edition_id = e.edition_id
    LEFT JOIN plays p ON e.play_id = p.play_id
    LEFT JOIN series sr ON e.series_id = sr.series_id
    LEFT JOIN sets st ON e.set_id = st.set_id;
  `);
    console.log("✓ Created/verified nft_core_metadata_v2 view");
}

// Dump series from Snowflake
async function dumpSeries(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING SERIES");
    console.log("══════════════════════════════════════════════════════════════\n");

    const sql = `
    SELECT DISTINCT
      event_data:id::string AS series_id,
      event_data:name::string AS series_name
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SeriesCreated'
      AND tx_succeeded = TRUE
    ORDER BY series_id;
  `;

    console.log("Querying Snowflake for series...");
    const rows = await executeSnowflakeWithRetry(connection, sql, 3, 5000);
    console.log(`Found ${rows.length} series`);

    if (rows.length === 0) return 0;

    // Insert into PostgreSQL
    const values = rows.map(r =>
        `('${escapeLiteral(r.SERIES_ID)}', '${escapeLiteral(r.SERIES_NAME || '')}')`
    ).join(',\n');

    await pgQuery(`
    INSERT INTO series (series_id, series_name)
    VALUES ${values}
    ON CONFLICT (series_id) DO UPDATE SET
      series_name = EXCLUDED.series_name;
  `);

    console.log(`✓ Inserted/updated ${rows.length} series`);
    return rows.length;
}

// Dump sets from Snowflake
async function dumpSets(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING SETS");
    console.log("══════════════════════════════════════════════════════════════\n");

    const sql = `
    SELECT DISTINCT
      event_data:id::string AS set_id,
      event_data:name::string AS set_name
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'SetCreated'
      AND tx_succeeded = TRUE
    ORDER BY set_id;
  `;

    console.log("Querying Snowflake for sets...");
    const rows = await executeSnowflakeWithRetry(connection, sql, 3, 5000);
    console.log(`Found ${rows.length} sets`);

    if (rows.length === 0) return 0;

    const values = rows.map(r =>
        `('${escapeLiteral(r.SET_ID)}', '${escapeLiteral(r.SET_NAME || '')}')`
    ).join(',\n');

    await pgQuery(`
    INSERT INTO sets (set_id, set_name)
    VALUES ${values}
    ON CONFLICT (set_id) DO UPDATE SET
      set_name = EXCLUDED.set_name;
  `);

    console.log(`✓ Inserted/updated ${rows.length} sets`);
    return rows.length;
}

// Dump plays from Snowflake
async function dumpPlays(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING PLAYS");
    console.log("══════════════════════════════════════════════════════════════\n");

    const sql = `
    SELECT
      event_data:id::STRING AS play_id,
      MAX(CASE WHEN m.value:key.value::STRING = 'playerFirstName' THEN m.value:value.value::STRING END) AS first_name,
      MAX(CASE WHEN m.value:key.value::STRING = 'playerLastName' THEN m.value:value.value::STRING END) AS last_name,
      MAX(CASE WHEN m.value:key.value::STRING = 'teamName' THEN m.value:value.value::STRING END) AS team_name,
      MAX(CASE WHEN m.value:key.value::STRING = 'playerPosition' THEN m.value:value.value::STRING END) AS position,
      MAX(CASE WHEN m.value:key.value::STRING = 'playerNumber' THEN TRY_TO_NUMBER(m.value:value.value::STRING) END) AS jersey_number
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS,
         TABLE(FLATTEN(event_data:metadata)) AS m
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'PlayCreated'
      AND tx_succeeded = TRUE
    GROUP BY event_data:id::STRING
    ORDER BY play_id;
  `;

    console.log("Querying Snowflake for plays...");
    const rows = await executeSnowflakeWithRetry(connection, sql, 3, 30000);
    console.log(`Found ${rows.length} plays`);

    if (rows.length === 0) return 0;

    // Insert in batches
    const batchSize = 1000;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const values = batch.map(r =>
            `('${escapeLiteral(r.PLAY_ID)}', ${r.FIRST_NAME ? `'${escapeLiteral(r.FIRST_NAME)}'` : 'NULL'}, ${r.LAST_NAME ? `'${escapeLiteral(r.LAST_NAME)}'` : 'NULL'}, ${r.TEAM_NAME ? `'${escapeLiteral(r.TEAM_NAME)}'` : 'NULL'}, ${r.POSITION ? `'${escapeLiteral(r.POSITION)}'` : 'NULL'}, ${r.JERSEY_NUMBER != null ? r.JERSEY_NUMBER : 'NULL'})`
        ).join(',\n');

        await pgQuery(`
      INSERT INTO plays (play_id, first_name, last_name, team_name, position, jersey_number)
      VALUES ${values}
      ON CONFLICT (play_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        team_name = EXCLUDED.team_name,
        position = EXCLUDED.position,
        jersey_number = EXCLUDED.jersey_number;
    `);

        inserted += batch.length;
        console.log(`  Progress: ${inserted}/${rows.length} plays`);
    }

    console.log(`✓ Inserted/updated ${rows.length} plays`);
    return rows.length;
}

// Dump editions from Snowflake
async function dumpEditions(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING EDITIONS");
    console.log("══════════════════════════════════════════════════════════════\n");

    const sql = `
    SELECT DISTINCT
      event_data:id::string AS edition_id,
      event_data:playID::string AS play_id,
      event_data:seriesID::string AS series_id,
      event_data:setID::string AS set_id,
      event_data:tier::string AS tier,
      TRY_TO_NUMBER(event_data:maxMintSize::string) AS max_mint_size
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'EditionCreated'
      AND tx_succeeded = TRUE
    ORDER BY edition_id;
  `;

    console.log("Querying Snowflake for editions...");
    const rows = await executeSnowflakeWithRetry(connection, sql, 3, 30000);
    console.log(`Found ${rows.length} editions`);

    if (rows.length === 0) return 0;

    // Insert in batches
    const batchSize = 1000;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const values = batch.map(r =>
            `('${escapeLiteral(r.EDITION_ID)}', '${escapeLiteral(r.PLAY_ID)}', '${escapeLiteral(r.SERIES_ID)}', '${escapeLiteral(r.SET_ID)}', ${r.TIER ? `'${escapeLiteral(r.TIER)}'` : 'NULL'}, ${r.MAX_MINT_SIZE != null ? r.MAX_MINT_SIZE : 'NULL'})`
        ).join(',\n');

        await pgQuery(`
      INSERT INTO editions (edition_id, play_id, series_id, set_id, tier, max_mint_size)
      VALUES ${values}
      ON CONFLICT (edition_id) DO UPDATE SET
        play_id = EXCLUDED.play_id,
        series_id = EXCLUDED.series_id,
        set_id = EXCLUDED.set_id,
        tier = EXCLUDED.tier,
        max_mint_size = EXCLUDED.max_mint_size;
    `);

        inserted += batch.length;
        console.log(`  Progress: ${inserted}/${rows.length} editions`);
    }

    console.log(`✓ Inserted/updated ${rows.length} editions`);
    return rows.length;
}

// Dump NFTs from Snowflake (this is the big one!)
async function dumpNFTs(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING NFTs (this will take a while...)");
    console.log("══════════════════════════════════════════════════════════════\n");

    // First, get total count
    const countSql = `
    SELECT COUNT(*) AS total
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTMinted'
      AND tx_succeeded = TRUE;
  `;

    console.log("Getting total NFT count...");
    const countResult = await executeSnowflakeWithRetry(connection, countSql, 3, 30000);
    const totalNFTs = parseInt(countResult[0]?.TOTAL || 0);
    console.log(`Total NFTs to dump: ${totalNFTs.toLocaleString()}`);

    // Get highest NFT ID already in our database
    const maxIdResult = await pgQuery(`SELECT MAX(nft_id::bigint) as max_id FROM nfts WHERE nft_id ~ '^[0-9]+$';`);
    const startAfterNftId = maxIdResult.rows[0]?.max_id || 0;
    console.log(`Starting after NFT ID: ${startAfterNftId}`);

    let totalInserted = 0;
    let lastNftId = startAfterNftId.toString();
    let hasMore = true;

    while (hasMore) {
        const sql = `
      SELECT
        event_data:id::string AS nft_id,
        event_data:editionID::string AS edition_id,
        TRY_TO_NUMBER(event_data:serialNumber::string) AS serial_number,
        block_timestamp AS minted_at
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
        AND tx_succeeded = TRUE
        AND TRY_TO_NUMBER(event_data:id::string) > ${lastNftId}
      ORDER BY TRY_TO_NUMBER(event_data:id::string)
      LIMIT ${BATCH_SIZE};
    `;

        console.log(`\nFetching batch after NFT ID ${lastNftId}...`);
        const rows = await executeSnowflakeWithRetry(connection, sql, 3, 120000);

        if (rows.length === 0) {
            hasMore = false;
            break;
        }

        // Insert batch into PostgreSQL
        const batchSize = 5000;
        for (let j = 0; j < rows.length; j += batchSize) {
            const batch = rows.slice(j, j + batchSize);
            const values = batch.map(r => {
                const mintedAt = r.MINTED_AT ? `'${new Date(r.MINTED_AT).toISOString()}'` : 'NULL';
                return `('${escapeLiteral(r.NFT_ID)}', '${escapeLiteral(r.EDITION_ID)}', ${r.SERIAL_NUMBER != null ? r.SERIAL_NUMBER : 'NULL'}, ${mintedAt})`;
            }).join(',\n');

            await pgQuery(`
        INSERT INTO nfts (nft_id, edition_id, serial_number, minted_at)
        VALUES ${values}
        ON CONFLICT (nft_id) DO UPDATE SET
          edition_id = EXCLUDED.edition_id,
          serial_number = EXCLUDED.serial_number,
          minted_at = EXCLUDED.minted_at;
      `);
        }

        totalInserted += rows.length;
        lastNftId = rows[rows.length - 1].NFT_ID;

        const pct = ((totalInserted / totalNFTs) * 100).toFixed(1);
        console.log(`  ✓ Inserted batch of ${rows.length} NFTs (Total: ${totalInserted.toLocaleString()} / ${totalNFTs.toLocaleString()} = ${pct}%)`);

        if (rows.length < BATCH_SIZE) {
            hasMore = false;
        }

        await delay(BATCH_DELAY_MS);
    }

    console.log(`\n✓ Finished dumping ${totalInserted.toLocaleString()} NFTs`);
    return totalInserted;
}

// Dump current holdings from Snowflake
async function dumpHoldings(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  DUMPING CURRENT HOLDINGS");
    console.log("══════════════════════════════════════════════════════════════\n");

    const db = process.env.SNOWFLAKE_DATABASE || 'ALLDAY_VIEWER';
    const schema = process.env.SNOWFLAKE_SCHEMA || 'ALLDAY';

    // First check if the holdings table exists in Snowflake
    const checkSql = `
    SELECT COUNT(*) AS cnt
    FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
    LIMIT 1;
  `;

    let useSnowflakeHoldings = true;
    try {
        await executeSnowflakeWithRetry(connection, checkSql, 1, 10000);
    } catch (err) {
        console.log("ALLDAY_WALLET_HOLDINGS_CURRENT table not found, will use wallet_holdings instead");
        useSnowflakeHoldings = false;
    }

    if (useSnowflakeHoldings) {
        // Get count
        const countSql = `SELECT COUNT(*) AS total FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT;`;
        const countResult = await executeSnowflakeWithRetry(connection, countSql, 3, 30000);
        const totalHoldings = parseInt(countResult[0]?.TOTAL || 0);
        console.log(`Total holdings to dump: ${totalHoldings.toLocaleString()}`);

        let totalInserted = 0;
        let offset = 0;

        while (true) {
            const sql = `
        SELECT
          LOWER(wallet_address) AS wallet_address,
          nft_id,
          COALESCE(is_locked, FALSE) AS is_locked,
          last_event_ts AS acquired_at
        FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
        ORDER BY wallet_address, nft_id
        LIMIT ${BATCH_SIZE}
        OFFSET ${offset};
      `;

            console.log(`Fetching holdings batch at offset ${offset}...`);
            const rows = await executeSnowflakeWithRetry(connection, sql, 3, 120000);

            if (rows.length === 0) break;

            // Insert batch
            const values = rows.map(r => {
                const acquiredAt = r.ACQUIRED_AT ? `'${new Date(r.ACQUIRED_AT).toISOString()}'` : 'NULL';
                return `('${escapeLiteral(r.WALLET_ADDRESS)}', '${escapeLiteral(r.NFT_ID)}', ${r.IS_LOCKED ? 'TRUE' : 'FALSE'}, ${acquiredAt})`;
            }).join(',\n');

            await pgQuery(`
        INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
        VALUES ${values}
        ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
          is_locked = EXCLUDED.is_locked,
          acquired_at = EXCLUDED.acquired_at;
      `);

            totalInserted += rows.length;
            offset += BATCH_SIZE;

            const pct = ((totalInserted / totalHoldings) * 100).toFixed(1);
            console.log(`  ✓ Inserted batch of ${rows.length} holdings (Total: ${totalInserted.toLocaleString()} = ${pct}%)`);

            if (rows.length < BATCH_SIZE) break;
            await delay(BATCH_DELAY_MS);
        }

        console.log(`\n✓ Finished dumping ${totalInserted.toLocaleString()} holdings`);
        return totalInserted;
    } else {
        // Copy from existing wallet_holdings table
        console.log("Copying from existing wallet_holdings table...");
        const result = await pgQuery(`
      INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
      SELECT 
        wallet_address, 
        nft_id, 
        COALESCE(is_locked, FALSE),
        last_event_ts
      FROM wallet_holdings
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked = EXCLUDED.is_locked,
        acquired_at = EXCLUDED.acquired_at;
    `);
        console.log(`✓ Copied ${result.rowCount || 'all'} holdings from wallet_holdings`);
        return result.rowCount || 0;
    }
}

// Mark burned NFTs
async function markBurnedNFTs(connection) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  MARKING BURNED NFTs");
    console.log("══════════════════════════════════════════════════════════════\n");

    const sql = `
    SELECT
      event_data:id::string AS nft_id,
      block_timestamp AS burned_at
    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
    WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
      AND event_type = 'MomentNFTBurned'
      AND tx_succeeded = TRUE;
  `;

    console.log("Querying Snowflake for burned NFTs...");
    const rows = await executeSnowflakeWithRetry(connection, sql, 3, 60000);
    console.log(`Found ${rows.length} burned NFTs`);

    if (rows.length === 0) return 0;

    // Update in batches
    const batchSize = 1000;
    let updated = 0;

    for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);

        for (const r of batch) {
            const burnedAt = r.BURNED_AT ? new Date(r.BURNED_AT).toISOString() : new Date().toISOString();
            await pgQuery(`
        UPDATE nfts SET burned_at = $1 WHERE nft_id = $2;
      `, [burnedAt, r.NFT_ID]);
        }

        updated += batch.length;
        console.log(`  Progress: ${updated}/${rows.length} burned NFTs marked`);
    }

    console.log(`✓ Marked ${rows.length} NFTs as burned`);
    return rows.length;
}

// Print summary statistics
async function printSummary() {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  FINAL SUMMARY");
    console.log("══════════════════════════════════════════════════════════════\n");

    const tables = ['series', 'sets', 'plays', 'editions', 'nfts', 'holdings'];

    for (const table of tables) {
        const result = await pgQuery(`SELECT COUNT(*) AS count FROM ${table};`);
        console.log(`  ${table}: ${parseInt(result.rows[0].count).toLocaleString()} rows`);
    }

    // Test the view
    const viewResult = await pgQuery(`SELECT COUNT(*) AS count FROM nft_core_metadata_v2;`);
    console.log(`  nft_core_metadata_v2 (view): ${parseInt(viewResult.rows[0].count).toLocaleString()} rows`);

    console.log("\n✓ Data dump complete!");
}

// Main function
async function main() {
    console.log("╔══════════════════════════════════════════════════════════════╗");
    console.log("║        SNOWFLAKE FINAL DATA DUMP                             ║");
    console.log("║    One-time extraction of all NFL All Day data               ║");
    console.log("╚══════════════════════════════════════════════════════════════╝\n");

    const startTime = Date.now();

    try {
        // Create Snowflake connection
        console.log("Connecting to Snowflake...");
        const connection = await createSnowflakeConnection();
        console.log("✓ Connected to Snowflake\n");

        // Create tables
        await createNormalizedTables();

        // Dump all data
        await dumpSeries(connection);
        await dumpSets(connection);
        await dumpPlays(connection);
        await dumpEditions(connection);
        await dumpNFTs(connection);
        await dumpHoldings(connection);
        await markBurnedNFTs(connection);

        // Print summary
        await printSummary();

        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`\nTotal time: ${elapsed} minutes`);

    } catch (err) {
        console.error("\n❌ ERROR:", err.message);
        console.error(err.stack);
        process.exit(1);
    }

    process.exit(0);
}

main();
