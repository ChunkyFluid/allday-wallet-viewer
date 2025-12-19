// scripts/sync_nft_core_metadata_from_snowflake.js
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { executeSnowflakeWithRetry, delay, createSnowflakeConnection } from "./snowflake-utils.js";

dotenv.config();

// Large enough for throughput, small enough for memory / query time.
const BATCH_SIZE = 100000;
const BATCH_DELAY_MS = parseInt(process.env.SNOWFLAKE_BATCH_DELAY_MS || '500', 10); // Delay between batches

// Check for --incremental flag
const isIncremental = process.argv.includes('--incremental');

function escapeLiteral(str) {
    if (str == null) return null;
    return String(str).replace(/'/g, "''");
}

async function ensureMetadataTable() {
    console.log("Ensuring Render nft_core_metadata table exists...");
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS nft_core_metadata (
      nft_id        TEXT PRIMARY KEY,
      edition_id    TEXT,
      play_id       TEXT,
      series_id     TEXT,
      set_id        TEXT,
      tier          TEXT,
      serial_number INTEGER,
      max_mint_size INTEGER,
      first_name    TEXT,
      last_name     TEXT,
      team_name     TEXT,
      position      TEXT,
      jersey_number INTEGER,
      series_name   TEXT,
      set_name      TEXT
    );
  `);

    const before = await pgQuery(`SELECT COUNT(*) AS c FROM nft_core_metadata;`);
    console.log("Current Render nft_core_metadata row count (before sync):", before.rows[0].c);
}

async function syncMetadata() {
    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    console.log("Using Snowflake", `${db}.${schema}`);
    console.log("Mode:", isIncremental ? "INCREMENTAL (upsert only)" : "FULL REFRESH (truncate + reload)");

    const connection = await createSnowflakeConnection();
    await ensureMetadataTable();

    if (!isIncremental) {
        console.log("Truncating Render nft_core_metadata (full snapshot sync)...");
        await pgQuery(`TRUNCATE TABLE nft_core_metadata;`);
    } else {
        console.log("Incremental mode: skipping truncate, will upsert only");
    }

    // Keyset pagination over nft_id instead of OFFSET/LIMIT for better scaling.
    let lastNftId = null;
    let total = 0;

    while (true) {
        let whereClause = "";
        if (lastNftId) {
            const nftLit = `'${escapeLiteral(lastNftId)}'`;
            whereClause = `WHERE m.NFT_ID > ${nftLit}`;
        }

        const sql = `
      SELECT
        m.NFT_ID                             AS nft_id,
        m.EDITION_ID                         AS edition_id,
        m.PLAY_ID                            AS play_id,
        m.SERIES_ID                          AS series_id,
        m.SET_ID                             AS set_id,
        m.TIER                               AS tier,
        TRY_TO_NUMBER(m.SERIAL_NUMBER)       AS serial_number,
        TRY_TO_NUMBER(m.MAX_MINT_SIZE)       AS max_mint_size,
        m.FIRST_NAME                         AS first_name,
        m.LAST_NAME                          AS last_name,
        m.TEAM_NAME                          AS team_name,
        m.POSITION                           AS position,
        TRY_TO_NUMBER(m.JERSEY_NUMBER)       AS jersey_number,
        m.SERIES_NAME                        AS series_name,
        m.SET_NAME                           AS set_name
      FROM ${db}.${schema}.ALLDAY_CORE_NFT_METADATA m
      JOIN ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT h
        ON m.NFT_ID = h.NFT_ID
      ${whereClause}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY m.NFT_ID ORDER BY m.NFT_ID) = 1
      ORDER BY m.NFT_ID
      LIMIT ${BATCH_SIZE};
    `;

        console.log(
            `Fetching metadata batch from Snowflake: after_nft_id=${lastNftId || "START"}, limit=${BATCH_SIZE}...`
        );
        const rows = await executeSnowflakeWithRetry(connection, sql);

        console.log(`Snowflake returned ${rows.length} metadata rows for this batch.`);
        if (!rows.length) {
            console.log("No more metadata rows from Snowflake. Done reading.");
            break;
        }

        const valueLiterals = [];
        const seenInBatch = new Set(); // extra safety

        for (const row of rows) {
            const nftId = String(row.NFT_ID ?? row.nft_id);
            if (!nftId) continue;

            if (seenInBatch.has(nftId)) {
                continue;
            }
            seenInBatch.add(nftId);

            const editionId = row.EDITION_ID ?? row.edition_id ?? null;
            const playId = row.PLAY_ID ?? row.play_id ?? null;
            const seriesId = row.SERIES_ID ?? row.series_id ?? null;
            const setId = row.SET_ID ?? row.set_id ?? null;
            const tier = row.TIER ?? row.tier ?? null;
            const serialNumber = row.SERIAL_NUMBER ?? row.serial_number ?? null;
            const maxMintSize = row.MAX_MINT_SIZE ?? row.max_mint_size ?? null;
            const firstName = row.FIRST_NAME ?? row.first_name ?? null;
            const lastName = row.LAST_NAME ?? row.last_name ?? null;
            const teamName = row.TEAM_NAME ?? row.team_name ?? null;
            const position = row.POSITION ?? row.position ?? null;
            const jerseyNumber = row.JERSEY_NUMBER ?? row.jersey_number ?? null;
            const seriesName = row.SERIES_NAME ?? row.series_name ?? null;
            const setName = row.SET_NAME ?? row.set_name ?? null;

            const nftLit = `'${escapeLiteral(nftId)}'`;
            const editionLit = editionId != null ? `'${escapeLiteral(editionId)}'` : "NULL";
            const playLit = playId != null ? `'${escapeLiteral(playId)}'` : "NULL";
            const seriesLit = seriesId != null ? `'${escapeLiteral(seriesId)}'` : "NULL";
            const setLit = setId != null ? `'${escapeLiteral(setId)}'` : "NULL";
            const tierLit = tier != null ? `'${escapeLiteral(tier)}'` : "NULL";
            const serialLit = serialNumber != null ? `${Number(serialNumber)}` : "NULL";
            const maxMintLit = maxMintSize != null ? `${Number(maxMintSize)}` : "NULL";
            const firstNameLit = firstName != null ? `'${escapeLiteral(firstName)}'` : "NULL";
            const lastNameLit = lastName != null ? `'${escapeLiteral(lastName)}'` : "NULL";
            const teamNameLit = teamName != null ? `'${escapeLiteral(teamName)}'` : "NULL";
            const positionLit = position != null ? `'${escapeLiteral(position)}'` : "NULL";
            const jerseyLit = jerseyNumber != null ? `${Number(jerseyNumber)}` : "NULL";
            const seriesNameLit = seriesName != null ? `'${escapeLiteral(seriesName)}'` : "NULL";
            const setNameLit = setName != null ? `'${escapeLiteral(setName)}'` : "NULL";

            valueLiterals.push(
                `(${nftLit}, ${editionLit}, ${playLit}, ${seriesLit}, ${setLit}, ${tierLit}, ` +
                    `${serialLit}, ${maxMintLit}, ${firstNameLit}, ${lastNameLit}, ${teamNameLit}, ` +
                    `${positionLit}, ${jerseyLit}, ${seriesNameLit}, ${setNameLit})`
            );
        }

        if (!valueLiterals.length) {
            console.log("No valid metadata rows in this batch after cleaning/dedupe, skipping insert.");
            continue;
        }

        const insertSql = `
      INSERT INTO nft_core_metadata (
        nft_id,
        edition_id,
        play_id,
        series_id,
        set_id,
        tier,
        serial_number,
        max_mint_size,
        first_name,
        last_name,
        team_name,
        position,
        jersey_number,
        series_name,
        set_name
      )
      VALUES ${valueLiterals.join(",")}
      ON CONFLICT (nft_id) DO UPDATE SET
        edition_id    = EXCLUDED.edition_id,
        play_id       = EXCLUDED.play_id,
        series_id     = EXCLUDED.series_id,
        set_id        = EXCLUDED.set_id,
        tier          = EXCLUDED.tier,
        serial_number = EXCLUDED.serial_number,
        max_mint_size = EXCLUDED.max_mint_size,
        first_name    = EXCLUDED.first_name,
        last_name     = EXCLUDED.last_name,
        team_name     = EXCLUDED.team_name,
        position      = EXCLUDED.position,
        jersey_number = EXCLUDED.jersey_number,
        series_name   = EXCLUDED.series_name,
        set_name      = EXCLUDED.set_name;
    `;

        console.log(`Inserting ${valueLiterals.length} metadata rows into Render...`);
        await pgQuery(insertSql); // no params

        total += valueLiterals.length;

        // Advance keyset cursor using the last row in this batch
        const lastRow = rows[rows.length - 1];
        const nftIdLast = String(lastRow.NFT_ID ?? lastRow.nft_id);
        lastNftId = nftIdLast;

        console.log(`Upserted ${total} nft_core_metadata rows so far... last_nft_id=${lastNftId}`);
        
        // Add delay between batches to reduce load on Snowflake
        if (BATCH_DELAY_MS > 0) {
            await delay(BATCH_DELAY_MS);
        }
    }

    const after = await pgQuery(`SELECT COUNT(*) AS c FROM nft_core_metadata;`);
    console.log("✅ Final Render nft_core_metadata row count:", after.rows[0].c);

    connection.destroy(() => {
        console.log("Snowflake connection closed.");
    });
}

syncMetadata()
    .then(() => {
        console.log("✅ Metadata sync complete.");
        process.exit(0);
    })
    .catch((err) => {
        console.error("💥 Fatal error during metadata sync:", err);
        process.exit(1);
    });
