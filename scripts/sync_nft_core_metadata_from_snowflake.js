// scripts/sync_nft_core_metadata_from_snowflake.js
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import { pgQuery } from "../db.js";

dotenv.config();

// Large enough for throughput, small enough for memory / query time.
const BATCH_SIZE = 100000;

function createSnowflakeConnection() {
    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    return new Promise((resolve, reject) => {
        connection.connect((err, conn) => {
            if (err) {
                console.error("❌ Snowflake connect error:", err);
                return reject(err);
            }
            console.log("✅ Connected to Snowflake as", conn.getId());
            resolve(connection);
        });
    });
}

function executeSnowflake(connection, sqlText) {
    return new Promise((resolve, reject) => {
        connection.execute({
            sqlText,
            complete(err, stmt, rows) {
                if (err) {
                    console.error("❌ Snowflake query error:", err);
                    return reject(err);
                }
                resolve(rows || []);
            }
        });
    });
}

function escapeLiteral(str) {
    if (str == null) return null;
    return String(str).replace(/'/g, "''");
}

async function ensureMetadataTable() {
    console.log("Ensuring Neon nft_core_metadata table exists...");
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
    console.log("Current Neon nft_core_metadata row count (before sync):", before.rows[0].c);
}

async function syncMetadata() {
    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    console.log("Using Snowflake", `${db}.${schema}`);

    const connection = await createSnowflakeConnection();
    await ensureMetadataTable();

    console.log("Truncating Neon nft_core_metadata (full snapshot sync)...");
    await pgQuery(`TRUNCATE TABLE nft_core_metadata;`);

    // Keyset pagination over nft_id instead of OFFSET/LIMIT for better scaling.
    let lastNftId = null;
    let total = 0;

    while (true) {
        let whereClause = "";
        if (lastNftId) {
            const nftLit = `'${escapeLiteral(lastNftId)}'`;
            whereClause = `
      WHERE m.nft_id > ${nftLit}`;
        }

        const sql = `
      SELECT
        m.nft_id                             AS nft_id,
        m.editionID                          AS edition_id,
        m.playID                             AS play_id,
        m.seriesID                           AS series_id,
        m.setID                              AS set_id,
        m.tier                               AS tier,
        TRY_TO_NUMBER(m.serialNumber)        AS serial_number,
        TRY_TO_NUMBER(m.maxMintSize)         AS max_mint_size,
        m.firstName                          AS first_name,
        m.lastName                           AS last_name,
        m.teamName                           AS team_name,
        m.position                           AS position,
        TRY_TO_NUMBER(m.jerseyNumber)        AS jersey_number,
        m.seriesName                         AS series_name,
        m.setName                            AS set_name
      FROM ${db}.${schema}.ALLDAY_CORE_NFT_METADATA m
      JOIN ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT h
        ON m.nft_id = h.nft_id
      QUALIFY ROW_NUMBER() OVER (PARTITION BY m.nft_id ORDER BY m.nft_id) = 1
      ${whereClause}
      ORDER BY m.nft_id
      LIMIT ${BATCH_SIZE};
    `;

        console.log(
            `Fetching metadata batch from Snowflake: after_nft_id=${lastNftId || "START"}, limit=${BATCH_SIZE}...`
        );
        const rows = await executeSnowflake(connection, sql);

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

        console.log(`Inserting ${valueLiterals.length} metadata rows into Neon...`);
        await pgQuery(insertSql); // no params

        total += valueLiterals.length;

        // Advance keyset cursor using the last row in this batch
        const lastRow = rows[rows.length - 1];
        const nftIdLast = String(lastRow.NFT_ID ?? lastRow.nft_id);
        lastNftId = nftIdLast;

        console.log(`Upserted ${total} nft_core_metadata rows so far... last_nft_id=${lastNftId}`);
    }

    const after = await pgQuery(`SELECT COUNT(*) AS c FROM nft_core_metadata;`);
    console.log("✅ Final Neon nft_core_metadata row count:", after.rows[0].c);

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
