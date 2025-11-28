// etl_edition_prices.js - LEGACY analytics script.
// Computes ASP per edition via Snowflake into edition_price_stats.
// The main app uses public.edition_price_scrape instead.

import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import { pgQuery } from "./db.js";

dotenv.config();

// --- Snowflake connection (same style as other ETL scripts) ---
const sfConnection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE
});

function connectSnowflake() {
  return new Promise((resolve, reject) => {
    sfConnection.connect((err, conn) => {
      if (err) {
        console.error("Snowflake connect error:", err);
        return reject(err);
      }
      console.log("Connected to Snowflake as", conn.getId());
      resolve();
    });
  });
}

function sfQueryWithBinds(sqlText, binds) {
  return new Promise((resolve, reject) => {
    sfConnection.execute({
      sqlText,
      binds,
      complete(err, _stmt, rows) {
        if (err) {
          console.error("Snowflake query error:", err);
          return reject(err);
        }
        resolve(rows);
      }
    });
  });
}

// --- Main ETL ---
async function run() {
  try {
    await connectSnowflake();

    // 1) Get edition IDs we actually care about: those that appear in wallet_holdings
    console.log("Getting edition IDs from Postgres...");
    const editionRes = await pgQuery(`
      SELECT DISTINCT m.edition_id
      FROM nft_core_metadata m
      JOIN wallet_holdings h ON h.nft_id = m.nft_id
      WHERE m.edition_id IS NOT NULL
    `);

    const allEditionIds = editionRes.rows.map((r) => r.edition_id);
    console.log("Total edition_ids to price:", allEditionIds.length);

    if (!allEditionIds.length) {
      console.log("No edition IDs found to price. Exiting.");
      process.exit(0);
    }

    // 2) Chunk edition IDs into batches for Snowflake
    const batchSize = 200;
    const batches = [];
    for (let i = 0; i < allEditionIds.length; i += batchSize) {
      batches.push(allEditionIds.slice(i, i + batchSize));
    }
    console.log("Total batches:", batches.length);

    // 3) ASP query (this is basically your original working getASP SQL)
    const buildAspSql = (count) => {
      const placeholders = Array(count).fill("?").join(",");
      return `
    SELECT
      event_data:editionID::string AS editionID,
      AVG(TRY_TO_DOUBLE(event_data:price::string)) AS asp
    FROM flow_onchain_core_data.core.fact_events
    WHERE tx_succeeded = TRUE
      -- only look at events with prices
      AND TRY_TO_DOUBLE(event_data:price::string) IS NOT NULL
      -- limit to the editions we care about
      AND event_data:editionID::string IN (${placeholders})
      -- only last 90 days for performance
      AND block_timestamp >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1;
  `;
    };

    // 4) Accumulate results in a map: edition_id -> asp
    const aspMap = new Map();

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Processing batch ${i + 1}/${batches.length} (size ${batch.length})...`);

      const sql = buildAspSql(batch.length);
      let rows = [];
      try {
        rows = await sfQueryWithBinds(sql, batch);
      } catch (err) {
        console.error("Batch ASP query failed, skipping this batch:", err.message || err);
        continue;
      }

      for (const r of rows) {
        const editionId = r.EDITIONID || r.editionid || r.edition_id;
        const asp = r.ASP || r.asp;
        if (!editionId || asp == null) continue;
        aspMap.set(String(editionId), Number(asp));
      }
    }

    console.log("Total editions with ASP from Snowflake:", aspMap.size);

    // 5) Write results into edition_price_stats
    await pgQuery("BEGIN");
    // Keep it simple: rebuild table each run
    await pgQuery("DELETE FROM edition_price_stats");

    let inserted = 0;
    for (const [editionId, asp] of aspMap.entries()) {
      await pgQuery(
        `
        INSERT INTO edition_price_stats (
          edition_id,
          asp_90d,
          last_sale,
          last_sale_ts,
          updated_at
        ) VALUES ($1, $2, NULL, NULL, NOW())
        ON CONFLICT (edition_id) DO UPDATE SET
          asp_90d    = EXCLUDED.asp_90d,
          last_sale  = EXCLUDED.last_sale,
          last_sale_ts = EXCLUDED.last_sale_ts,
          updated_at = EXCLUDED.updated_at
        ;
        `,
        [editionId, asp]
      );
      inserted++;
    }

    await pgQuery("COMMIT");
    console.log("Edition price ETL complete. Rows upserted:", inserted);
    process.exit(0);
  } catch (err) {
    console.error("Edition price ETL FAILED:", err);
    try {
      await pgQuery("ROLLBACK");
    } catch {
      // ignore
    }
    process.exit(1);
  }
}

run();
