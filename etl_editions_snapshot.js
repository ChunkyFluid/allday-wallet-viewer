// etl_editions_snapshot.js
import * as dotenv from "dotenv";
import pool from "./db/pool.js";

dotenv.config();

async function refreshEditionsSnapshot() {
  const client = await pool.connect();

  try {
    console.log("Refreshing editions_snapshot…");
    await client.query("BEGIN");

    // Create table if it doesn't exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS editions_snapshot (
        edition_id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        team_name TEXT,
        position TEXT,
        tier TEXT,
        series_name TEXT,
        set_name TEXT,
        max_mint_size INTEGER,
        total_moments INTEGER NOT NULL,
        min_serial INTEGER,
        max_serial INTEGER,
        lowest_ask_usd NUMERIC,
        avg_sale_usd NUMERIC,
        top_sale_usd NUMERIC,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    // Create indexes for faster filtering and sorting
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_player 
      ON editions_snapshot (last_name NULLS LAST, first_name NULLS LAST);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_team 
      ON editions_snapshot (team_name);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_tier 
      ON editions_snapshot (tier);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_series 
      ON editions_snapshot (series_name);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_set 
      ON editions_snapshot (set_name);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_position 
      ON editions_snapshot (position);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_price 
      ON editions_snapshot (lowest_ask_usd NULLS LAST);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_editions_snapshot_asp 
      ON editions_snapshot (avg_sale_usd NULLS LAST);
    `);

    // Wipe previous snapshot
    await client.query("TRUNCATE TABLE editions_snapshot");

    // Recompute from nft_core_metadata + prices, grouped by edition_id
    await client.query(`
      INSERT INTO editions_snapshot (
        edition_id,
        first_name,
        last_name,
        team_name,
        position,
        tier,
        series_name,
        set_name,
        max_mint_size,
        total_moments,
        min_serial,
        max_serial,
        lowest_ask_usd,
        avg_sale_usd,
        top_sale_usd,
        updated_at
      )
      SELECT
        e.edition_id,
        MAX(e.first_name) AS first_name,
        MAX(e.last_name) AS last_name,
        MAX(e.team_name) AS team_name,
        MAX(e.position) AS position,
        MAX(e.tier) AS tier,
        MAX(e.series_name) AS series_name,
        MAX(e.set_name) AS set_name,
        MAX(e.max_mint_size) AS max_mint_size,
        COUNT(*)::int AS total_moments,
        MIN(e.serial_number) AS min_serial,
        MAX(e.serial_number) AS max_serial,
        eps.lowest_ask_usd,
        eps.avg_sale_usd,
        eps.top_sale_usd,
        now() AS updated_at
      FROM nft_core_metadata e
      LEFT JOIN public.edition_price_scrape eps ON eps.edition_id = e.edition_id
      WHERE e.edition_id IS NOT NULL
      GROUP BY 
        e.edition_id,
        eps.lowest_ask_usd,
        eps.avg_sale_usd,
        eps.top_sale_usd;
    `);

    await client.query("COMMIT");
    console.log("✅ editions_snapshot refresh complete.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("❌ Error refreshing editions_snapshot:", err);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

refreshEditionsSnapshot();

