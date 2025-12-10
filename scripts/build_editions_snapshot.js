// Build editions_snapshot for fast browse searches (one row per edition)
import * as dotenv from "dotenv";
dotenv.config();

import { pgQuery } from "../db.js";

async function ensureTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS editions_snapshot (
      edition_id TEXT PRIMARY KEY,
      first_name TEXT,
      last_name  TEXT,
      team_name  TEXT,
      position   TEXT,
      tier       TEXT,
      series_name TEXT,
      set_name    TEXT,
      max_mint_size INTEGER,
      total_moments INTEGER,
      min_serial INTEGER,
      max_serial INTEGER,
      lowest_ask_usd NUMERIC,
      avg_sale_usd  NUMERIC,
      top_sale_usd  NUMERIC,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function buildSnapshot() {
  console.log("[editions] Building editions_snapshot...");
  await ensureTable();

  await pgQuery(`TRUNCATE editions_snapshot;`);

  await pgQuery(`
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
      MAX(e.last_name)  AS last_name,
      MAX(e.team_name)  AS team_name,
      MAX(e.position)   AS position,
      MAX(e.tier)       AS tier,
      MAX(e.series_name) AS series_name,
      MAX(e.set_name)    AS set_name,
      MAX(e.max_mint_size)::int AS max_mint_size,
      COUNT(*)::int AS total_moments,
      MIN(e.serial_number)::int AS min_serial,
      MAX(e.serial_number)::int AS max_serial,
      MAX(eps.lowest_ask_usd) AS lowest_ask_usd,
      MAX(eps.avg_sale_usd)   AS avg_sale_usd,
      MAX(eps.top_sale_usd)   AS top_sale_usd,
      now() AS updated_at
    FROM nft_core_metadata e
    LEFT JOIN public.edition_price_scrape eps ON eps.edition_id = e.edition_id
    GROUP BY e.edition_id;
  `);

  const { rows } = await pgQuery(`SELECT COUNT(*)::int AS cnt FROM editions_snapshot;`);
  console.log("[editions] ✅ Snapshot updated. Rows:", rows[0].cnt);
}

buildSnapshot()
  .then(() => {
    console.log("[editions] Done");
    process.exit(0);
  })
  .catch((err) => {
    console.error("[editions] ❌ Error", err);
    process.exit(1);
  });

