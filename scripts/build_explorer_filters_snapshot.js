// Build explorer_filters_snapshot from nft_core_metadata for fast filter dropdowns
import * as dotenv from "dotenv";
dotenv.config();

import { pgQuery } from "../db.js";

async function ensureTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS explorer_filters_snapshot (
      id INTEGER PRIMARY KEY DEFAULT 1,
      players JSONB,
      teams JSONB,
      series JSONB,
      sets JSONB,
      positions JSONB,
      tiers JSONB,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT explorer_filters_snapshot_single_row CHECK (id = 1)
    );
  `);
}

async function buildSnapshot() {
  console.log("[filters] Building explorer_filters_snapshot...");
  await ensureTable();

  const [playersRes, teamsRes, seriesRes, setsRes, positionsRes, tiersRes] = await Promise.all([
    pgQuery(`
      SELECT
        COALESCE(first_name, '') AS first_name,
        COALESCE(last_name, '')  AS last_name
      FROM nft_core_metadata
      WHERE first_name IS NOT NULL AND first_name <> ''
        AND last_name  IS NOT NULL AND last_name  <> ''
      GROUP BY first_name, last_name
      ORDER BY last_name, first_name
      LIMIT 5000;
    `),
    pgQuery(`
      SELECT team_name
      FROM nft_core_metadata
      WHERE team_name IS NOT NULL AND team_name <> ''
      GROUP BY team_name
      ORDER BY team_name
      LIMIT 1000;
    `),
    pgQuery(`
      SELECT series_name
      FROM nft_core_metadata
      WHERE series_name IS NOT NULL AND series_name <> ''
      GROUP BY series_name
      ORDER BY series_name
      LIMIT 1000;
    `),
    pgQuery(`
      SELECT set_name
      FROM nft_core_metadata
      WHERE set_name IS NOT NULL AND set_name <> ''
      GROUP BY set_name
      ORDER BY set_name
      LIMIT 2000;
    `),
    pgQuery(`
      SELECT position
      FROM nft_core_metadata
      WHERE position IS NOT NULL AND position <> ''
      GROUP BY position
      ORDER BY position
      LIMIT 200;
    `),
    pgQuery(`
      SELECT tier
      FROM nft_core_metadata
      WHERE tier IS NOT NULL AND tier <> ''
      GROUP BY tier
      ORDER BY tier
      LIMIT 50;
    `)
  ]);

  const players = playersRes.rows.map(r => ({ first_name: r.first_name, last_name: r.last_name }));
  const teams = teamsRes.rows.map(r => r.team_name);
  const series = seriesRes.rows.map(r => r.series_name);
  const sets = setsRes.rows.map(r => r.set_name);
  const positions = positionsRes.rows.map(r => r.position);
  const tiers = tiersRes.rows.map(r => r.tier);

  await pgQuery(
    `
    INSERT INTO explorer_filters_snapshot (
      id, players, teams, series, sets, positions, tiers, updated_at
    )
    VALUES (1, $1::jsonb, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, now())
    ON CONFLICT (id) DO UPDATE SET
      players    = EXCLUDED.players,
      teams      = EXCLUDED.teams,
      series     = EXCLUDED.series,
      sets       = EXCLUDED.sets,
      positions  = EXCLUDED.positions,
      tiers      = EXCLUDED.tiers,
      updated_at = now();
  `,
    [JSON.stringify(players), JSON.stringify(teams), JSON.stringify(series), JSON.stringify(sets), JSON.stringify(positions), JSON.stringify(tiers)]
  );

  console.log("[filters] Snapshot updated", {
    players: players.length,
    teams: teams.length,
    series: series.length,
    sets: sets.length,
    positions: positions.length,
    tiers: tiers.length
  });
}

buildSnapshot()
  .then(() => {
    console.log("[filters] ✅ Done");
    process.exit(0);
  })
  .catch((err) => {
    console.error("[filters] ❌ Error", err);
    process.exit(1);
  });

