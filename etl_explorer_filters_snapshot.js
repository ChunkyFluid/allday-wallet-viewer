// etl_explorer_filters_snapshot.js
import * as dotenv from "dotenv";
import pool from "./db/pool.js";

dotenv.config();

async function refreshExplorerFiltersSnapshot() {
  const client = await pool.connect();

  try {
    console.log("Refreshing explorer_filters_snapshot…");
    await client.query("BEGIN");

    // 1) Load distinct values from nft_core_metadata (same logic as /api/explorer-filters)
    const [
      playersRes,
      teamsRes,
      seriesRes,
      setsRes,
      positionsRes,
      tiersRes
    ] = await Promise.all([
      client.query(`
        SELECT DISTINCT
          COALESCE(first_name, '') AS first_name,
          COALESCE(last_name, '')  AS last_name
        FROM nft_core_metadata
        WHERE first_name IS NOT NULL
          AND first_name <> ''
          AND last_name IS NOT NULL
          AND last_name <> ''
        ORDER BY last_name, first_name
        LIMIT 5000;
      `),
      client.query(`
        SELECT DISTINCT team_name
        FROM nft_core_metadata
        WHERE team_name IS NOT NULL
          AND team_name <> ''
        ORDER BY team_name
        LIMIT 1000;
      `),
      client.query(`
        SELECT DISTINCT series_name
        FROM nft_core_metadata
        WHERE series_name IS NOT NULL
          AND series_name <> ''
        ORDER BY series_name
        LIMIT 1000;
      `),
      client.query(`
        SELECT DISTINCT set_name
        FROM nft_core_metadata
        WHERE set_name IS NOT NULL
          AND set_name <> ''
        ORDER BY set_name
        LIMIT 2000;
      `),
      client.query(`
        SELECT DISTINCT position
        FROM nft_core_metadata
        WHERE position IS NOT NULL
          AND position <> ''
        ORDER BY position
        LIMIT 100;
      `),
      client.query(`
        SELECT DISTINCT tier
        FROM nft_core_metadata
        WHERE tier IS NOT NULL
          AND tier <> ''
        ORDER BY tier
        LIMIT 20;
      `)
    ]);

    const players = playersRes.rows.map((r) => ({
      first_name: r.first_name || "",
      last_name: r.last_name || ""
    }));

    const teams = teamsRes.rows
      .map((r) => r.team_name)
      .filter(Boolean);

    const series = seriesRes.rows
      .map((r) => r.series_name)
      .filter(Boolean);

    const sets = setsRes.rows
      .map((r) => r.set_name)
      .filter(Boolean);

    const positions = positionsRes.rows
      .map((r) => r.position)
      .filter(Boolean);

    const tiers = tiersRes.rows
      .map((r) => r.tier)
      .filter(Boolean);

    console.log("Explorer filters counts:", {
      players: players.length,
      teams: teams.length,
      series: series.length,
      sets: sets.length,
      positions: positions.length,
      tiers: tiers.length
    });

    // 2) Replace the single snapshot row
    await client.query("DELETE FROM explorer_filters_snapshot WHERE id = 1");

    await client.query(
      `
      INSERT INTO explorer_filters_snapshot (
        id,
        players,
        teams,
        series,
        sets,
        positions,
        tiers,
        updated_at
      )
      VALUES (
        1,
        $1::jsonb,
        $2::jsonb,
        $3::jsonb,
        $4::jsonb,
        $5::jsonb,
        $6::jsonb,
        now()
      );
      `,
      [
        JSON.stringify(players),
        JSON.stringify(teams),
        JSON.stringify(series),
        JSON.stringify(sets),
        JSON.stringify(positions),
        JSON.stringify(tiers)
      ]
    );

    await client.query("COMMIT");
    console.log("explorer_filters_snapshot refresh complete.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error refreshing explorer_filters_snapshot:", err);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

refreshExplorerFiltersSnapshot();
