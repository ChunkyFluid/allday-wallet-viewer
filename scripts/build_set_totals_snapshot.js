import * as dotenv from "dotenv";
dotenv.config();
import { pgQuery } from "../db.js";

async function ensureTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS set_totals_snapshot (
      set_name TEXT PRIMARY KEY,
      total_editions INTEGER NOT NULL DEFAULT 0,
      is_team_set BOOLEAN NOT NULL DEFAULT false,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_set_totals_teamflag ON set_totals_snapshot(is_team_set);`);
}

async function build() {
  console.log("[set-totals] building snapshot...");
  await ensureTable();

  const teamNamesRes = await pgQuery(`
    SELECT DISTINCT LOWER(team_name) AS team_name
    FROM nft_core_metadata
    WHERE team_name IS NOT NULL AND team_name <> ''
  `);
  const teamNames = teamNamesRes.rows.map(r => r.team_name);

  await pgQuery(`TRUNCATE set_totals_snapshot;`);
  await pgQuery(
    `
    INSERT INTO set_totals_snapshot (set_name, total_editions, is_team_set, updated_at)
    SELECT
      m.set_name,
      COUNT(DISTINCT m.edition_id) AS total_editions,
      FALSE AS is_team_set,
      now() AS updated_at
    FROM nft_core_metadata m
    WHERE m.set_name IS NOT NULL
    GROUP BY m.set_name;
    `
  );

  // Update is_team_set based on team names appearing in set_name
  if (teamNames.length) {
    const patterns = teamNames.map(t => `'%' || ${pgQuoteLiteral(t)} || '%'`).join(" OR ");
    // We cannot parameterize dynamic LIKE list easily; do a simple update in JS loop
   for (const team of teamNames) {
      await pgQuery(
        `
        UPDATE set_totals_snapshot
        SET is_team_set = true
        WHERE LOWER(set_name) LIKE $1;
        `,
        [`%${team}%`]
      );
    }
  }

  const { rows } = await pgQuery(`SELECT COUNT(*)::int AS cnt FROM set_totals_snapshot;`);
  console.log(`[set-totals] done. Rows: ${rows[0].cnt}`);
}

function pgQuoteLiteral(str) {
  return str.replace(/'/g, "''");
}

build()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });