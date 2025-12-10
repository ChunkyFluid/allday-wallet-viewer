// Build set_editions_snapshot: one row per (set_name, edition_id) with price fields
import * as dotenv from "dotenv";
dotenv.config();
import { pgQuery } from "../db.js";

async function ensureTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS set_editions_snapshot (
      set_name TEXT NOT NULL,
      edition_id TEXT NOT NULL,
      lowest_ask_usd NUMERIC,
      avg_sale_usd NUMERIC,
      top_sale_usd NUMERIC,
      updated_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (set_name, edition_id)
    );
  `);
}

async function build() {
  console.log("[set-editions] building snapshot...");
  await ensureTable();
  await pgQuery(`TRUNCATE set_editions_snapshot;`);
  await pgQuery(`
    INSERT INTO set_editions_snapshot (
      set_name, edition_id, lowest_ask_usd, avg_sale_usd, top_sale_usd, updated_at
    )
    SELECT
      m.set_name,
      m.edition_id,
      eps.lowest_ask_usd,
      eps.avg_sale_usd,
      eps.top_sale_usd,
      now()
    FROM nft_core_metadata m
    LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
    WHERE m.set_name IS NOT NULL
    GROUP BY m.set_name, m.edition_id, eps.lowest_ask_usd, eps.avg_sale_usd, eps.top_sale_usd;
  `);
  const { rows } = await pgQuery(`SELECT COUNT(*)::int AS cnt FROM set_editions_snapshot;`);
  console.log(`[set-editions] done. Rows: ${rows[0].cnt}`);
}

build()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

