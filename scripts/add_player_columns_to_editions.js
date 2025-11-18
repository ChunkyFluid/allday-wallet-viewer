// scripts/add_player_columns_to_editions.js
import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

const pool = new Pool({
  host: process.env.PGHOST,
  port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  ssl: { rejectUnauthorized: false }
});

pool.on("error", (err) => {
  console.error("Postgres pool error:", err);
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("Connected to Neon as:", {
      host: process.env.PGHOST,
      db: process.env.PGDATABASE,
      user: process.env.PGUSER
    });

    await client.query(`
      ALTER TABLE editions
        ADD COLUMN IF NOT EXISTS play_id text,
        ADD COLUMN IF NOT EXISTS first_name text,
        ADD COLUMN IF NOT EXISTS last_name text,
        ADD COLUMN IF NOT EXISTS team_name text,
        ADD COLUMN IF NOT EXISTS position text,
        ADD COLUMN IF NOT EXISTS jersey_number integer
    `);

    console.log("✅ editions table altered: player/team columns added (if missing).");
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error("❌ Error in add_player_columns_to_editions:", err);
  process.exit(1);
});
