// scripts/add_columns_to_wallet_holdings.js
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
      ALTER TABLE wallet_holdings
        ADD COLUMN IF NOT EXISTS is_locked boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS last_event_ts timestamptz;
    `);

    console.log("✅ wallet_holdings table altered: is_locked + last_event_ts added (if missing).");
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error("❌ Error in add_columns_to_wallet_holdings:", err);
  process.exit(1);
});
