import dotenv from "dotenv";
// IMPORTANT: override any existing env vars like PGHOST
dotenv.config({ override: true });

import pkg from "pg";
const { Pool } = pkg;

// hard-fail if env isn’t set
if (!process.env.PGHOST) {
  throw new Error("PGHOST is not set – check your .env file");
}
if (!process.env.PGUSER || !process.env.PGPASSWORD) {
  throw new Error("PGUSER / PGPASSWORD not set – check your .env file");
}

const useSsl = (process.env.PGSSLMODE || "").toLowerCase() === "require";

console.log("Postgres config:", {
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  ssl: useSsl
});

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: String(process.env.PGPASSWORD ?? ""),
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  ssl: useSsl ? { rejectUnauthorized: false } : undefined
});

export async function pgQuery(text, params) {
  const client = await pool.connect();
  try {
    return await client.query(text, params);
  } finally {
    client.release();
  }
}
