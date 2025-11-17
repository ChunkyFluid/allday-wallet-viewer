// init_db.js
// Create allday_analytics database + core tables

import pg from 'pg';
import * as dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

// This pool connects to the default "postgres" database so we can CREATE DATABASE
const adminPool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: Number(process.env.PGPORT || 5432),
  database: 'postgres',
  user: process.env.PGUSER || 'postgres',
  password: process.env.PGPASSWORD || '',
});

// Helper for one-off queries using a given pool
async function runQuery(pool, text, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(text, params);
  } finally {
    client.release();
  }
}

async function ensureDatabaseExists() {
  const dbName = 'allday_analytics';

  console.log(`Checking if database "${dbName}" exists...`);

  const checkRes = await runQuery(
    adminPool,
    'SELECT 1 FROM pg_database WHERE datname = $1',
    [dbName]
  );

  if (checkRes.rowCount > 0) {
    console.log(`Database "${dbName}" already exists.`);
    return;
  }

  console.log(`Creating database "${dbName}"...`);
  await runQuery(adminPool, `CREATE DATABASE ${dbName}`);
  console.log(`Database "${dbName}" created.`);
}

async function createTables() {
  const dbName = 'allday_analytics';

  // New pool, connected specifically to allday_analytics
  const appPool = new pg.Pool({
    host: process.env.PGHOST || 'localhost',
    port: Number(process.env.PGPORT || 5432),
    database: dbName,
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || '',
  });

  console.log(`Creating tables in database "${dbName}"...`);

  // 1) nft_core_metadata
  await runQuery(
    appPool,
    `
    CREATE TABLE IF NOT EXISTS nft_core_metadata (
      nft_id         TEXT PRIMARY KEY,
      edition_id     TEXT,
      play_id        TEXT,
      series_id      TEXT,
      set_id         TEXT,
      tier           TEXT,
      serial_number  INTEGER,
      max_mint_size  INTEGER,
      first_name     TEXT,
      last_name      TEXT,
      team_name      TEXT,
      position       TEXT,
      jersey_number  TEXT,
      series_name    TEXT,
      set_name       TEXT
    );
    `
  );

  // 2) wallet_holdings
  await runQuery(
    appPool,
    `
    CREATE TABLE IF NOT EXISTS wallet_holdings (
      wallet_address TEXT,
      nft_id         TEXT,
      is_locked      BOOLEAN NOT NULL DEFAULT FALSE,
      last_event_ts  TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (wallet_address, nft_id)
    );
    `
  );

  await runQuery(
    appPool,
    `
    CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet
      ON wallet_holdings (wallet_address);
    `
  );

  // 3) edition_price_stats
  await runQuery(
    appPool,
    `
    CREATE TABLE IF NOT EXISTS edition_price_stats (
      edition_id   TEXT PRIMARY KEY,
      asp_90d      NUMERIC,
      last_sale    NUMERIC,
      last_sale_ts TIMESTAMPTZ,
      low_ask      NUMERIC,
      low_ask_ts   TIMESTAMPTZ,
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    `
  );

  console.log('Tables created / verified.');

  await appPool.end();
}

async function run() {
  try {
    await ensureDatabaseExists();
    await createTables();
    console.log('init_db.js finished successfully.');
    process.exit(0);
  } catch (err) {
    console.error('init_db.js FAILED:', err);
    process.exit(1);
  } finally {
    await adminPool.end();
  }
}

run();
