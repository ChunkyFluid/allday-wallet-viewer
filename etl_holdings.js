// etl_holdings.js - build current wallet holdings from Snowflake snapshot

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import * as dotenv from 'dotenv';
import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load the holdings snapshot SQL
const holdingsSqlPath = path.join(__dirname, 'holdings_events_query.sql');
const HOLDINGS_SQL = fs.readFileSync(holdingsSqlPath, 'utf8');

// Setup Snowflake connection
const sfConnection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE
});

function connectSnowflake() {
  return new Promise((resolve, reject) => {
    sfConnection.connect((err, conn) => {
      if (err) {
        console.error('Snowflake connect error:', err);
        return reject(err);
      }
      console.log('Connected to Snowflake as', conn.getId());
      resolve();
    });
  });
}

function sfQuery(sqlText) {
  return new Promise((resolve, reject) => {
    sfConnection.execute({
      sqlText,
      complete(err, stmt, rows) {
        if (err) {
          console.error('Snowflake query error:', err);
          return reject(err);
        }
        console.log('Snowflake returned', rows.length, 'rows');
        resolve(rows);
      }
    });
  });
}

async function run() {
  try {
    await connectSnowflake();

    console.log('Running holdings snapshot query in Snowflake...');
    const rows = await sfQuery(HOLDINGS_SQL);

    // TEMP LIMIT so this stays light while we test
    const limitedRows = rows.slice(0, 20000);
    console.log('Holdings rows to upsert:', limitedRows.length);

    await pgQuery('BEGIN');

    // For now, rebuild the table from scratch each run
    await pgQuery('DELETE FROM wallet_holdings');

    for (const row of limitedRows) {
      const { nft_id, wallet_address, block_timestamp } = row;

      if (!nft_id || !wallet_address) continue;

      const ts = new Date(block_timestamp).toISOString();

      await pgQuery(
        `
        INSERT INTO wallet_holdings (
          wallet_address,
          nft_id,
          is_locked,
          last_event_ts
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
          is_locked     = EXCLUDED.is_locked,
          last_event_ts = EXCLUDED.last_event_ts
        ;
        `,
        [wallet_address.toLowerCase(), nft_id, false, ts]
      );
    }

    await pgQuery('COMMIT');
    console.log('Holdings ETL complete.');
    process.exit(0);
  } catch (err) {
    console.error('Holdings ETL FAILED:', err);
    try {
      await pgQuery('ROLLBACK');
    } catch {
      // ignore
    }
    process.exit(1);
  }
}

run();
