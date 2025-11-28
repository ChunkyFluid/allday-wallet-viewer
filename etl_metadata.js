// etl_metadata.js - pull ALL NFT metadata from Snowflake into Postgres

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import * as dotenv from 'dotenv';
import snowflake from 'snowflake-sdk';
import { pgQuery } from './db.js';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load the metadata SQL from file
const metadataSqlPath = path.join(__dirname, 'metadata_query.sql');
const METADATA_SQL = fs.readFileSync(metadataSqlPath, 'utf8');

// Setup Snowflake connection using your existing env vars
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

    console.log('Running metadata query in Snowflake...');
    const rows = await sfQuery(METADATA_SQL);

console.log('Upserting metadata into Postgres... rows to upsert:', rows.length);

// Use a transaction
await pgQuery('BEGIN');

for (const row of rows) {
      const {
        nft_id,
        edition_id,
        play_id,
        series_id,
        set_id,
        tier,
        serial_number,
        max_mint_size,
        first_name,
        last_name,
        team_name,
        position,
        jersey_number,
        series_name,
        set_name
      } = row;

      await pgQuery(
        `
        INSERT INTO nft_core_metadata (
          nft_id,
          edition_id,
          play_id,
          series_id,
          set_id,
          tier,
          serial_number,
          max_mint_size,
          first_name,
          last_name,
          team_name,
          position,
          jersey_number,
          series_name,
          set_name
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12, $13, $14, $15
        )
        ON CONFLICT (nft_id) DO UPDATE SET
          edition_id     = EXCLUDED.edition_id,
          play_id        = EXCLUDED.play_id,
          series_id      = EXCLUDED.series_id,
          set_id         = EXCLUDED.set_id,
          tier           = EXCLUDED.tier,
          serial_number  = EXCLUDED.serial_number,
          max_mint_size  = EXCLUDED.max_mint_size,
          first_name     = EXCLUDED.first_name,
          last_name      = EXCLUDED.last_name,
          team_name      = EXCLUDED.team_name,
          position       = EXCLUDED.position,
          jersey_number  = EXCLUDED.jersey_number,
          series_name    = EXCLUDED.series_name,
          set_name       = EXCLUDED.set_name
        ;
        `,
        [
          nft_id,
          edition_id,
          play_id,
          series_id,
          set_id,
          tier,
          serial_number,
          max_mint_size,
          first_name,
          last_name,
          team_name,
          position,
          jersey_number,
          series_name,
          set_name
        ]
      );
    }

    await pgQuery('COMMIT');
    console.log('Metadata ETL complete.');
    process.exit(0);
  } catch (err) {
    console.error('ETL failed:', err);
    try {
      await pgQuery('ROLLBACK');
    } catch {
      // ignore
    }
    process.exit(1);
  }
}

run();
