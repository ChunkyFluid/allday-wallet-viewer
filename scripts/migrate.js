// scripts/migrate.js
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Build config: use DATABASE_URL if present, otherwise PGHOST/etc.
const hasConnectionString = !!process.env.DATABASE_URL;

const pgConfig = hasConnectionString
  ? {
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    }
  : {
      host: process.env.PGHOST,
      port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
      user: process.env.PGUSER,
      password: process.env.PGPASSWORD,
      database: process.env.PGDATABASE,
      ssl: { rejectUnauthorized: false }
    };

// Log what we’re about to use (no passwords).
console.log('Migration Postgres config:', {
  ...pgConfig,
  // hide password if not using connectionString
  password: undefined,
  connectionString: hasConnectionString ? '[REDACTED]' : undefined
});

async function runMigrations() {
  const pool = new Pool(pgConfig);
  const client = await pool.connect();

  try {
    const dbInfo = await client.query('SELECT current_database(), current_user');
    console.log('Connected to DB as:', dbInfo.rows[0]);

    const schemaPath = path.join(__dirname, '..', 'db', 'schema.sql');
    console.log('Loading schema from:', schemaPath);

    if (!fs.existsSync(schemaPath)) {
      throw new Error(`schema.sql not found at ${schemaPath}`);
    }

    const schemaSql = fs.readFileSync(schemaPath, 'utf8');

    console.log('Starting transaction and running schema...');
    await client.query('BEGIN');
    await client.query(schemaSql);
    await client.query('COMMIT');

    console.log('✅ Migrations COMPLETE');
  } catch (err) {
    console.error('❌ Migration FAILED:', err);
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      console.error('Rollback also failed:', rollbackErr);
    }
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

runMigrations().catch((err) => {
  console.error('Unexpected top-level error:', err);
  process.exitCode = 1;
});
