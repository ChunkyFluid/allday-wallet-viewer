// test_pg.js - quick check that Postgres connection works

import { pgQuery } from './db.js';

async function run() {
  try {
    const result = await pgQuery('SELECT 1 AS value');
    console.log('Postgres test result:', result.rows[0]);
    process.exit(0);
  } catch (err) {
    console.error('Postgres test FAILED:', err);
    process.exit(1);
  }
}

run();
