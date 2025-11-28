// test_metadata.js - check how many rows are in nft_core_metadata

import { pgQuery } from './db.js';

async function run() {
  try {
    const countRes = await pgQuery('SELECT COUNT(*) AS count FROM nft_core_metadata');
    const sampleRes = await pgQuery('SELECT * FROM nft_core_metadata LIMIT 3');

    console.log('Row count in nft_core_metadata:', countRes.rows[0].count);
    console.log('Sample rows:');
    console.log(sampleRes.rows);

    process.exit(0);
  } catch (err) {
    console.error('test_metadata FAILED:', err);
    process.exit(1);
  }
}

run();
