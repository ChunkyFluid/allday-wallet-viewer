// test_holdings.js - see what's in wallet_holdings

import { pgQuery } from './db.js';

async function run() {
  try {
    const countRes = await pgQuery('SELECT COUNT(*) AS count FROM wallet_holdings');
    console.log('Row count in wallet_holdings:', countRes.rows[0].count);

    // Grab a few distinct wallets
    const walletsRes = await pgQuery(`
      SELECT wallet_address
      FROM wallet_holdings
      GROUP BY wallet_address
      ORDER BY wallet_address
      LIMIT 5;
    `);

    console.log('Sample wallets:');
    console.log(walletsRes.rows);

    if (walletsRes.rowCount > 0) {
      const w = walletsRes.rows[0].wallet_address;
      const holdingsRes = await pgQuery(
        `
        SELECT wallet_address, nft_id, last_event_ts
        FROM wallet_holdings
        WHERE wallet_address = $1
        ORDER BY last_event_ts DESC
        LIMIT 5;
        `,
        [w]
      );

      console.log(`Sample holdings for wallet ${w}:`);
      console.log(holdingsRes.rows);
    }

    process.exit(0);
  } catch (err) {
    console.error('test_holdings FAILED:', err);
    process.exit(1);
  }
}

run();
