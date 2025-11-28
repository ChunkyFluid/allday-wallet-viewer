// insert_dummy_holding.js
// Pick one NFT from metadata and insert a dummy holding row for a test wallet

import { pgQuery } from './db.js';

const TEST_WALLET = '0x379c2a0e88d8081f'.toLowerCase();

async function run() {
  try {
    // Grab one random-ish NFT from metadata
    const metaRes = await pgQuery(
      'SELECT nft_id FROM nft_core_metadata ORDER BY nft_id LIMIT 1'
    );

    if (metaRes.rowCount === 0) {
      console.error('No metadata rows found, run etl_metadata first.');
      process.exit(1);
    }

    const nftId = metaRes.rows[0].nft_id;
    console.log('Using nft_id:', nftId, 'for dummy holding');

    // Insert into wallet_holdings
    const now = new Date().toISOString();

    await pgQuery(
      `
      INSERT INTO wallet_holdings (
        wallet_address,
        nft_id,
        is_locked,
        last_event_ts
      ) VALUES ($1, $2, $3, $4)
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked    = EXCLUDED.is_locked,
        last_event_ts = EXCLUDED.last_event_ts
      ;
      `,
      [TEST_WALLET, nftId, false, now]
    );

    console.log('Dummy holding inserted for wallet', TEST_WALLET);
    process.exit(0);
  } catch (err) {
    console.error('insert_dummy_holding FAILED:', err);
    process.exit(1);
  }
}

run();
