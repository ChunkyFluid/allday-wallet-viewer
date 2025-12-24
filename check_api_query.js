// Check what the API would return for this wallet
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // This is the exact query the /api/wallet-summary uses
    const result = await pgQuery(`
      SELECT
        h.wallet_address,
        COUNT(*)::int AS moments_total,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
        COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count
      FROM holdings h
      WHERE h.wallet_address = $1
      GROUP BY h.wallet_address
    `, [WALLET]);

    console.log('API would show for holdings table:');
    console.table(result.rows);

    // Compare with wallet_holdings
    const legacy = await pgQuery(`
      SELECT
        h.wallet_address,
        COUNT(*)::int AS moments_total,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
        COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count
      FROM wallet_holdings h
      WHERE h.wallet_address = $1
      GROUP BY h.wallet_address
    `, [WALLET]);

    console.log('Legacy wallet_holdings would show:');
    console.table(legacy.rows);

    // Check the first 5 newest in holdings
    const newest = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number, h.acquired_at
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
        ORDER BY h.acquired_at DESC NULLS LAST
        LIMIT 5
    `, [WALLET]);
    console.log('5 newest NFTs in holdings by acquired_at:');
    console.table(newest.rows);

    process.exit(0);
}
check();
