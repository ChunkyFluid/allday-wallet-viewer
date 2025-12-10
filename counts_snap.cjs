const { Client } = require('pg');
const conn = 'postgresql://chunky_wallet_viewer_user:MEJwFMG0B97OTlObqwRrS4SEyyLzfY9p@dpg-d4q1qnshg0os7380pec0-a.ohio-postgres.render.com/chunky_wallet_viewer';
const client = new Client({ connectionString: conn, ssl: { rejectUnauthorized: false } });
(async () => {
  await client.connect();
  const qs = [
    'SELECT COUNT(*) AS top_wallets_snapshot FROM top_wallets_snapshot',
    'SELECT COUNT(*) AS top_wallets_by_team_snapshot FROM top_wallets_by_team_snapshot',
    'SELECT COUNT(*) AS top_wallets_by_tier_snapshot FROM top_wallets_by_tier_snapshot',
    'SELECT COUNT(*) AS top_wallets_by_value_snapshot FROM top_wallets_by_value_snapshot',
    'SELECT wallet_address, display_name, total_moments FROM top_wallets_snapshot ORDER BY total_moments DESC LIMIT 5'
  ];
  for (const sql of qs) {
    const res = await client.query(sql);
    console.log(sql, res.rows);
  }
  await client.end();
})();
