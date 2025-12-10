const { Client } = require('pg');
const conn = 'postgresql://chunky_wallet_viewer_user:MEJwFMG0B97OTlObqwRrS4SEyyLzfY9p@dpg-d4q1qnshg0os7380pec0-a.ohio-postgres.render.com/chunky_wallet_viewer';
const wallet = '0x7541bafd155b683e';

const client = new Client({ connectionString: conn, ssl: { rejectUnauthorized: false } });

(async () => {
  await client.connect();
  const queries = [
    'SELECT COUNT(*) AS wallet_holdings FROM wallet_holdings',
    `SELECT COUNT(*) AS wallet_holdings_wallet FROM wallet_holdings WHERE wallet_address='${wallet}'`,
    'SELECT COUNT(*) AS top_wallets_snapshot FROM top_wallets_snapshot',
    'SELECT COUNT(*) AS edition_price_scrape FROM edition_price_scrape',
    'SELECT wallet_address, display_name, total_moments FROM top_wallets_snapshot ORDER BY total_moments DESC LIMIT 5'
  ];
  for (const sql of queries) {
    const res = await client.query(sql);
    console.log(sql, res.rows);
  }
  await client.end();
  process.exit(0);
})().catch(err => { console.error(err); process.exit(1); });
