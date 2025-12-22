import { pgQuery } from './db.js';
const ADDRESS = '0xcfd9bad75352b43b';

async function checkLocal() {
    const walletHoldings = await pgQuery(`
    SELECT is_locked, count(*) 
    FROM wallet_holdings 
    WHERE wallet_address = $1 
    GROUP BY is_locked
  `, [ADDRESS]);
    console.log('WALLET_HOLDINGS:', JSON.stringify(walletHoldings.rows));

    const holdings = await pgQuery(`
    SELECT count(*) FROM holdings WHERE wallet_address = $1
  `, [ADDRESS]);
    console.log('HOLDINGS:', holdings.rows[0].count);
    process.exit(0);
}
checkLocal();
