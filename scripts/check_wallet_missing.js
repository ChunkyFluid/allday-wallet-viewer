/**
 * Check a specific wallet for missing dates
 */
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const { Pool } = pg;
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    const wallet = '0x7541bafd155b683e';
    const result = await pool.query(
        "SELECT COUNT(*) as count FROM wallet_holdings WHERE wallet_address = $1 AND last_event_ts IS NULL",
        [wallet]
    );
    console.log(`Wallet ${wallet} is missing ${result.rows[0].count} dates.`);
    await pool.end();
}
main();
