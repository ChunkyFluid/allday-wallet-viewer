import { pgQuery } from "./db.js";

async function testConnection() {
  try {
    console.log("Testing Postgres connection...");
    const res = await pgQuery("SELECT NOW()");
    console.log("Postgres connection successful:", res.rows[0]);
    
    console.log("Testing wallet_holdings count...");
    const countRes = await pgQuery("SELECT COUNT(*) FROM wallet_holdings");
    console.log("wallet_holdings count:", countRes.rows[0].count);
  } catch (err) {
    console.error("Connection test failed:", err);
  } finally {
    process.exit(0);
  }
}

testConnection();
