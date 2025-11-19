// scripts/sync_wallet_profiles_from_dapper.js
import * as dotenv from "dotenv";
import fetch from "node-fetch";
import { pgQuery } from "../db.js";

dotenv.config();

const BATCH_LIMIT = 5000;         // how many wallets to process per run
const REQUEST_DELAY_MS = 200;     // ~5 requests/second to avoid hammering

async function ensureWalletProfilesTable() {
  console.log("Ensuring Neon wallet_profiles table exists...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_profiles (
      wallet_address TEXT PRIMARY KEY,
      display_name   TEXT,
      source         TEXT,
      last_checked   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  const before = await pgQuery(`SELECT COUNT(*) AS c FROM wallet_profiles;`);
  console.log(
    "Current Neon wallet_profiles row count (before sync):",
    before.rows[0].c
  );
}

async function getWalletsToProcess(limit) {
  console.log("Selecting wallets that don't have a profile yet...");
  const result = await pgQuery(
    `
    SELECT wh.wallet_address
    FROM wallet_holdings wh
    LEFT JOIN wallet_profiles p
      ON p.wallet_address = wh.wallet_address
    WHERE p.wallet_address IS NULL
    GROUP BY wh.wallet_address
    ORDER BY wh.wallet_address
    LIMIT $1;
    `,
    [limit]
  );

  const wallets = result.rows.map((r) => r.wallet_address.toLowerCase());
  console.log(`Found ${wallets.length} wallets to fetch profiles for.`);
  return wallets;
}

async function fetchProfileFromDapper(walletAddress) {
  const url = `https://open.meetdapper.com/profile?address=${walletAddress}`;
  try {
    const res = await fetch(url, {
      headers: {
        "user-agent": "allday-wallet-viewer/1.0",
        "accept": "application/json",
      },
    });

    if (!res.ok) {
      console.log(
        `Profile HTTP ${res.status} for ${walletAddress} (treating as no name)`
      );
      return null;
    }

    let data;
    try {
      data = await res.json();
    } catch (e) {
      console.log(`Non-JSON response for ${walletAddress}, skipping name.`);
      return null;
    }

    if (!data || typeof data.displayName !== "string" || !data.displayName) {
      console.log(`No displayName for ${walletAddress}`);
      return null;
    }

    return data.displayName;
  } catch (err) {
    console.error(`Fetch error for ${walletAddress}:`, err.message);
    return null;
  }
}

async function upsertProfile(walletAddress, displayName) {
  await pgQuery(
    `
    INSERT INTO wallet_profiles (wallet_address, display_name, source, last_checked)
    VALUES ($1, $2, 'dapper', now())
    ON CONFLICT (wallet_address) DO UPDATE SET
      display_name = EXCLUDED.display_name,
      source       = EXCLUDED.source,
      last_checked = EXCLUDED.last_checked;
    `,
    [walletAddress.toLowerCase(), displayName]
  );
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("=== Sync wallet profiles from Dapper -> Neon ===");
  await ensureWalletProfilesTable();

  const wallets = await getWalletsToProcess(BATCH_LIMIT);
  if (!wallets.length) {
    console.log("No new wallets to process. Done.");
    return;
  }

  let processed = 0;

  for (const wallet of wallets) {
    const name = await fetchProfileFromDapper(wallet);
    await upsertProfile(wallet, name);
    processed++;

    if (processed % 100 === 0) {
      console.log(`Processed ${processed}/${wallets.length} wallets...`);
    }

    // be nice to Dapper; ~5 req/sec
    await delay(REQUEST_DELAY_MS);
  }

  console.log(`✅ Done. Total wallets processed this run: ${processed}`);

  const after = await pgQuery(`SELECT COUNT(*) AS c FROM wallet_profiles;`);
  console.log("Final wallet_profiles row count:", after.rows[0].c);
}

main()
  .then(() => {
    console.log("✅ wallet_profiles sync complete.");
    process.exit(0);
  })
  .catch((err) => {
    console.error("💥 Fatal error during wallet_profiles sync:", err);
    process.exit(1);
  });
