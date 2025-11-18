// scripts/load_wallet_holdings_from_csv.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";
import dotenv from "dotenv";
import { parse } from "csv-parse";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
  host: process.env.PGHOST,
  port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  ssl: { rejectUnauthorized: false }
});

pool.on("error", (err) => {
  console.error("Postgres pool error:", err);
});

function fileExists(filePath) {
  try {
    fs.accessSync(filePath, fs.constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function loadWalletHoldings() {
  const dataDir = path.join(__dirname, "..", "data");
  const filePath = path.join(dataDir, "wallet_holdings.csv");

  console.log("Looking for CSV at:", filePath);

  if (!fileExists(filePath)) {
    console.error("❌ wallet_holdings.csv not found in /data.");
    await pool.end();
    process.exit(1);
  }

  console.log(
    "Found wallet_holdings.csv, starting load (row-by-row, headers forced to lowercase)..."
  );

  const client = await pool.connect();

  const stream = fs.createReadStream(filePath).pipe(
    parse({
      columns(header) {
        const cols = header.map((h) => String(h).trim().toLowerCase());
        console.log("CSV header columns (wallet_holdings):", cols);
        return cols;
      },
      skip_empty_lines: true,
      trim: true
    })
  );

  let inserted = 0;
  let skippedMissingKeys = 0;
  let skippedMissingMoment = 0;
  let otherFailures = 0;

  try {
    for await (const row of stream) {
      const walletAddressRaw = row.wallet_address;
      const nftId = row.nft_id;
      const isLockedRaw = row.is_locked;
      const lastEventTsRaw = row.last_event_ts;

      if (!walletAddressRaw || !nftId) {
        skippedMissingKeys += 1;
        console.warn(
          "Skipping row with missing wallet_address or nft_id:",
          row
        );
        continue;
      }

      const walletAddress = String(walletAddressRaw).toLowerCase();

      // Normalize is_locked -> boolean (default false)
      let isLocked = false;
      if (typeof isLockedRaw === "string") {
        const v = isLockedRaw.trim().toLowerCase();
        isLocked = v === "true" || v === "1" || v === "t" || v === "yes";
      } else if (typeof isLockedRaw === "boolean") {
        isLocked = isLockedRaw;
      }

      // Normalize timestamp; let Postgres parse the string
      const lastEventTs =
        lastEventTsRaw && String(lastEventTsRaw).trim() !== ""
          ? String(lastEventTsRaw).trim()
          : null;

      const tsForWallet = lastEventTs || new Date().toISOString();

      try {
        // Upsert wallet
        await client.query(
          `
          INSERT INTO wallets (
            wallet_address,
            username,
            first_seen_at,
            last_seen_at,
            updated_at
          )
          VALUES ($1, NULL, $2, $2, NOW())
          ON CONFLICT (wallet_address)
          DO UPDATE SET
            last_seen_at = GREATEST(wallets.last_seen_at, EXCLUDED.last_seen_at),
            updated_at = NOW()
          `,
          [walletAddress, tsForWallet]
        );

        // Upsert holding – may fail if nft_id not in moments
        await client.query(
          `
          INSERT INTO wallet_holdings (
            wallet_address,
            nft_id,
            is_locked,
            last_event_ts
          )
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (wallet_address, nft_id)
          DO UPDATE SET
            is_locked     = EXCLUDED.is_locked,
            last_event_ts = EXCLUDED.last_event_ts
          `,
          [walletAddress, nftId, isLocked, lastEventTs]
        );

        inserted += 1;
        if (inserted % 100 === 0) {
          console.log(`Inserted/updated ${inserted} wallet holdings...`);
        }
      } catch (err) {
        // Foreign-key violation: nft_id not present in moments
        if (
          err.code === "23503" &&
          err.constraint === "wallet_holdings_nft_id_fkey"
        ) {
          skippedMissingMoment += 1;
          console.warn(
            `Skipping holding for wallet=${walletAddress}, nft_id=${nftId}: no matching row in moments.`
          );
          continue;
        }

        otherFailures += 1;
        console.error(
          `Row failed for wallet=${walletAddress}, nft_id=${nftId}: ${
            err.code || ""
          } ${err.message || String(err)}`
        );
      }
    }

    console.log("==========================================");
    console.log(
      `✅ Done. Wallet holdings inserted/updated: ${inserted},` +
        ` skipped_missing_moment: ${skippedMissingMoment},` +
        ` skipped_missing_keys: ${skippedMissingKeys},` +
        ` other_failures: ${otherFailures}`
    );
    console.log("==========================================");
  } finally {
    client.release();
    await pool.end();
  }
}

loadWalletHoldings().catch((err) => {
  console.error("Unexpected top-level error:", err);
  process.exit(1);
});
