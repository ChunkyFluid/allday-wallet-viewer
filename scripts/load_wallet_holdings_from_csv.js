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

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function loadWalletHoldings() {
    const client = await pool.connect();
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "wallet_holdings.csv");

    console.log("Looking for CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error("❌ wallet_holdings.csv not found. Create data/wallet_holdings.csv first.");
        client.release();
        await pool.end();
        process.exit(1);
    }

    console.log("Found wallet_holdings.csv, starting load...");

    const stream = fs.createReadStream(filePath).pipe(
        parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        })
    );

    try {
        await client.query("BEGIN");

        let count = 0;

        for await (const row of stream) {
            const { wallet_address, nft_id, acquired_at, disposition } = row;

            if (!wallet_address || !nft_id) {
                console.warn("Skipping row with missing wallet_address or nft_id:", row);
                continue;
            }

            // Upsert into wallets
            const ts = acquired_at && acquired_at.trim() !== "" ? acquired_at : new Date().toISOString();

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
                [wallet_address.toLowerCase(), ts]
            );

            // Upsert into wallet_holdings
            await client.query(
                `
        INSERT INTO wallet_holdings (
          wallet_address,
          nft_id,
          acquired_at,
          disposition,
          last_updated_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (wallet_address, nft_id)
        DO UPDATE SET
          acquired_at = COALESCE(EXCLUDED.acquired_at, wallet_holdings.acquired_at),
          disposition = COALESCE(EXCLUDED.disposition, wallet_holdings.disposition),
          last_updated_at = NOW()
        `,
                [
                    wallet_address.toLowerCase(),
                    nft_id,
                    acquired_at && acquired_at.trim() !== "" ? acquired_at : null,
                    disposition && disposition.trim() !== "" ? disposition : "owned"
                ]
            );

            count += 1;
            if (count % 100 === 0) {
                console.log(`Inserted/updated ${count} wallet holdings...`);
            }
        }

        await client.query("COMMIT");
        console.log(`✅ Done. Total wallet holdings inserted/updated: ${count}`);
    } catch (err) {
        console.error("❌ Error loading wallet holdings, rolling back:", err);
        try {
            await client.query("ROLLBACK");
        } catch (rollbackErr) {
            console.error("Rollback failed:", rollbackErr);
        }
    } finally {
        client.release();
        await pool.end();
    }
}

loadWalletHoldings().catch((err) => {
    console.error("Unexpected top-level error:", err);
    process.exit(1);
});
