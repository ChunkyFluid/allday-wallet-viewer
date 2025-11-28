// scripts/debug_wallet_csv.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parse } from "csv-parse";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvPath = path.join(__dirname, "..", "data", "wallet_holdings.csv");

// your dapper wallet
const TARGET_WALLET = "0x7541bafd155b683e";

(async () => {
    if (!fs.existsSync(csvPath)) {
        console.error("CSV not found at:", csvPath);
        process.exit(1);
    }

    console.log("Reading:", csvPath);
    console.log("Target wallet:", TARGET_WALLET);

    let total = 0;
    let forWallet = 0;

    const parser = fs.createReadStream(csvPath).pipe(
        parse({
            columns: (header) => header.map((h) => h.toLowerCase().trim()),
            skip_empty_lines: true,
            trim: true
        })
    );

    for await (const row of parser) {
        total++;
        const addr = (row.wallet_address || "").toString().trim().toLowerCase();
        if (addr === TARGET_WALLET.toLowerCase()) {
            forWallet++;
        }
    }

    console.log("Total rows in CSV:", total);
    console.log(`Rows for ${TARGET_WALLET}:`, forWallet);
})();
