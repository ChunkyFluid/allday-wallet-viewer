// export_wallet_snowflake.js
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---- 1. Wallet arg & basic validation ----
const wallet = (process.argv[2] || "").trim();

if (!wallet) {
  console.error("Usage: node export_wallet_snowflake.js 0xYourWalletHere");
  process.exit(1);
}

if (!/^0x[0-9a-fA-F]{4,64}$/.test(wallet)) {
  console.error("Invalid wallet address:", wallet);
  process.exit(1);
}

// ---- 2. Load SQL template and inject wallet ----
const sqlTemplatePath = path.join(__dirname, "wallet_query.sql");
if (!fs.existsSync(sqlTemplatePath)) {
  console.error("wallet_query.sql not found in project root.");
  process.exit(1);
}

const sqlTemplate = fs.readFileSync(sqlTemplatePath, "utf8");
// we always pass lowercase into the template
const sqlText = sqlTemplate.replace(/{{WALLET}}/g, wallet.toLowerCase());

// ---- 3. Snowflake connection helpers ----
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
  password: process.env.SNOWFLAKE_PASSWORD
});

function connectSnowflake() {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) return reject(err);
      resolve(conn);
    });
  });
}

function executeQuery(sql) {
  return new Promise((resolve, reject) => {
    const statement = connection.execute({ sqlText: sql });
    const rows = [];
    const stream = statement.streamRows();

    stream.on("data", (row) => rows.push(row));
    stream.on("error", (err) => reject(err));
    stream.on("end", () => resolve(rows));
  });
}

// ---- 4. CSV helpers ----
function rowsToCsv(rows) {
  if (!rows.length) return "";

  const headers = Object.keys(rows[0]);
  const escapeCell = (val) => {
    if (val === null || val === undefined) return "";
    const s = String(val);
    if (s.includes('"') || s.includes(",") || s.includes("\n")) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };

  const lines = [];
  lines.push(headers.join(","));
  for (const row of rows) {
    const line = headers.map((h) => escapeCell(row[h])).join(",");
    lines.push(line);
  }

  return lines.join("\n");
}

// ---- 5. Main ----
(async () => {
  try {
    console.log("Connecting to Snowflake...");
    await connectSnowflake();

    console.log("Running query for wallet", wallet, "...");
    const rows = await executeQuery(sqlText);
    console.log(`Got ${rows.length} rows.`);

    if (!rows.length) {
      console.log("No moments found for this wallet (according to Snowflake query).");
      return;
    }

    const csv = rowsToCsv(rows);
    const outName = path.join(__dirname, `wallet_snowflake_${wallet.toLowerCase()}.csv`);
    fs.writeFileSync(outName, csv, "utf8");
    console.log("Wrote", outName);
  } catch (err) {
    console.error("Error:", err);
    process.exit(1);
  } finally {
    try {
      connection.destroy();
    } catch {
      // ignore
    }
  }
})();
