// server.js
import express from "express";
import cors from "cors";
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { pgQuery } from "./db.js";
import fetch from "node-fetch";
import pool from "./db/pool.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ---- Snowflake connection ----
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE
});

let snowflakeConnected = false;

function ensureSnowflakeConnected() {
  return new Promise((resolve, reject) => {
    if (snowflakeConnected) return resolve();

    connection.connect((err, conn) => {
      if (err) {
        console.error("Snowflake connect error:", err);
        return reject(err);
      }
      console.log("Snowflake connected as", conn.getId());
      snowflakeConnected = true;
      resolve();
    });
  });
}

// simple alias so other code can call ensureConnected()
function ensureConnected() {
  return ensureSnowflakeConnected();
}

// ---- Load base SQL (with your original wallet baked in) ----
const sqlPath = path.join(__dirname, "NFLAllDayWalletGrab.sql");
const baseSql = fs.readFileSync(sqlPath, "utf8");

// Helper to swap the hard-coded wallet with the one from the request
function buildSqlForWallet(wallet) {
  const normalized = wallet.toLowerCase().trim();

  // Super basic validation to avoid injection
  if (!/^0x[0-9a-f]{4,64}$/.test(normalized)) {
    throw new Error("Invalid wallet address format");
  }

  // Replace every occurrence of your original wallet with the requested one
  // (you had 0x7541bafd155b683e in the SQL)
  const updatedSql = baseSql.replace(/0x7541bafd155b683e/gi, normalized);

  return updatedSql;
}

function executeSql(sqlText) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      complete(err, stmt, rows) {
        if (err) {
          console.error("Snowflake query error:", err);
          return reject(err);
        }
        console.log("Snowflake query executed, row count:", rows.length);
        resolve(rows);
      }
    });
  });
}

// ---- API route: /api/collection?wallet=0x... ----
app.get("/api/collection", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    await ensureSnowflakeConnected();
    const sqlText = buildSqlForWallet(wallet);
    const rows = await executeSql(sqlText);

    res.json({
      ok: true,
      wallet: wallet.toLowerCase(),
      count: rows.length,
      rows
    });
  } catch (err) {
    console.error("Error in /api/collection:", err);
    res.status(500).json({
      ok: false,
      error: err && err.message ? err.message : String(err)
    });
  }
});

// GET /api/top-wallets?limit=50
// Returns top wallets by moment count from wallet_holdings
app.get("/api/top-wallets", async (req, res) => {
  try {
    const rawLimit = parseInt(req.query.limit, 10);
    const limit = Number.isNaN(rawLimit) ? 50 : rawLimit;
    const safeLimit = Math.min(Math.max(limit, 1), 200); // 1–200

    const result = await pgQuery(
      `
      SELECT
        h.wallet_address,
        h.is_locked,
        h.last_event_ts,
        m.nft_id,
        m.edition_id,
        m.play_id,

        -- edition-level metadata
        e.series_id,
        e.set_id,
        e.tier,
        e.max_mint_size,
        e.series_name,
        e.set_name

        -- placeholders for player fields for now
        ,
        NULL::text  AS first_name,
        NULL::text  AS last_name,
        NULL::text  AS team_name,
        NULL::text  AS position,
        NULL::int   AS jersey_number,
        m.serial_number
      FROM wallet_holdings h
      JOIN moments m
        ON m.nft_id = h.nft_id
      LEFT JOIN editions e
        ON e.edition_id = m.edition_id
      WHERE h.wallet_address = $1
      ORDER BY h.last_event_ts DESC
      `,
      [wallet]
    );

    return res.json({
      ok: true,
      limit: safeLimit,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/top-wallets:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// ---- ASP / low-ask helpers (still using Postgres + scrape) ----
async function getASP(editionIds) {
  if (!editionIds.length) return {};

  // Query local Postgres table filled by etl_edition_prices.js
  const res = await pgQuery(
    `
    SELECT edition_id, asp_90d
    FROM edition_price_stats
    WHERE edition_id = ANY($1::text[])
    `,
    [editionIds]
  );

  const map = {};
  for (const row of res.rows) {
    if (row.asp_90d != null) {
      map[row.edition_id] = Number(row.asp_90d);
    }
  }
  return map;
}

async function getLowAsks(editionIds) {
  if (!process.env.ENABLE_MARKET_SCRAPE) return {};
  const map = {};

  for (const id of editionIds) {
    try {
      const res = await fetch(`https://nflallday.com/listing/moment/${id}`, {
        headers: { "user-agent": "Mozilla/5.0" }
      });
      const html = await res.text();

      // try to find a dollar amount that looks like the lowest ask
      const m =
        html.match(/(?:lowestAsk|low(?:est)?\s*ask)[^0-9$]*\$?\s*(\d[\d,]*(?:\.\d{1,2})?)/) ||
        html.match(/\$\s*(\d[\d,]*(?:\.\d{1,2})?)/);

      if (m) {
        const num = Number(String(m[1]).replace(/[^0-9.]/g, ""));
        if (!Number.isNaN(num)) {
          map[id] = num;
        }
      }
    } catch {
      // ignore scrape errors for individual editions
    }
  }

  return map;
}

app.get("/api/prices", async (req, res) => {
  try {
    const list = String(req.query.editions || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const unique = [...new Set(list)];
    if (!unique.length) {
      return res.json({ ok: true, asp: {}, lowAsk: {} });
    }

    const result = await pgQuery(
      `
      SELECT
        edition_id,
        asp_90d,
        low_ask
      FROM edition_price_stats
      WHERE edition_id = ANY($1::text[])
      `,
      [unique]
    );

    const asp = {};
    const lowAsk = {};

    for (const row of result.rows) {
      if (row.asp_90d != null) {
        asp[row.edition_id] = Number(row.asp_90d);
      }
      if (row.low_ask != null) {
        lowAsk[row.edition_id] = Number(row.low_ask);
      }
    }

    return res.json({ ok: true, asp, lowAsk });
  } catch (err) {
    console.error("Error in /api/prices:", err);
    return res.status(500).json({ ok: false, error: err.message || String(err) });
  }
});

// ---- NEW: /api/query now uses Neon schema (no nft_core_metadata) ----
// ---- Wallet query using Neon (moments + editions) ----
app.get("/api/query", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) {
      return res
        .status(400)
        .json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic wallet format check
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res
        .status(400)
        .json({ ok: false, error: "Invalid wallet format" });
    }

    const result = await pgQuery(
      `
      SELECT
        h.wallet_address,
        h.is_locked,
        h.last_event_ts,
        m.nft_id,
        m.edition_id,
        m.play_id,
        -- edition-level metadata
        e.series_id,
        e.set_id,
        e.tier,
        e.max_mint_size,
        e.series_name,
        e.set_name,
        e.first_name,
        e.last_name,
        e.team_name,
        e.position,
        e.jersey_number,
        -- moment-level
        m.serial_number
      FROM wallet_holdings h
      JOIN moments m
        ON m.nft_id = h.nft_id
      LEFT JOIN editions e
        ON e.edition_id = m.edition_id
      WHERE h.wallet_address = $1
      ORDER BY h.last_event_ts DESC
      `,
      [wallet]
    );

    return res.json({
      ok: true,
      wallet,
      count: result.rowCount,
      rows: result.rows,
    });
  } catch (err) {
    console.error("Error in /api/query:", err);
    return res
      .status(500)
      .json({ ok: false, error: err.message || String(err) });
  }
});


// ---- Neon test route ----
app.get("/api/test-neon", async (req, res) => {
  try {
    const client = await pool.connect();

    try {
      const { rows } = await client.query(
        `SELECT nft_id, edition_id, play_id, serial_number, current_owner
         FROM moments
         ORDER BY nft_id
         LIMIT 10`
      );

      res.json({
        count: rows.length,
        moments: rows
      });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Error in /api/test-neon:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// ---- Start server ----
const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`NFL ALL DAY collection viewer running on http://localhost:${port}`);
});
