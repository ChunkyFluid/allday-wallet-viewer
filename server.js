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
// Returns top wallets by moment count from wallet_holdings,
// with optional display_name from wallet_profiles.
app.get("/api/top-wallets", async (req, res) => {
  try {
    const rawLimit = parseInt(req.query.limit, 10);
    const limit = Number.isNaN(rawLimit) ? 50 : rawLimit;
    const safeLimit = Math.min(Math.max(limit, 1), 200); // 1–200

    const result = await pgQuery(
      `
      SELECT
        h.wallet_address,
        COALESCE(p.display_name, NULL) AS display_name,
        COUNT(*)::int AS moments
      FROM wallet_holdings h
      LEFT JOIN wallet_profiles p
        ON p.wallet_address = h.wallet_address
      GROUP BY h.wallet_address, p.display_name
      ORDER BY moments DESC
      LIMIT $1;
      `,
      [safeLimit]
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

// GET /api/wallet-profile?wallet=0x...
// Returns display_name (if any) from wallet_profiles in Neon
app.get("/api/wallet-profile", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    const result = await pgQuery(
      `
      SELECT wallet_address, display_name
      FROM wallet_profiles
      WHERE wallet_address = $1
      `,
      [wallet]
    );

    const profile = result.rows[0] || null;

    return res.json({
      ok: true,
      wallet,
      profile
    });
  } catch (err) {
    console.error("Error in /api/wallet-profile:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// GET /api/search-profiles?q=chunk
// Simple lookup by Dapper display name in wallet_profiles.
app.get("/api/search-profiles", async (req, res) => {
  try {
    const qRaw = (req.query.q || "").toString().trim();
    if (!qRaw) {
      return res.status(400).json({ ok: false, error: "Missing ?q= search term" });
    }

    const pattern = `%${qRaw}%`;
    const result = await pgQuery(
      `
      SELECT wallet_address, display_name
      FROM wallet_profiles
      WHERE display_name ILIKE $1
      ORDER BY display_name ASC, wallet_address ASC
      LIMIT 50;
      `,
      [pattern]
    );

    return res.json({
      ok: true,
      query: qRaw,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/search-profiles:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// GET /api/top-holders?edition=1717
// Returns wallets that hold a given edition_id, ordered by copies desc.
app.get("/api/top-holders", async (req, res) => {
  try {
    const edition = (req.query.edition || "").toString().trim();
    if (!edition) {
      return res.status(400).json({ ok: false, error: "Missing ?edition=" });
    }

    const result = await pgQuery(
      `
      SELECT
        m.edition_id,
        h.wallet_address,
        COALESCE(p.display_name, NULL) AS display_name,
        COUNT(*)::int AS copies,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
        COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count
      FROM wallet_holdings h
      JOIN nft_core_metadata m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      WHERE m.edition_id = $1
      GROUP BY m.edition_id, h.wallet_address, p.display_name
      ORDER BY copies DESC, h.wallet_address ASC;
      `,
      [edition]
    );

    return res.json({
      ok: true,
      edition,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/top-holders:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// GET /api/search-moments
// Search moments across nft_core_metadata by player, team, tier, series, set, etc.
app.get("/api/search-moments", async (req, res) => {
  try {
    const playerRaw = (req.query.player || req.query.q || "").toString().trim();
    const teamRaw   = (req.query.team   || "").toString().trim();
    const tierRaw   = (req.query.tier   || "").toString().trim();
    const seriesRaw = (req.query.series || "").toString().trim();
    const setRaw    = (req.query.set    || "").toString().trim();
    const limitRaw  = parseInt(req.query.limit, 10);

    const safeLimit = Number.isNaN(limitRaw) ? 200 : Math.min(Math.max(limitRaw, 10), 500);

    const filters = [];
    const params = [];
    let idx = 1;

    if (playerRaw) {
      // Match "first last" or last name alone
      filters.push(
        "(LOWER(COALESCE(m.first_name,'') || ' ' || COALESCE(m.last_name,'')) LIKE $" + idx + " OR " +
        "LOWER(COALESCE(m.last_name,'')) LIKE $" + idx + ")"
      );
      params.push(`%${playerRaw.toLowerCase()}%`);
      idx++;
    }

    if (teamRaw) {
      filters.push("LOWER(m.team_name) LIKE $" + idx);
      params.push(`%${teamRaw.toLowerCase()}%`);
      idx++;
    }

    if (tierRaw) {
      filters.push("UPPER(m.tier) = $" + idx);
      params.push(tierRaw.toUpperCase());
      idx++;
    }

    if (seriesRaw) {
      filters.push("LOWER(m.series_name) LIKE $" + idx);
      params.push(`%${seriesRaw.toLowerCase()}%`);
      idx++;
    }

    if (setRaw) {
      filters.push("LOWER(m.set_name) LIKE $" + idx);
      params.push(`%${setRaw.toLowerCase()}%`);
      idx++;
    }

    const whereClause = filters.length ? `WHERE ${filters.join(" AND ")}` : "";

    const sql = `
      SELECT
        m.nft_id,
        m.edition_id,
        m.play_id,
        m.series_id,
        m.set_id,
        m.tier,
        m.serial_number,
        m.max_mint_size,
        m.first_name,
        m.last_name,
        m.team_name,
        m.position,
        m.jersey_number,
        m.series_name,
        m.set_name
      FROM nft_core_metadata m
      ${whereClause}
      ORDER BY m.nft_id::bigint
      LIMIT $${idx};
    `;

    params.push(safeLimit);

    const result = await pgQuery(sql, params);

    return res.json({
      ok: true,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/search-moments:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});


// ---- Wallet summary: stats + Dapper display name ----
app.get("/api/wallet-summary", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();

    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic format check (same as /api/query)
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // 1) Get profile (Dapper display name) if we have it
    const profileResult = await pgQuery(
      `
      SELECT wallet_address, display_name
      FROM wallet_profiles
      WHERE wallet_address = $1
      LIMIT 1;
      `,
      [wallet]
    );

    const profileRow = profileResult.rows[0] || null;

    // 2) Get counts and tier breakdown from Neon
    //    IMPORTANT: use nft_core_metadata for tier (same as /api/query)
    const statsResult = await pgQuery(
      `
      SELECT
        h.wallet_address,
        COUNT(*)::int AS moments_total,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
        COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int     AS common_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int   AS uncommon_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int       AS rare_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int  AS legendary_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int   AS ultimate_count
      FROM wallet_holdings h
      JOIN nft_core_metadata m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      GROUP BY h.wallet_address;
      `,
      [wallet]
    );

    const statsRow = statsResult.rows[0] || null;

    const stats = statsRow
      ? {
          momentsTotal: statsRow.moments_total,
          lockedCount: statsRow.locked_count,
          unlockedCount: statsRow.unlocked_count,
          byTier: {
            Common: statsRow.common_count,
            Uncommon: statsRow.uncommon_count,
            Rare: statsRow.rare_count,
            Legendary: statsRow.legendary_count,
            Ultimate: statsRow.ultimate_count
          }
        }
      : {
          momentsTotal: 0,
          lockedCount: 0,
          unlockedCount: 0,
          byTier: {
            Common: 0,
            Uncommon: 0,
            Rare: 0,
            Legendary: 0,
            Ultimate: 0
          }
        };

    return res.json({
      ok: true,
      wallet,
      displayName: profileRow ? profileRow.display_name : null,
      stats
    });
  } catch (err) {
    console.error("Error in /api/wallet-summary:", err);
    return res.status(500).json({ ok: false, error: err.message || String(err) });
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

// ---- MAIN WALLET QUERY: rows for the table ----
app.get("/api/query", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic Flow/Dapper-style address check
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
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
        m.series_id,
        m.set_id,
        m.tier,
        m.serial_number,
        m.max_mint_size,
        m.first_name,
        m.last_name,
        m.team_name,
        m.position,
        m.jersey_number,
        m.series_name,
        m.set_name
      FROM wallet_holdings h
      JOIN nft_core_metadata m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      ORDER BY h.last_event_ts DESC;
      `,
      [wallet]
    );

    return res.json({
      ok: true,
      wallet,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/query (Neon):", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
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
