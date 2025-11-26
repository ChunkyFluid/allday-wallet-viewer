// server.js
import cors from "cors";
import * as dotenv from "dotenv";
import snowflake from "snowflake-sdk";
import fs from "fs";
import { pgQuery } from "./db.js";
import fetch from "node-fetch";
import pool from "./db/pool.js";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg"; // (still imported; fine even if unused)
import session from "express-session";
import bcrypt from "bcrypt";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();

// Basic middleware
app.use(cors());
app.use(express.json());

// Static files
app.use(express.static(path.join(__dirname, "public")));

// Sessions (must be before routes)
app.use(
  session({
    secret: process.env.SESSION_SECRET || "dev-secret-change-me",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      maxAge: 1000 * 60 * 60 * 24 * 7, // 7 days
      secure: false // set to true when behind HTTPS + proxy
    }
  })
);

// ------------------ Snowflake connection ------------------

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

function ensureConnected() {
  return ensureSnowflakeConnected();
}

// ------------------ Wallet SQL (Snowflake base query) ------------------

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

// ------------------ Auth helpers ------------------

function isValidEmail(email) {
  return typeof email === "string" && email.includes("@") && email.length <= 255;
}

// ------------------ Auth routes ------------------

// POST /api/signup  { email, password }
app.post("/api/signup", async (req, res) => {
  try {
    const { email, password } = req.body || {};

    if (!isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: "Invalid email address." });
    }
    if (!password || typeof password !== "string" || password.length < 8) {
      return res.status(400).json({
        ok: false,
        error: "Password must be at least 8 characters."
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const insertSql = `
      INSERT INTO public.users (email, password_hash)
      VALUES ($1, $2)
      ON CONFLICT (email) DO NOTHING
      RETURNING id, email, created_at;
    `;
    const { rows } = await pool.query(insertSql, [email.toLowerCase(), passwordHash]);

    if (!rows.length) {
      return res.status(409).json({ ok: false, error: "An account with that email already exists." });
    }

    const user = rows[0];

    // Create session
    req.session.user = {
      id: user.id,
      email: user.email
    };

    return res.json({
      ok: true,
      user: {
        id: user.id,
        email: user.email,
        created_at: user.created_at
      }
    });
  } catch (err) {
    console.error("POST /api/signup error:", err);
    return res.status(500).json({ ok: false, error: "Failed to create account." });
  }
});

// POST /api/login  { email, password }
app.post("/api/login", async (req, res) => {
  try {
    const { email, password } = req.body || {};

    if (!isValidEmail(email) || !password) {
      return res.status(400).json({ ok: false, error: "Email and password are required." });
    }

    const selectSql = `
      SELECT id, email, password_hash, default_wallet_address
      FROM public.users
      WHERE email = $1
      LIMIT 1;
    `;
    const { rows } = await pool.query(selectSql, [email.toLowerCase()]);

    if (!rows.length) {
      return res.status(401).json({ ok: false, error: "Invalid email or password." });
    }

    const user = rows[0];

    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      return res.status(401).json({ ok: false, error: "Invalid email or password." });
    }

    // Set session
    req.session.user = {
      id: user.id,
      email: user.email
    };

    return res.json({
      ok: true,
      user: {
        id: user.id,
        email: user.email,
        default_wallet_address: user.default_wallet_address || null
      }
    });
  } catch (err) {
    console.error("POST /api/login error:", err);
    return res.status(500).json({ ok: false, error: "Failed to log in." });
  }
});

// POST /api/logout
app.post("/api/logout", (req, res) => {
  if (!req.session) {
    return res.json({ ok: true });
  }
  req.session.destroy((err) => {
    if (err) {
      console.error("POST /api/logout error:", err);
      return res.status(500).json({ ok: false, error: "Failed to log out." });
    }
    res.clearCookie("connect.sid");
    return res.json({ ok: true });
  });
});

// POST /api/me/wallet  { wallet_address }
app.post("/api/me/wallet", async (req, res) => {
  try {
    const sessUser = req.session?.user;
    if (!sessUser || !sessUser.id) {
      console.log("POST /api/me/wallet: no session user", req.session);
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    let { wallet_address } = req.body || {};

    if (wallet_address && typeof wallet_address === "string") {
      wallet_address = wallet_address.trim();
      if (wallet_address && !wallet_address.startsWith("0x")) {
        wallet_address = "0x" + wallet_address;
      }
      wallet_address = wallet_address.toLowerCase();
      // simple sanity check (optional)
      if (!/^0x[0-9a-f]{4,64}$/.test(wallet_address)) {
        return res.status(400).json({ ok: false, error: "Invalid wallet format." });
      }
    } else {
      // Treat empty/undefined as "clear default"
      wallet_address = null;
    }

    const { rows } = await pool.query(
      `
      UPDATE public.users
      SET default_wallet_address = $1
      WHERE id = $2
      RETURNING id, email, default_wallet_address;
      `,
      [wallet_address, sessUser.id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "User not found" });
    }

    const u = rows[0];

    // Optionally update session (purely cosmetic)
    req.session.user = {
      id: u.id,
      email: u.email
    };

    return res.json({
      ok: true,
      default_wallet_address: u.default_wallet_address
    });
  } catch (err) {
    console.error("POST /api/me/wallet error", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ------------------ Existing API routes ------------------

// Snowflake collection endpoint
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

// Wallet profile
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

// Search profiles by display name prefix
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

// Top holders for a specific edition
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
      WHERE
        m.edition_id = $1
        AND h.wallet_address NOT IN (
          '0xe4cf4bdc1751c65d', -- NFL All Day contract
          '0xb6f2481eba4df97b'  -- huge custodial/system wallet
        )
      GROUP BY
        m.edition_id,
        h.wallet_address,
        p.display_name
      ORDER BY
        copies DESC,
        h.wallet_address ASC;
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

// Moment search (explorer)
app.get("/api/search-moments", async (req, res) => {
  try {
    const { player = "", team = "", tier = "", series = "", set = "", position = "", limit } = req.query;

    const rawLimit = parseInt(limit, 10);
    const safeLimit = Math.min(Math.max(rawLimit || 200, 1), 1000);

    const conditions = [];
    const params = [];
    let idx = 1;

    if (player) {
      conditions.push(`LOWER(first_name || ' ' || last_name) LIKE LOWER($${idx++})`);
      params.push(`%${player}%`);
    }

    if (team) {
      conditions.push(`team_name = $${idx++}`);
      params.push(team);
    }

    if (tier) {
      conditions.push(`tier = $${idx++}`);
      params.push(tier);
    }

    if (series) {
      conditions.push(`series_name = $${idx++}`);
      params.push(series);
    }

    if (set) {
      conditions.push(`set_name = $${idx++}`);
      params.push(set);
    }

    if (position) {
      conditions.push(`position = $${idx++}`);
      params.push(position);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    const sql = `
      SELECT
        nft_id,
        edition_id,
        play_id,
        serial_number,
        max_mint_size,
        first_name,
        last_name,
        team_name,
        position,
        tier,
        series_name,
        set_name
      FROM nft_core_metadata
      ${whereClause}
      ORDER BY last_name NULLS LAST, first_name NULLS LAST, nft_id
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

// Explorer filters
app.get("/api/explorer-filters", async (req, res) => {
  try {
    console.log("GET /api/explorer-filters – trying snapshot…");

    // 1) Fast path: snapshot table
    const snapRes = await pgQuery(
      `
      SELECT
        players,
        teams,
        series,
        sets,
        positions,
        tiers,
        updated_at
      FROM explorer_filters_snapshot
      WHERE id = 1
      LIMIT 1;
      `
    );

    if (snapRes.rowCount > 0) {
      const row = snapRes.rows[0];

      return res.json({
        ok: true,
        players: row.players || [],
        teams: row.teams || [],
        series: row.series || [],
        sets: row.sets || [],
        positions: row.positions || [],
        tiers: row.tiers || [],
        updatedAt: row.updated_at,
        fromSnapshot: true
      });
    }

    // 2) Fallback: live distincts (only if snapshot missing)
    console.log("explorer_filters_snapshot empty – falling back to live DISTINCT queries.");

    const [playersRes, teamsRes, seriesRes, setsRes, positionsRes, tiersRes] = await Promise.all([
      pgQuery(`
        SELECT DISTINCT
          COALESCE(first_name, '') AS first_name,
          COALESCE(last_name, '')  AS last_name
        FROM nft_core_metadata
        WHERE first_name IS NOT NULL
          AND first_name <> ''
          AND last_name IS NOT NULL
          AND last_name <> ''
        ORDER BY last_name, first_name
        LIMIT 5000;
      `),
      pgQuery(`
        SELECT DISTINCT team_name
        FROM nft_core_metadata
        WHERE team_name IS NOT NULL
          AND team_name <> ''
        ORDER BY team_name
        LIMIT 1000;
      `),
      pgQuery(`
        SELECT DISTINCT series_name
        FROM nft_core_metadata
        WHERE series_name IS NOT NULL
          AND series_name <> ''
        ORDER BY series_name
        LIMIT 1000;
      `),
      pgQuery(`
        SELECT DISTINCT set_name
        FROM nft_core_metadata
        WHERE set_name IS NOT NULL
          AND set_name <> ''
        ORDER BY set_name
        LIMIT 2000;
      `),
      pgQuery(`
        SELECT DISTINCT position
        FROM nft_core_metadata
        WHERE position IS NOT NULL
          AND position <> ''
        ORDER BY position
        LIMIT 100;
      `),
      pgQuery(`
        SELECT DISTINCT tier
        FROM nft_core_metadata
        WHERE tier IS NOT NULL
          AND tier <> ''
        ORDER BY tier
        LIMIT 20;
      `)
    ]);

    console.log("Explorer filters row counts (live):", {
      players: playersRes.rowCount,
      teams: teamsRes.rowCount,
      series: seriesRes.rowCount,
      sets: setsRes.rowCount,
      positions: positionsRes.rowCount,
      tiers: tiersRes.rowCount
    });

    const players = playersRes.rows.map((r) => ({
      first_name: r.first_name || "",
      last_name: r.last_name || ""
    }));

    const teams = teamsRes.rows.map((r) => r.team_name).filter(Boolean);

    const series = seriesRes.rows.map((r) => r.series_name).filter(Boolean);

    const sets = setsRes.rows.map((r) => r.set_name).filter(Boolean);

    const positions = positionsRes.rows.map((r) => r.position).filter(Boolean);

    const tiers = tiersRes.rows.map((r) => r.tier).filter(Boolean);

    return res.json({
      ok: true,
      players,
      teams,
      series,
      sets,
      positions,
      tiers,
      fromSnapshot: false
    });
  } catch (err) {
    console.error("Error in /api/explorer-filters:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// Wallet summary
app.get("/api/wallet-summary", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();

    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic Flow/Dapper-style address check
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // 1) Display name (from wallet_profiles)
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

    // 2) Stats + value based on edition_price_scrape (same source used by /api/prices)
    //
    // - We JOIN wallet_holdings -> nft_core_metadata on nft_id
    // - LEFT JOIN edition_price_scrape on edition_id
    // - Each row in wallet_holdings is ONE copy, so summing lowest_ask_usd/avg_sale_usd
    //   across rows naturally multiplies by number of copies.
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
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int   AS ultimate_count,

        -- floor_value = sum of lowest_ask_usd per *copy* (unknown prices treated as 0)
        COALESCE(
          SUM(
            CASE
              WHEN eps.lowest_ask_usd IS NOT NULL THEN eps.lowest_ask_usd
              ELSE 0
            END
          ),
          0
        )::numeric AS floor_value,

        -- asp_value = sum of avg_sale_usd per *copy*
        COALESCE(
          SUM(
            CASE
              WHEN eps.avg_sale_usd IS NOT NULL THEN eps.avg_sale_usd
              ELSE 0
            END
          ),
          0
        )::numeric AS asp_value,

        -- how many of your moments have a price row at all
        COUNT(*) FILTER (WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL)::int
          AS priced_moments

      FROM wallet_holdings h
      JOIN nft_core_metadata m
        ON m.nft_id = h.nft_id
      LEFT JOIN public.edition_price_scrape eps
        ON eps.edition_id = m.edition_id
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
          },
          floorValue: Number(statsRow.floor_value) || 0,
          aspValue: Number(statsRow.asp_value) || 0,
          pricedMoments: Number(statsRow.priced_moments) || 0
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
          },
          floorValue: 0,
          aspValue: 0,
          pricedMoments: 0
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

// Edition prices
app.get("/api/prices", async (req, res) => {
  try {
    const list = String(req.query.editions || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const unique = [...new Set(list)];
    if (!unique.length) {
      return res.json({ ok: true, asp: {}, lowAsk: {}, topSale: {} });
    }

    const result = await pgQuery(
      `
      SELECT
        edition_id,
        lowest_ask_usd,
        avg_sale_usd,
        top_sale_usd
      FROM public.edition_price_scrape
      WHERE edition_id = ANY($1::text[])
      `,
      [unique]
    );

    const asp = {};
    const lowAsk = {};
    const topSale = {};

    for (const row of result.rows) {
      const id = row.edition_id;
      if (row.avg_sale_usd != null) {
        asp[id] = Number(row.avg_sale_usd);
      }
      if (row.lowest_ask_usd != null) {
        lowAsk[id] = Number(row.lowest_ask_usd);
      }
      if (row.top_sale_usd != null) {
        topSale[id] = Number(row.top_sale_usd);
      }
    }

    return res.json({ ok: true, asp, lowAsk, topSale });
  } catch (err) {
    console.error("Error in /api/prices:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// Full wallet query
app.get("/api/query", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

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

// Test Neon
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

// Profiles aggregate search for profiles page
app.get("/api/profiles", async (req, res) => {
  const query = (req.query.query || "").trim();

  if (!query) {
    return res.json({ ok: true, profiles: [] });
  }

  try {
    const sql = `
      SELECT
        s.display_name,
        s.wallet_address,
        COALESCE(COUNT(s.nft_id), 0) AS total_moments,
        COALESCE(COUNT(*) FILTER (WHERE s.is_locked = FALSE), 0) AS unlocked_moments,
        COALESCE(COUNT(*) FILTER (WHERE s.is_locked = TRUE), 0) AS locked_moments,
        COALESCE(COUNT(*) FILTER (WHERE s.tier_norm = 'common'), 0)    AS tier_common,
        COALESCE(COUNT(*) FILTER (WHERE s.tier_norm = 'uncommon'), 0)  AS tier_uncommon,
        COALESCE(COUNT(*) FILTER (WHERE s.tier_norm = 'rare'), 0)      AS tier_rare,
        COALESCE(COUNT(*) FILTER (WHERE s.tier_norm = 'legendary'), 0) AS tier_legendary,
        COALESCE(COUNT(*) FILTER (WHERE s.tier_norm = 'ultimate'), 0)  AS tier_ultimate
      FROM (
        SELECT
          wp.display_name,
          wp.wallet_address,
          wh.nft_id,
          wh.is_locked,
          LOWER(ncm.tier) AS tier_norm
        FROM public.wallet_profiles AS wp
        LEFT JOIN public.wallet_holdings AS wh
          ON wh.wallet_address = wp.wallet_address
        LEFT JOIN public.nft_core_metadata AS ncm
          ON ncm.nft_id = wh.nft_id
        WHERE LOWER(wp.display_name) LIKE LOWER($1 || '%')
      ) AS s
      GROUP BY
        s.display_name,
        s.wallet_address
      ORDER BY
        total_moments DESC,
        s.display_name ASC
      LIMIT 50;
    `;

    const { rows } = await pool.query(sql, [query]);

    return res.json({
      ok: true,
      profiles: rows
    });
  } catch (err) {
    console.error("GET /api/profiles error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load profiles"
    });
  }
});

// Top wallets page
app.get("/api/top-wallets", async (req, res) => {
  let limit = parseInt(req.query.limit, 10);
  if (Number.isNaN(limit) || limit <= 0) limit = 50;
  if (limit > 500) limit = 500;

  try {
    const { rows } = await pool.query(
      `
      SELECT
        wallet_address,
        display_name,
        total_moments,
        unlocked_moments,
        locked_moments,
        tier_common,
        tier_uncommon,
        tier_rare,
        tier_legendary,
        tier_ultimate,
        updated_at
      FROM top_wallets_snapshot
      WHERE wallet_address NOT IN (
        '0xe4cf4bdc1751c65d', -- AllDay contract
        '0xb6f2481eba4df97b'  -- huge custodial/system wallet
      )
      ORDER BY total_moments DESC, display_name ASC
      LIMIT $1;
      `,
      [limit]
    );

    return res.json({
      ok: true,
      limit,
      count: rows.length,
      wallets: rows,
      schemaVersion: 3
    });
  } catch (err) {
    console.error("GET /api/top-wallets error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load top wallets"
    });
  }
});

// Paged wallet query: /api/query-paged?wallet=0x...&page=1&pageSize=200
app.get("/api/query-paged", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // Paging params
    const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
    const pageSizeRaw = parseInt(req.query.pageSize, 10) || 200;
    const pageSize = Math.min(Math.max(pageSizeRaw, 10), 500); // 10–500
    const offset = (page - 1) * pageSize;

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
        m.set_name,
        COUNT(*) OVER ()::int AS total_count
      FROM wallet_holdings h
      JOIN nft_core_metadata m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      ORDER BY h.last_event_ts DESC
      LIMIT $2 OFFSET $3;
      `,
      [wallet, pageSize, offset]
    );

    const rows = result.rows || [];
    const total = rows.length ? rows[0].total_count : 0;

    // Strip the window column before returning
    const cleanedRows = rows.map(({ total_count, ...rest }) => rest);

    return res.json({
      ok: true,
      wallet,
      page,
      pageSize,
      total,
      rows: cleanedRows
    });
  } catch (err) {
    console.error("Error in /api/query-paged (Neon):", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// GET /api/me – return current session user (used by nav + wallet auto-load)
app.get("/api/me", (req, res) => {
  if (!req.session || !req.session.user) {
    return res.json({ ok: false, user: null });
  }

  return res.json({
    ok: true,
    user: req.session.user
  });
});

// POST /api/login-dapper  { wallet_address }
app.post("/api/login-dapper", async (req, res) => {
  try {
    let { wallet_address } = req.body || {};

    if (!wallet_address || typeof wallet_address !== "string") {
      return res.status(400).json({ ok: false, error: "Missing wallet_address" });
    }

    wallet_address = wallet_address.trim().toLowerCase();
    if (!wallet_address.startsWith("0x")) {
      wallet_address = "0x" + wallet_address;
    }

    // Basic sanity check – Flow/Dapper-style address
    if (!/^0x[0-9a-f]{8,64}$/.test(wallet_address)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet address format" });
    }

    // Use a synthetic email so we can reuse the existing users table
    const syntheticEmail = `dapper:${wallet_address}`;

    const upsertSql = `
      INSERT INTO public.users (email, password_hash, default_wallet_address)
      VALUES ($1, NULL, $2)
      ON CONFLICT (email)
      DO UPDATE SET
        default_wallet_address = EXCLUDED.default_wallet_address
      RETURNING id, email, default_wallet_address;
    `;

    const { rows } = await pool.query(upsertSql, [syntheticEmail, wallet_address]);
    const user = rows[0];

    // Put essential stuff in the session
    req.session.user = {
      id: user.id,
      email: user.email, // will look like "dapper:0x..."
      default_wallet_address: user.default_wallet_address
    };

    return res.json({
      ok: true,
      user: req.session.user
    });
  } catch (err) {
    console.error("POST /api/login-dapper error:", err);
    return res.status(500).json({ ok: false, error: "Failed to log in with Dapper" });
  }
});

// ------------------ Start server ------------------

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`NFL ALL DAY collection viewer running on http://localhost:${port}`);
});
