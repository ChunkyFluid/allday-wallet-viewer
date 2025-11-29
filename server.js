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
import WebSocket from "ws";

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

// ------------------ Flow WebSocket Stream API for Live Events ------------------

const FLOW_WS_URL = "wss://rest-mainnet.onflow.org/v1/ws";
const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";

// Cache of recent live events (keep last 200)
let liveEventsCache = [];
const MAX_LIVE_EVENTS = 200;
let flowWsConnection = null;
let flowWsConnected = false;
let flowWsReconnectTimer = null;
let lastFlowEventTime = null;

// Event types we care about
const ALLDAY_EVENT_TYPES = [
  `${ALLDAY_CONTRACT}.Deposit`,
  `${ALLDAY_CONTRACT}.Withdraw`
];

function addLiveEvent(event) {
  liveEventsCache.unshift(event);
  if (liveEventsCache.length > MAX_LIVE_EVENTS) {
    liveEventsCache = liveEventsCache.slice(0, MAX_LIVE_EVENTS);
  }
  lastFlowEventTime = new Date();
}

function connectToFlowWebSocket() {
  if (flowWsConnection && flowWsConnection.readyState === WebSocket.OPEN) {
    console.log("Flow WebSocket already connected");
    return;
  }

  console.log("Connecting to Flow WebSocket Stream API...");
  
  try {
    flowWsConnection = new WebSocket(FLOW_WS_URL);
    
    flowWsConnection.on("open", () => {
      console.log("Flow WebSocket connected!");
      flowWsConnected = true;
      
      // Subscribe to AllDay events
      const subscribeMsg = {
        subscription_id: `allday-events-${Date.now()}`,
        action: "subscribe",
        topic: "events",
        arguments: {
          event_types: ALLDAY_EVENT_TYPES,
          start_block_status: "finalized"
        }
      };
      console.log("Sending subscription:", JSON.stringify(subscribeMsg));
      flowWsConnection.send(JSON.stringify(subscribeMsg));
      console.log(`Subscribed to AllDay events: ${ALLDAY_EVENT_TYPES.join(', ')}`);
    });
    
    flowWsConnection.on("message", async (data) => {
      try {
        const rawData = data.toString();
        const msg = JSON.parse(rawData);
        
        // Log all incoming messages for debugging
        console.log("Flow WS message received:", JSON.stringify(msg).substring(0, 500));
        
        // Handle subscription confirmation
        if (msg.subscription_id && !msg.events) {
          console.log(`Subscription confirmed: ${msg.subscription_id}`);
          return;
        }
        
        // Handle error messages
        if (msg.error) {
          console.error("Flow WS error:", msg.error);
          return;
        }
        
        // Handle events - they come in various formats
        if (msg.events && Array.isArray(msg.events)) {
          console.log(`Received ${msg.events.length} events`);
          for (const event of msg.events) {
            await processFlowEvent(event);
          }
        } else if (msg.type && msg.payload) {
          // Single event format
          await processFlowEvent(msg);
        } else if (msg.event) {
          // Another possible format
          await processFlowEvent(msg.event);
        }
      } catch (err) {
        console.error("Error processing Flow WebSocket message:", err.message, err.stack);
      }
    });
    
    flowWsConnection.on("error", (err) => {
      console.error("Flow WebSocket error:", err.message);
      flowWsConnected = false;
    });
    
    flowWsConnection.on("close", (code, reason) => {
      console.log(`Flow WebSocket closed: ${code} - ${reason}`);
      flowWsConnected = false;
      
      // Reconnect after 5 seconds
      if (!flowWsReconnectTimer) {
        flowWsReconnectTimer = setTimeout(() => {
          flowWsReconnectTimer = null;
          connectToFlowWebSocket();
        }, 5000);
      }
    });
  } catch (err) {
    console.error("Failed to create Flow WebSocket connection:", err.message);
    flowWsConnected = false;
    
    // Retry connection after 10 seconds
    if (!flowWsReconnectTimer) {
      flowWsReconnectTimer = setTimeout(() => {
        flowWsReconnectTimer = null;
        connectToFlowWebSocket();
      }, 10000);
    }
  }
}

async function processFlowEvent(event) {
  try {
    // Parse the event data
    const eventType = event.type ? event.type.split(".").pop() : "Unknown";
    let payload = {};
    
    // Handle different payload formats
    if (event.payload) {
      if (typeof event.payload === 'string') {
        try {
          // Try base64 decode first
          payload = JSON.parse(Buffer.from(event.payload, 'base64').toString());
        } catch {
          try {
            payload = JSON.parse(event.payload);
          } catch {
            payload = {};
          }
        }
      } else if (typeof event.payload === 'object') {
        payload = event.payload;
      }
    }
    
    // Extract NFT ID from payload - Flow events use Cadence value format
    let nftId = null;
    if (payload.value && payload.value.fields) {
      // Cadence JSON-CDC format
      const idField = payload.value.fields.find(f => f.name === 'id');
      if (idField && idField.value) {
        nftId = idField.value.value || idField.value;
      }
    } else {
      nftId = payload.id || payload.nftID || payload.nft_id || null;
    }
    
    // Extract addresses
    let fromAddr = null;
    let toAddr = null;
    if (payload.value && payload.value.fields) {
      const fromField = payload.value.fields.find(f => f.name === 'from');
      const toField = payload.value.fields.find(f => f.name === 'to');
      if (fromField && fromField.value) {
        fromAddr = (fromField.value.value || fromField.value || '').toString().toLowerCase();
      }
      if (toField && toField.value) {
        toAddr = (toField.value.value || toField.value || '').toString().toLowerCase();
      }
    } else {
      fromAddr = payload.from ? payload.from.toString().toLowerCase() : null;
      toAddr = payload.to ? payload.to.toString().toLowerCase() : null;
    }
    
    const timestamp = new Date().toISOString(); // Use current time for live events
    const blockHeight = event.block_height || event.height || 0;
    const txId = event.transaction_id || event.tx_id || null;
    
    // Create the event object
    const liveEvent = {
      type: eventType,
      nftId: nftId ? nftId.toString() : null,
      from: fromAddr || null,
      to: toAddr || null,
      timestamp: timestamp,
      blockHeight: blockHeight,
      txId: txId,
      source: 'live'
    };
    
    console.log(`Live event: ${eventType} NFT=${nftId} from=${fromAddr} to=${toAddr}`);
    
    // Try to enrich with wallet names and moment details
    if (liveEvent.from) {
      try {
        const sellerResult = await pgQuery(
          `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
          [liveEvent.from]
        );
        liveEvent.sellerName = sellerResult.rows[0]?.display_name || null;
      } catch (err) { /* ignore */ }
    }
    
    if (liveEvent.to) {
      try {
        const buyerResult = await pgQuery(
          `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
          [liveEvent.to]
        );
        liveEvent.buyerName = buyerResult.rows[0]?.display_name || null;
      } catch (err) { /* ignore */ }
    }
    
    if (liveEvent.nftId) {
      try {
        const momentResult = await pgQuery(
          `SELECT first_name, last_name, team_name, position, tier, set_name, series_name
           FROM nft_core_metadata WHERE nft_id = $1 LIMIT 1`,
          [liveEvent.nftId]
        );
        const moment = momentResult.rows[0];
        if (moment) {
          liveEvent.moment = {
            playerName: moment.first_name && moment.last_name 
              ? `${moment.first_name} ${moment.last_name}` : null,
            teamName: moment.team_name,
            position: moment.position,
            tier: moment.tier,
            setName: moment.set_name,
            seriesName: moment.series_name
          };
        }
      } catch (err) { /* ignore */ }
    }
    
    addLiveEvent(liveEvent);
  } catch (err) {
    console.error("Error processing Flow event:", err.message);
  }
}

// Start the WebSocket connection when server starts
setTimeout(() => {
  connectToFlowWebSocket();
}, 2000);

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

    // Update session with fresh data
    req.session.user = {
      id: u.id,
      email: u.email,
      default_wallet_address: u.default_wallet_address
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

// POST /api/me/name  { display_name }
app.post("/api/me/name", async (req, res) => {
  try {
    const sessUser = req.session?.user;
    if (!sessUser || !sessUser.id) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    let { display_name } = req.body || {};

    if (display_name && typeof display_name === "string") {
      display_name = display_name.trim().slice(0, 50);
    } else {
      display_name = null;
    }

    const { rows } = await pool.query(
      `
      UPDATE public.users
      SET display_name = $1
      WHERE id = $2
      RETURNING id, email, display_name, default_wallet_address;
      `,
      [display_name, sessUser.id]
    );

    if (!rows.length) {
      return res.status(404).json({ ok: false, error: "User not found" });
    }

    const u = rows[0];

    // Update session
    req.session.user = {
      id: u.id,
      email: u.email,
      display_name: u.display_name,
      default_wallet_address: u.default_wallet_address
    };

    return res.json({ ok: true, display_name: u.display_name });
  } catch (err) {
    console.error("POST /api/me/name error", err);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// POST /api/me/password  { current_password, new_password }
app.post("/api/me/password", async (req, res) => {
  try {
    const sessUser = req.session?.user;
    if (!sessUser || !sessUser.id) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const { current_password, new_password } = req.body || {};

    if (!current_password || !new_password) {
      return res.status(400).json({ ok: false, error: "Missing password fields" });
    }

    if (new_password.length < 8) {
      return res.status(400).json({ ok: false, error: "New password must be at least 8 characters" });
    }

    // Get current password hash
    const { rows: userRows } = await pool.query(
      `SELECT password_hash FROM public.users WHERE id = $1`,
      [sessUser.id]
    );

    if (!userRows.length) {
      return res.status(404).json({ ok: false, error: "User not found" });
    }

    // Verify current password
    const bcrypt = require("bcrypt");
    const valid = await bcrypt.compare(current_password, userRows[0].password_hash);
    if (!valid) {
      return res.status(400).json({ ok: false, error: "Current password is incorrect" });
    }

    // Hash new password and update
    const newHash = await bcrypt.hash(new_password, 10);
    await pool.query(
      `UPDATE public.users SET password_hash = $1 WHERE id = $2`,
      [newHash, sessUser.id]
    );

    return res.json({ ok: true });
  } catch (err) {
    console.error("POST /api/me/password error", err);
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

// Edition search (explorer) - groups by edition_id
app.get("/api/search-editions", async (req, res) => {
  try {
    const { player = "", team = "", tier = "", series = "", set = "", position = "", limit } = req.query;

    const rawLimit = parseInt(limit, 10);
    const safeLimit = Math.min(Math.max(rawLimit || 200, 1), 1000);

    const conditionsSnapshot = [];
    const conditionsLive = [];
    const params = [];
    let idx = 1;

    if (player) {
      conditionsSnapshot.push(`LOWER(first_name || ' ' || last_name) LIKE LOWER($${idx})`);
      conditionsLive.push(`LOWER(e.first_name || ' ' || e.last_name) LIKE LOWER($${idx})`);
      params.push(`%${player}%`);
      idx++;
    }

    if (team) {
      conditionsSnapshot.push(`team_name = $${idx}`);
      conditionsLive.push(`e.team_name = $${idx}`);
      params.push(team);
      idx++;
    }

    if (tier) {
      conditionsSnapshot.push(`tier = $${idx}`);
      conditionsLive.push(`e.tier = $${idx}`);
      params.push(tier);
      idx++;
    }

    if (series) {
      conditionsSnapshot.push(`series_name = $${idx}`);
      conditionsLive.push(`e.series_name = $${idx}`);
      params.push(series);
      idx++;
    }

    if (set) {
      conditionsSnapshot.push(`set_name = $${idx}`);
      conditionsLive.push(`e.set_name = $${idx}`);
      params.push(set);
      idx++;
    }

    if (position) {
      conditionsSnapshot.push(`position = $${idx}`);
      conditionsLive.push(`e.position = $${idx}`);
      params.push(position);
      idx++;
    }

    const whereClauseSnapshot = conditionsSnapshot.length ? `WHERE ${conditionsSnapshot.join(" AND ")}` : "";
    const whereClauseLive = conditionsLive.length ? `WHERE ${conditionsLive.join(" AND ")}` : "";

    // Try snapshot table first, fall back to live query if it doesn't exist
    let sql = `
      SELECT
        edition_id,
        first_name,
        last_name,
        team_name,
        position,
        tier,
        series_name,
        set_name,
        max_mint_size,
        total_moments,
        min_serial,
        max_serial,
        lowest_ask_usd,
        avg_sale_usd,
        top_sale_usd
      FROM editions_snapshot
      ${whereClauseSnapshot}
      ORDER BY last_name NULLS LAST, first_name NULLS LAST, edition_id
      LIMIT $${idx};
    `;

    params.push(safeLimit);

    let result;
    try {
      result = await pgQuery(sql, params);
    } catch (snapshotError) {
      // If snapshot table doesn't exist, fall back to live query
      if (snapshotError.message && (snapshotError.message.includes("does not exist") || snapshotError.message.includes("relation") && snapshotError.message.includes("editions_snapshot"))) {
        console.log("editions_snapshot table not found, falling back to live query");
        
        // Remove the limit param we just added
        params.pop();
        
        // Use live query with GROUP BY
        sql = `
          SELECT
            e.edition_id,
            MAX(e.first_name) AS first_name,
            MAX(e.last_name) AS last_name,
            MAX(e.team_name) AS team_name,
            MAX(e.position) AS position,
            MAX(e.tier) AS tier,
            MAX(e.series_name) AS series_name,
            MAX(e.set_name) AS set_name,
            MAX(e.max_mint_size) AS max_mint_size,
            COUNT(*) AS total_moments,
            MIN(e.serial_number) AS min_serial,
            MAX(e.serial_number) AS max_serial,
            eps.lowest_ask_usd,
            eps.avg_sale_usd,
            eps.top_sale_usd
          FROM nft_core_metadata e
          LEFT JOIN public.edition_price_scrape eps ON eps.edition_id = e.edition_id
          ${whereClauseLive}
          GROUP BY e.edition_id, eps.lowest_ask_usd, eps.avg_sale_usd, eps.top_sale_usd
          ORDER BY MAX(e.last_name) NULLS LAST, MAX(e.first_name) NULLS LAST, e.edition_id
          LIMIT $${idx};
        `;
        
        params.push(safeLimit);
        result = await pgQuery(sql, params);
      } else {
        throw snapshotError;
      }
    }

    return res.json({
      ok: true,
      count: result.rowCount,
      editions: result.rows
    });
  } catch (err) {
    console.error("Error in /api/search-editions:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// Get all moments for a specific edition
app.get("/api/edition-moments", async (req, res) => {
  try {
    const editionId = (req.query.edition || "").toString().trim();
    if (!editionId) {
      return res.status(400).json({ ok: false, error: "Missing ?edition=" });
    }

    const sql = `
      SELECT
        nft_id,
        edition_id,
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
      WHERE edition_id = $1
      ORDER BY serial_number NULLS LAST, nft_id
      LIMIT 1000;
    `;

    const result = await pgQuery(sql, [editionId]);

    return res.json({
      ok: true,
      count: result.rowCount,
      moments: result.rows
    });
  } catch (err) {
    console.error("Error in /api/edition-moments:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// Moment search (explorer) - kept for backward compatibility
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

// ============================================================
// LIVE WALLET DATA - Fetch directly from Flow blockchain
// ============================================================

// Cadence script to get AllDay NFT IDs from a wallet (Cadence 1.0 syntax)
// Using NonFungibleToken standard interface
const GET_ALLDAY_NFTS_SCRIPT = `
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d

access(all) fun main(address: Address): [UInt64] {
    let account = getAccount(address)
    
    // Try the standard AllDay collection path
    if let collectionRef = account.capabilities.borrow<&{NonFungibleToken.CollectionPublic}>(
        /public/AllDayNFTCollection
    ) {
        return collectionRef.getIDs()
    }
    
    return []
}
`;

// Fetch wallet's AllDay NFTs - use database snapshot (fast) 
// Live blockchain queries are too slow for real-time use
async function fetchLiveWalletNFTs(walletAddress) {
  // For now, return null to use database - live queries are too slow
  // TODO: Set up a background job to keep wallet_holdings updated
  return null;
}

// Wallet summary - NOW WITH LIVE DATA!
app.get("/api/wallet-summary", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    const useLive = req.query.live !== 'false'; // Default to live data

    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic Flow/Dapper-style address check
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // Try to fetch live data from blockchain
    let liveNftIds = null;
    let dataSource = 'database';
    
    if (useLive) {
      console.log(`[LiveWallet] Fetching live data for ${wallet}...`);
      liveNftIds = await fetchLiveWalletNFTs(wallet);
      if (liveNftIds !== null) {
        dataSource = 'blockchain';
        console.log(`[LiveWallet] Got ${liveNftIds.length} NFTs from blockchain`);
      } else {
        console.log(`[LiveWallet] Falling back to database`);
      }
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

    // 2) Stats + value based on edition_price_scrape
    let statsResult;
    
    if (liveNftIds !== null && liveNftIds.length > 0) {
      // Use LIVE blockchain data - query metadata for the NFT IDs we got from chain
      statsResult = await pgQuery(
        `
        SELECT
          $1 AS wallet_address,

          COUNT(*)::int AS moments_total,
          0::int AS locked_count,
          COUNT(*)::int AS unlocked_count,

          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int     AS common_count,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int   AS uncommon_count,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int       AS rare_count,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int  AS legendary_count,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int   AS ultimate_count,

          COALESCE(SUM(COALESCE(eps.lowest_ask_usd, 0)), 0)::numeric AS floor_value,
          COALESCE(SUM(COALESCE(eps.avg_sale_usd, 0)), 0)::numeric AS asp_value,
          COUNT(*) FILTER (WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL)::int AS priced_moments

        FROM nft_core_metadata m
        LEFT JOIN public.edition_price_scrape eps
          ON eps.edition_id = m.edition_id
        WHERE m.nft_id = ANY($2::text[]);
        `,
        [wallet, liveNftIds]
      );
    } else if (liveNftIds !== null && liveNftIds.length === 0) {
      // Live data returned empty wallet
      statsResult = { rows: [{ 
        wallet_address: wallet,
        moments_total: 0, locked_count: 0, unlocked_count: 0,
        common_count: 0, uncommon_count: 0, rare_count: 0, legendary_count: 0, ultimate_count: 0,
        floor_value: 0, asp_value: 0, priced_moments: 0
      }] };
    } else {
      // Fall back to database snapshot
      statsResult = await pgQuery(
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

        COALESCE(
          SUM(
            CASE
              WHEN eps.lowest_ask_usd IS NOT NULL THEN eps.lowest_ask_usd
              ELSE 0
            END
          ),
          0
        )::numeric AS floor_value,

        COALESCE(
          SUM(
            CASE
              WHEN eps.avg_sale_usd IS NOT NULL THEN eps.avg_sale_usd
              ELSE 0
            END
          ),
          0
        )::numeric AS asp_value,

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
    }

    // 3) Holdings + price freshness metadata
    const holdingsMetaResult = await pgQuery(
      `
      SELECT
        MAX(last_event_ts)  AS last_event_ts,
        MAX(last_synced_at) AS last_synced_at
      FROM wallet_holdings
      WHERE wallet_address = $1;
      `,
      [wallet]
    );
    const holdingsMetaRow = holdingsMetaResult.rows[0] || null;

    const pricesMetaResult = await pgQuery(
      `
      SELECT MAX(scraped_at) AS last_scraped_at
      FROM public.edition_price_scrape;
      `
    );
    const pricesMetaRow = pricesMetaResult.rows[0] || null;

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
      stats,
      dataSource, // 'blockchain' = live, 'database' = snapshot
      liveNftCount: liveNftIds ? liveNftIds.length : null,
      holdingsLastEventTs: holdingsMetaRow ? holdingsMetaRow.last_event_ts : null,
      holdingsLastSyncedAt: holdingsMetaRow ? holdingsMetaRow.last_synced_at : null,
      pricesLastScrapedAt: pricesMetaRow ? pricesMetaRow.last_scraped_at : null
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
    // Search by both display name and wallet address
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
        WHERE 
          LOWER(wp.display_name) LIKE LOWER($1 || '%')
          OR LOWER(wp.wallet_address) LIKE LOWER($1 || '%')
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

// Top wallets by team
app.get("/api/top-wallets-by-team", async (req, res) => {
  let limit = parseInt(req.query.limit, 10);
  if (Number.isNaN(limit) || limit <= 0) limit = 50;
  if (limit > 500) limit = 500;

  const team = (req.query.team || "").toString().trim();
  if (!team) {
    return res.status(400).json({ ok: false, error: "Missing ?team=" });
  }

  try {
    // Use snapshot table for fast reads
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
        tier_ultimate
      FROM top_wallets_by_team_snapshot
      WHERE team_name = $1
      ORDER BY total_moments DESC, display_name ASC
      LIMIT $2;
      `,
      [team, limit]
    );

    return res.json({
      ok: true,
      team,
      limit,
      count: rows.length,
      wallets: rows
    });
  } catch (err) {
    console.error("GET /api/top-wallets-by-team error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load top wallets by team: " + (err.message || String(err))
    });
  }
});

// Top wallets by tier
app.get("/api/top-wallets-by-tier", async (req, res) => {
  let limit = parseInt(req.query.limit, 10);
  if (Number.isNaN(limit) || limit <= 0) limit = 50;
  if (limit > 500) limit = 500;

  const tier = (req.query.tier || "").toString().trim();
  if (!tier) {
    return res.status(400).json({ ok: false, error: "Missing ?tier=" });
  }

  try {
    // Use snapshot table for fast reads
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
        tier_ultimate
      FROM top_wallets_by_tier_snapshot
      WHERE tier = LOWER($1)
      ORDER BY total_moments DESC, display_name ASC
      LIMIT $2;
      `,
      [tier, limit]
    );

    return res.json({
      ok: true,
      tier,
      limit,
      count: rows.length,
      wallets: rows
    });
  } catch (err) {
    console.error("GET /api/top-wallets-by-tier error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load top wallets by tier: " + (err.message || String(err))
    });
  }
});

// Top wallets by value (collection value)
app.get("/api/top-wallets-by-value", async (req, res) => {
  let limit = parseInt(req.query.limit, 10);
  if (Number.isNaN(limit) || limit <= 0) limit = 50;
  if (limit > 500) limit = 500;

  const valueType = (req.query.valueType || "floor").toString().trim().toLowerCase();
  // Use floor_value for ordering (safe since we control the value)
  const orderByColumn = valueType === "asp" ? "asp_value" : "floor_value";

  try {
    // Use snapshot table for fast reads
    const orderBy = valueType === "asp" ? "asp_value" : "floor_value";
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
        floor_value,
        asp_value
      FROM top_wallets_by_value_snapshot
      ORDER BY ${orderBy} DESC, total_moments DESC, display_name ASC
      LIMIT $1;
      `,
      [limit]
    );

    return res.json({
      ok: true,
      valueType,
      limit,
      count: rows.length,
      wallets: rows.map(row => ({
        ...row,
        floor_value: Number(row.floor_value) || 0,
        asp_value: Number(row.asp_value) || 0
      }))
    });
  } catch (err) {
    console.error("GET /api/top-wallets-by-value error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load top wallets by value: " + (err.message || String(err))
    });
  }
});

// Cache for teams list (refreshes every 5 minutes)
let teamsCache = null;
let teamsCacheTime = 0;
const TEAMS_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Get list of teams for filter dropdown
app.get("/api/teams", async (req, res) => {
  try {
    // Return cached result if available and fresh
    const now = Date.now();
    if (teamsCache && (now - teamsCacheTime) < TEAMS_CACHE_TTL) {
      return res.json({
        ok: true,
        teams: teamsCache,
        cached: true
      });
    }

    // Try to use explorer_filters_snapshot first (much faster)
    let teams = null;
    try {
      const snapshotRes = await pool.query(
        `
        SELECT teams
        FROM explorer_filters_snapshot
        WHERE id = 1
        LIMIT 1;
        `
      );
      
      if (snapshotRes.rows.length > 0 && snapshotRes.rows[0].teams) {
        // PostgreSQL JSONB columns are automatically parsed to JavaScript arrays
        teams = snapshotRes.rows[0].teams;
        if (!Array.isArray(teams)) {
          // Fallback: if somehow it's not an array, try to parse it
          teams = typeof teams === 'string' ? JSON.parse(teams) : [];
        }
      }
    } catch (snapshotErr) {
      // If snapshot table doesn't exist or query fails, fall back to live query
      console.log("explorer_filters_snapshot not available, falling back to live query:", snapshotErr.message);
    }

    // Fallback to live query if snapshot not available
    if (!teams || teams.length === 0) {
      const { rows } = await pool.query(
        `
        SELECT DISTINCT team_name
        FROM nft_core_metadata
        WHERE team_name IS NOT NULL AND team_name != ''
        ORDER BY team_name ASC
        LIMIT 100;
        `
      );
      teams = rows.map(r => r.team_name);
    }
    
    // Update cache
    teamsCache = teams;
    teamsCacheTime = now;

    return res.json({
      ok: true,
      teams: teams,
      cached: false
    });
  } catch (err) {
    console.error("GET /api/teams error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load teams: " + (err.message || String(err))
    });
  }
});

// Ensure insights_snapshot table exists
async function ensureInsightsSnapshotTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS insights_snapshot (
        id INTEGER PRIMARY KEY DEFAULT 1,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT single_row CHECK (id = 1)
      );
    `);
  } catch (err) {
    console.error("Error ensuring insights_snapshot table:", err);
  }
}

// Refresh insights snapshot (runs all queries and caches result)
app.post("/api/insights/refresh", async (req, res) => {
  try {
    await ensureInsightsSnapshotTable();
    
    console.log("Refreshing insights snapshot...");
    const startTime = Date.now();
    
    // Run all queries in parallel for speed
    const [
      statsResult,
      sizeDistResult,
      biggestCollectorResult,
      lowSerialResult,
      topTeamsResult,
      topPlayersResult,
      topSetsResult,
      positionResult,
      marketResult,
      medianResult,
      whaleStatsResult,
      seriesResult,
      jerseyResult,
      editionSizeResult,
      mostValuableResult,
      serialDistResult,
      richestResult
    ] = await Promise.all([
      // Basic stats
      pool.query(`
      SELECT
        COUNT(*)::bigint AS total_wallets,
        SUM(total_moments)::bigint AS total_moments,
        AVG(total_moments)::numeric AS avg_collection_size,
        SUM(unlocked_moments)::bigint AS total_unlocked,
        SUM(locked_moments)::bigint AS total_locked,
        SUM(tier_common)::bigint AS tier_common_total,
        SUM(tier_uncommon)::bigint AS tier_uncommon_total,
        SUM(tier_rare)::bigint AS tier_rare_total,
        SUM(tier_legendary)::bigint AS tier_legendary_total,
        SUM(tier_ultimate)::bigint AS tier_ultimate_total
      FROM top_wallets_snapshot
        WHERE wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b');
      `),
      
      // Size distribution
      pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE total_moments BETWEEN 1 AND 10)::bigint AS bin_1_10,
        COUNT(*) FILTER (WHERE total_moments BETWEEN 11 AND 100)::bigint AS bin_10_100,
        COUNT(*) FILTER (WHERE total_moments BETWEEN 101 AND 1000)::bigint AS bin_100_1000,
        COUNT(*) FILTER (WHERE total_moments > 1000)::bigint AS bin_1000_plus
      FROM top_wallets_snapshot
        WHERE wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b');
      `),
      
      // Biggest collector
      pool.query(`
        SELECT 
          wallet_address,
          total_moments,
          COALESCE(display_name, wallet_address) AS name
        FROM top_wallets_snapshot
        WHERE wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b')
        ORDER BY total_moments DESC
        LIMIT 1;
      `),
      
      // Low serial counts
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE serial_number = 1)::bigint AS serial_1,
          COUNT(*) FILTER (WHERE serial_number <= 10)::bigint AS serial_10,
          COUNT(*) FILTER (WHERE serial_number <= 100)::bigint AS serial_100,
          COUNT(*) FILTER (WHERE serial_number <= 1000)::bigint AS serial_1000
        FROM nft_core_metadata;
      `),
      
      // Top 5 teams
      pool.query(`
        SELECT team_name, COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE team_name IS NOT NULL AND team_name != ''
        GROUP BY team_name
        ORDER BY count DESC
        LIMIT 5;
      `),
      
      // Top 5 players
      pool.query(`
        SELECT 
          CONCAT(first_name, ' ', last_name) AS player_name,
          team_name,
          COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE first_name IS NOT NULL AND last_name IS NOT NULL
        GROUP BY first_name, last_name, team_name
        ORDER BY count DESC
        LIMIT 5;
      `),
      
      // Top 5 sets
      pool.query(`
        SELECT set_name, COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE set_name IS NOT NULL AND set_name != ''
        GROUP BY set_name
        ORDER BY count DESC
        LIMIT 5;
      `),
      
      // Position breakdown
      pool.query(`
        SELECT position, COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE position IS NOT NULL AND position != ''
        GROUP BY position
        ORDER BY count DESC;
      `),
      
      // Market stats
      pool.query(`
        SELECT 
          COUNT(*)::bigint AS editions_with_price,
          ROUND(AVG(lowest_ask_usd)::numeric, 2) AS avg_floor,
          ROUND(SUM(lowest_ask_usd)::numeric, 2) AS total_floor_value,
          ROUND(MAX(lowest_ask_usd)::numeric, 2) AS highest_floor,
          ROUND(AVG(avg_sale_usd)::numeric, 2) AS avg_sale
        FROM edition_price_scrape
        WHERE lowest_ask_usd > 0;
      `),
      
      // Median collection size
      pool.query(`
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_moments) AS median
        FROM top_wallets_snapshot
        WHERE wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b');
      `),
      
      // 🐳 Whale stats
      pool.query(`
        WITH ranked AS (
          SELECT total_moments,
            ROW_NUMBER() OVER (ORDER BY total_moments DESC) AS rn,
            COUNT(*) OVER () AS total_count
          FROM top_wallets_snapshot
          WHERE wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b')
        )
        SELECT
          SUM(total_moments) FILTER (WHERE rn <= 10)::bigint AS top_10_moments,
          SUM(total_moments) FILTER (WHERE rn <= 100)::bigint AS top_100_moments,
          SUM(total_moments) FILTER (WHERE rn <= CEIL(total_count * 0.01))::bigint AS top_1pct_moments,
          SUM(total_moments)::bigint AS all_moments
        FROM ranked;
      `),
      
      // 📅 Series breakdown
      pool.query(`
        SELECT series_name, COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE series_name IS NOT NULL AND series_name != ''
        GROUP BY series_name
        ORDER BY series_name;
      `),
      
      // 🔢 Popular jersey numbers
      pool.query(`
        SELECT jersey_number, COUNT(*)::bigint AS count
        FROM nft_core_metadata
        WHERE jersey_number IS NOT NULL AND jersey_number > 0
        GROUP BY jersey_number
        ORDER BY count DESC
        LIMIT 10;
      `),
      
      // 📦 Edition size distribution
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE max_mint_size <= 50)::bigint AS ultra_limited,
          COUNT(*) FILTER (WHERE max_mint_size BETWEEN 51 AND 250)::bigint AS limited,
          COUNT(*) FILTER (WHERE max_mint_size BETWEEN 251 AND 1000)::bigint AS standard,
          COUNT(*) FILTER (WHERE max_mint_size BETWEEN 1001 AND 10000)::bigint AS large,
          COUNT(*) FILTER (WHERE max_mint_size > 10000)::bigint AS mass
        FROM nft_core_metadata
        WHERE max_mint_size IS NOT NULL AND max_mint_size > 0;
      `),
      
      // 🔥 Most valuable editions
      pool.query(`
        SELECT 
          e.edition_id,
          CONCAT(m.first_name, ' ', m.last_name) AS player_name,
          m.team_name,
          m.tier,
          m.set_name,
          e.lowest_ask_usd
        FROM edition_price_scrape e
        JOIN nft_core_metadata m ON m.edition_id = e.edition_id
        WHERE e.lowest_ask_usd > 0
        ORDER BY e.lowest_ask_usd DESC
        LIMIT 5;
      `),
      
      // 🎯 Serial distribution
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE serial_number <= 10)::bigint AS tier_1_10,
          COUNT(*) FILTER (WHERE serial_number BETWEEN 11 AND 100)::bigint AS tier_11_100,
          COUNT(*) FILTER (WHERE serial_number BETWEEN 101 AND 500)::bigint AS tier_101_500,
          COUNT(*) FILTER (WHERE serial_number BETWEEN 501 AND 1000)::bigint AS tier_501_1000,
          COUNT(*) FILTER (WHERE serial_number > 1000)::bigint AS tier_1000_plus
        FROM nft_core_metadata
        WHERE serial_number IS NOT NULL;
      `),
      
      // 🏆 Richest collections
      pool.query(`
        SELECT 
          h.wallet_address,
          COALESCE(t.display_name, h.wallet_address) AS name,
          COUNT(*)::bigint AS moment_count,
          ROUND(SUM(COALESCE(e.lowest_ask_usd, 0))::numeric, 2) AS floor_value
        FROM wallet_holdings h
        JOIN nft_core_metadata m ON m.nft_id = h.nft_id
        LEFT JOIN edition_price_scrape e ON e.edition_id = m.edition_id
        LEFT JOIN top_wallets_snapshot t ON t.wallet_address = h.wallet_address
        WHERE h.wallet_address NOT IN ('0xe4cf4bdc1751c65d', '0xb6f2481eba4df97b')
        GROUP BY h.wallet_address, t.display_name
        ORDER BY floor_value DESC
        LIMIT 5;
      `)
    ]);

    const stats = statsResult.rows[0] || {};
    const sizeDist = sizeDistResult.rows[0] || {};
    const biggestCollector = biggestCollectorResult.rows[0] || {};
    const lowSerials = lowSerialResult.rows[0] || {};
    const market = marketResult.rows[0] || {};
    const medianRow = medianResult.rows[0] || {};
    const whaleStats = whaleStatsResult.rows[0] || {};
    const editionSizes = editionSizeResult.rows[0] || {};
    const serialDist = serialDistResult.rows[0] || {};

    // Calculate tier percentages
    const totalMoments = Number(stats.total_moments) || 1;
    const tierPercents = {
      common: ((Number(stats.tier_common_total) / totalMoments) * 100).toFixed(1),
      uncommon: ((Number(stats.tier_uncommon_total) / totalMoments) * 100).toFixed(1),
      rare: ((Number(stats.tier_rare_total) / totalMoments) * 100).toFixed(1),
      legendary: ((Number(stats.tier_legendary_total) / totalMoments) * 100).toFixed(2),
      ultimate: ((Number(stats.tier_ultimate_total) / totalMoments) * 100).toFixed(3)
    };

    // Calculate whale percentages
    const allMoments = Number(whaleStats.all_moments) || 1;
    const whalePercents = {
      top10: ((Number(whaleStats.top_10_moments) / allMoments) * 100).toFixed(1),
      top100: ((Number(whaleStats.top_100_moments) / allMoments) * 100).toFixed(1),
      top1pct: ((Number(whaleStats.top_1pct_moments) / allMoments) * 100).toFixed(1)
    };

    // Calculate challenge engagement
    const challengeEngagement = ((Number(stats.total_locked) / totalMoments) * 100).toFixed(1);

    const snapshotData = {
      ok: true,
      stats: {
        totalWallets: Number(stats.total_wallets) || 0,
        totalMoments: Number(stats.total_moments) || 0,
        avgCollectionSize: Math.round(Number(stats.avg_collection_size) || 0),
        medianCollectionSize: Math.round(Number(medianRow.median) || 0),
        totalUnlocked: Number(stats.total_unlocked) || 0,
        totalLocked: Number(stats.total_locked) || 0,
        tierCommon: Number(stats.tier_common_total) || 0,
        tierUncommon: Number(stats.tier_uncommon_total) || 0,
        tierRare: Number(stats.tier_rare_total) || 0,
        tierLegendary: Number(stats.tier_legendary_total) || 0,
        tierUltimate: Number(stats.tier_ultimate_total) || 0,
        tierPercents,
        challengeEngagement
      },
      sizeDistribution: {
        "1-10": Number(sizeDist.bin_1_10) || 0,
        "11-100": Number(sizeDist.bin_10_100) || 0,
        "101-1K": Number(sizeDist.bin_100_1000) || 0,
        "1K+": Number(sizeDist.bin_1000_plus) || 0
      },
      biggestCollector: {
        name: biggestCollector.name || "Unknown",
        wallet: biggestCollector.wallet_address || "",
        moments: Number(biggestCollector.total_moments) || 0
      },
      lowSerials: {
        "#1": Number(lowSerials.serial_1) || 0,
        "≤10": Number(lowSerials.serial_10) || 0,
        "≤100": Number(lowSerials.serial_100) || 0,
        "≤1000": Number(lowSerials.serial_1000) || 0
      },
      topTeams: topTeamsResult.rows.map(r => ({ name: r.team_name, count: Number(r.count) })),
      topPlayers: topPlayersResult.rows.map(r => ({ name: r.player_name, team: r.team_name, count: Number(r.count) })),
      topSets: topSetsResult.rows.map(r => ({ name: r.set_name, count: Number(r.count) })),
      positions: positionResult.rows.reduce((acc, r) => {
        acc[r.position] = Number(r.count);
        return acc;
      }, {}),
      market: {
        editionsWithPrice: Number(market.editions_with_price) || 0,
        avgFloor: Number(market.avg_floor) || 0,
        totalFloorValue: Number(market.total_floor_value) || 0,
        highestFloor: Number(market.highest_floor) || 0,
        avgSale: Number(market.avg_sale) || 0
      },
      whales: {
        top10Moments: Number(whaleStats.top_10_moments) || 0,
        top100Moments: Number(whaleStats.top_100_moments) || 0,
        top1pctMoments: Number(whaleStats.top_1pct_moments) || 0,
        percents: whalePercents
      },
      series: seriesResult.rows.map(r => ({ name: r.series_name, count: Number(r.count) })),
      jerseys: jerseyResult.rows.map(r => ({ number: r.jersey_number, count: Number(r.count) })),
      editionSizes: {
        "≤50 (Ultra)": Number(editionSizes.ultra_limited) || 0,
        "51-250 (Limited)": Number(editionSizes.limited) || 0,
        "251-1K (Standard)": Number(editionSizes.standard) || 0,
        "1K-10K (Large)": Number(editionSizes.large) || 0,
        "10K+ (Mass)": Number(editionSizes.mass) || 0
      },
      mostValuable: mostValuableResult.rows.map(r => ({
        player: r.player_name,
        team: r.team_name,
        tier: r.tier,
        set: r.set_name,
        floor: Number(r.lowest_ask_usd) || 0
      })),
      serialDistribution: {
        "1-10": Number(serialDist.tier_1_10) || 0,
        "11-100": Number(serialDist.tier_11_100) || 0,
        "101-500": Number(serialDist.tier_101_500) || 0,
        "501-1K": Number(serialDist.tier_501_1000) || 0,
        "1K+": Number(serialDist.tier_1000_plus) || 0
      },
      richestCollections: richestResult.rows.map(r => ({
        name: r.name,
        wallet: r.wallet_address,
        moments: Number(r.moment_count) || 0,
        floorValue: Number(r.floor_value) || 0
      }))
    };

    // Upsert snapshot
    await pool.query(`
      INSERT INTO insights_snapshot (id, data, updated_at)
      VALUES (1, $1, now())
      ON CONFLICT (id) DO UPDATE
      SET data = $1, updated_at = now();
    `, [JSON.stringify(snapshotData)]);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`✅ Insights snapshot refreshed in ${duration}s`);

    return res.json({
      ok: true,
      message: `Snapshot refreshed in ${duration}s`,
      updated_at: new Date().toISOString()
    });
  } catch (err) {
    console.error("POST /api/insights/refresh error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to refresh insights snapshot: " + (err.message || String(err))
    });
  }
});

// Global insights: aggregate stats across all wallets (now returns cached snapshot)
app.get("/api/insights", async (req, res) => {
  try {
    await ensureInsightsSnapshotTable();
    
    // Fetch cached snapshot
    const result = await pool.query(`
      SELECT data, updated_at
      FROM insights_snapshot
      WHERE id = 1;
    `);

    if (!result.rows.length) {
      // No snapshot exists yet - return error suggesting refresh
      return res.status(503).json({
        ok: false,
        error: "Insights snapshot not available. Please refresh first.",
        needsRefresh: true
      });
    }

    const snapshot = result.rows[0];
    const data = typeof snapshot.data === 'string' ? JSON.parse(snapshot.data) : snapshot.data;
    
    // Add metadata about freshness
    data.snapshotUpdatedAt = snapshot.updated_at;
    data.fromCache = true;

    return res.json(data);
  } catch (err) {
    console.error("GET /api/insights error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to load insights: " + (err.message || String(err))
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
app.get("/api/me", async (req, res) => {
  if (!req.session || !req.session.user) {
    return res.json({ ok: false, user: null });
  }

  try {
    // Fetch fresh user data from database
    const { rows } = await pool.query(
      `SELECT id, email, default_wallet_address, display_name, created_at FROM public.users WHERE id = $1`,
      [req.session.user.id]
    );

    if (rows.length) {
      const user = rows[0];
      // Update session with fresh data
      req.session.user = {
        id: user.id,
        email: user.email,
        default_wallet_address: user.default_wallet_address,
        display_name: user.display_name,
        created_at: user.created_at
      };
    }

    return res.json({
      ok: true,
      user: req.session.user
    });
  } catch (err) {
    console.error("GET /api/me error:", err);
    // Fallback to session data if DB query fails
    return res.json({
      ok: true,
      user: req.session.user
    });
  }
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

// Live Flow blockchain events proxy
// Note: Flow REST API is unreliable, so we query one event type at a time with small block ranges
app.get("/api/flow-events", async (req, res) => {
  try {
    let { start_height, end_height } = req.query;
    const FLOW_ACCESS_NODE = "https://rest-mainnet.onflow.org";
    const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";

    // Validate and parse block heights
    start_height = start_height ? parseInt(start_height, 10) : null;
    end_height = end_height ? parseInt(end_height, 10) : null;
    
    if (start_height !== null && (isNaN(start_height) || start_height < 0)) {
      return res.status(400).json({
        ok: false,
        error: "Invalid start_height: must be a non-negative integer"
      });
    }
    
    if (end_height !== null && (isNaN(end_height) || end_height < 0)) {
      return res.status(400).json({
        ok: false,
        error: "Invalid end_height: must be a non-negative integer"
      });
    }
    
    if (start_height !== null && end_height !== null && start_height > end_height) {
      return res.status(400).json({
        ok: false,
        error: "start_height must be less than or equal to end_height"
      });
    }

    // Flow REST API is very unreliable - query one event type at a time with very small ranges
    const eventTypes = [
      `${ALLDAY_CONTRACT}.Deposit`,
      `${ALLDAY_CONTRACT}.Withdraw`
    ];
    
    // Limit block range to 1-2 blocks to avoid 500 errors
    const maxBlockRange = 2;
    if (start_height !== null && end_height !== null && (end_height - start_height) > maxBlockRange) {
      end_height = start_height + maxBlockRange;
    }

    // Query each event type separately and merge results
    const allEvents = [];
    for (const eventType of eventTypes) {
      try {
        const urlParams = new URLSearchParams();
        urlParams.set("type", eventType);
        if (start_height !== null) urlParams.set("start_height", start_height.toString());
        if (end_height !== null) urlParams.set("end_height", end_height.toString());
        
        const url = `${FLOW_ACCESS_NODE}/v1/events?${urlParams.toString()}`;
        console.log(`Fetching ${eventType} from blocks ${start_height}-${end_height}`);

        const flowRes = await fetch(url);
        const responseText = await flowRes.text();
        
        if (!flowRes.ok) {
          if (flowRes.status === 500) {
            console.warn(`Flow API 500 error for ${eventType} (blocks ${start_height}-${end_height})`);
            continue; // Skip this event type
          } else {
            console.error(`Flow API error for ${eventType}:`, flowRes.status);
            continue;
          }
        }
        
        let data;
        try {
          data = JSON.parse(responseText);
        } catch (parseErr) {
          console.error(`Failed to parse response for ${eventType}`);
          continue;
        }
        
        if (data.results && Array.isArray(data.results)) {
          allEvents.push(...data.results);
          console.log(`Got ${data.results.length} ${eventType} events`);
        }
      } catch (err) {
        console.error(`Error fetching ${eventType}:`, err.message);
        continue;
      }
    }
    
    // Process all collected events
    const data = { results: allEvents };
    
    // Transform events to a simpler format
    const events = (data.results || []).map(event => {
      const eventType = event.type ? event.type.split(".").pop() : "Unknown";
      // Flow events have payload as base64 encoded, or as direct fields
      let payload = {};
      
      if (event.payload) {
        // Try to parse if it's a string
        if (typeof event.payload === 'string') {
          try {
            payload = JSON.parse(Buffer.from(event.payload, 'base64').toString());
          } catch {
            // If not base64, try direct JSON
            try {
              payload = JSON.parse(event.payload);
            } catch {
              payload = {};
            }
          }
        } else {
          payload = event.payload;
        }
      }
      
      // Also check event.data which might contain the actual event data
      if (event.data) {
        payload = { ...payload, ...event.data };
      }
      
      return {
        type: eventType,
        nftId: payload.id || payload.nftID || payload.nft_id || null,
        from: payload.from ? payload.from.toLowerCase() : null,
        to: payload.to ? payload.to.toLowerCase() : null,
        timestamp: event.block_timestamp || event.timestamp,
        blockHeight: event.block_height || event.height,
        txId: event.transaction_id || event.tx_id
      };
    });

    console.log(`Fetched ${events.length} events from blocks ${start_height} to ${end_height}`);
    
    // Debug: log raw events if we got any
    if (data.results && data.results.length > 0) {
      console.log("=== RAW EVENT STRUCTURE ===");
      console.log("Number of events:", data.results.length);
      console.log("First event keys:", Object.keys(data.results[0]));
      console.log("First event full structure:", JSON.stringify(data.results[0], null, 2).substring(0, 1500));
    } else {
      console.log("No events in response. Response keys:", Object.keys(data || {}));
      console.log("Response structure:", JSON.stringify(data).substring(0, 500));
    }

    return res.json({
      ok: true,
      events: events,
      start_height: start_height || null,
      end_height: end_height || null
    });
  } catch (err) {
    console.error("GET /api/flow-events error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch Flow events: " + (err.message || String(err))
    });
  }
});

// Direct Flow blockchain query to see what events actually exist
app.get("/api/test-flow-events", async (req, res) => {
  try {
    const FLOW_ACCESS_NODE = "https://rest-mainnet.onflow.org";
    const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";
    
    // Get latest block first
    const blockRes = await fetch(`${FLOW_ACCESS_NODE}/v1/blocks?height=final`);
    const blockData = await blockRes.json();
    let currentBlock = 0;
    
    if (Array.isArray(blockData) && blockData.length > 0) {
      currentBlock = parseInt(blockData[0].header?.height || blockData[0].height || 0);
    } else if (blockData.header?.height) {
      currentBlock = parseInt(blockData.header.height);
    }
    
    console.log(`Current Flow block height: ${currentBlock}`);
    
    // Query last 100 blocks for any AllDay events
    const startBlock = Math.max(0, currentBlock - 100);
    const endBlock = currentBlock;
    
    const eventTypes = [
      `${ALLDAY_CONTRACT}.Deposit`,
      `${ALLDAY_CONTRACT}.Withdraw`
    ];
    
    const allEvents = [];
    for (const eventType of eventTypes) {
      try {
        const url = `${FLOW_ACCESS_NODE}/v1/events?type=${encodeURIComponent(eventType)}&start_height=${startBlock}&end_height=${endBlock}`;
        console.log(`Querying Flow for ${eventType} from blocks ${startBlock}-${endBlock}`);
        
        const flowRes = await fetch(url);
        if (flowRes.ok) {
          const data = await flowRes.json();
          if (data.results && data.results.length > 0) {
            allEvents.push(...data.results);
            console.log(`Found ${data.results.length} ${eventType} events`);
          }
        } else {
          console.log(`Flow API returned ${flowRes.status} for ${eventType}`);
        }
      } catch (err) {
        console.error(`Error querying ${eventType}:`, err.message);
      }
    }
    
    return res.json({
      ok: true,
      currentBlock: currentBlock,
      queryRange: `${startBlock}-${endBlock}`,
      eventsFound: allEvents.length,
      events: allEvents.slice(0, 10), // Return first 10 for inspection
      message: allEvents.length > 0 
        ? `Found ${allEvents.length} events in last 100 blocks`
        : "No events found in last 100 blocks"
    });
  } catch (err) {
    console.error("GET /api/test-flow-events error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to test Flow query: " + (err.message || String(err))
    });
  }
});

// Test endpoint to check what events exist in Snowflake
app.get("/api/test-snowflake-events", async (req, res) => {
  try {
    await ensureSnowflakeConnected();
    
    // Query for the most recent events regardless of time
    const sql = `
      SELECT
        EVENT_DATA:id::STRING AS nft_id,
        LOWER(EVENT_DATA:to::STRING) AS to_addr,
        LOWER(EVENT_DATA:from::STRING) AS from_addr,
        EVENT_TYPE AS event_type,
        BLOCK_TIMESTAMP AS block_timestamp,
        BLOCK_HEIGHT AS block_height,
        TX_ID AS tx_id,
        EVENT_INDEX AS event_index
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
        AND EVENT_TYPE IN ('Deposit', 'Withdraw', 'MomentNFTMinted', 'MomentNFTBurned')
        AND TX_SUCCEEDED = TRUE
      ORDER BY BLOCK_TIMESTAMP DESC, BLOCK_HEIGHT DESC, EVENT_INDEX DESC
      LIMIT 20
    `;
    
    console.log("Test query: Getting most recent events from Snowflake...");
    
    const result = await executeSql(sql);
    console.log(`Test query returned ${result ? result.length : 0} events`);
    
    if (result && result.length > 0) {
      console.log("Most recent event timestamp:", result[0].block_timestamp || result[0].BLOCK_TIMESTAMP);
      console.log("Sample event:", JSON.stringify(result[0]).substring(0, 500));
    }
    
    return res.json({
      ok: true,
      count: result ? result.length : 0,
      events: result || [],
      message: result && result.length > 0 
        ? `Found ${result.length} recent events. Most recent: ${result[0].block_timestamp || result[0].BLOCK_TIMESTAMP}`
        : "No events found in Snowflake"
    });
  } catch (err) {
    console.error("GET /api/test-snowflake-events error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to test Snowflake query: " + (err.message || String(err))
    });
  }
});

// Alternative: Get recent events from Snowflake (more reliable than Flow REST API)
// Note: Snowflake data is typically delayed by 30-90 minutes
app.get("/api/recent-events-snowflake", async (req, res) => {
  try {
    const { limit = 100, hours = 2 } = req.query;
    const limitNum = Math.min(parseInt(limit, 10) || 100, 500);
    const hoursAgo = parseInt(hours, 10) || 2; // Default to 2 hours which captures recent activity with typical Snowflake delay
    
    // Calculate timestamp for X hours ago (Snowflake data is delayed)
    const cutoffTime = new Date(Date.now() - hoursAgo * 60 * 60 * 1000);
    // Format for Snowflake: 'YYYY-MM-DD HH:MM:SS'
    const cutoffStr = cutoffTime.toISOString().replace('T', ' ').substring(0, 19);
    
    await ensureSnowflakeConnected();
    
    // Query for multiple event types: Deposit, Withdraw, MomentNFTMinted, MomentNFTBurned
    // Note: Snowflake returns fields in uppercase, so we need to handle both cases
    const sql = `
      SELECT
        EVENT_DATA:id::STRING AS nft_id,
        LOWER(EVENT_DATA:to::STRING) AS to_addr,
        LOWER(EVENT_DATA:from::STRING) AS from_addr,
        EVENT_TYPE AS event_type,
        BLOCK_TIMESTAMP AS block_timestamp,
        BLOCK_HEIGHT AS block_height,
        TX_ID AS tx_id,
        EVENT_INDEX AS event_index
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
        AND EVENT_TYPE IN ('Deposit', 'Withdraw', 'MomentNFTMinted', 'MomentNFTBurned')
        AND TX_SUCCEEDED = TRUE
        AND BLOCK_TIMESTAMP >= '${cutoffStr}'
      ORDER BY BLOCK_TIMESTAMP DESC, BLOCK_HEIGHT DESC, EVENT_INDEX DESC
      LIMIT ${limitNum}
    `;
    
    console.log(`Querying Snowflake for events in last ${hoursAgo} hours (since ${cutoffStr})`);
    console.log(`Current time: ${new Date().toISOString()}`);
    
    const result = await executeSql(sql);
    console.log(`Snowflake returned ${result ? result.length : 0} events`);
    
    if (result && result.length > 0) {
      console.log(`Sample event:`, JSON.stringify(result[0]).substring(0, 300));
      console.log(`Most recent event timestamp: ${result[0].block_timestamp || result[0].BLOCK_TIMESTAMP}`);
      console.log(`Most recent event block: ${result[0].block_height || result[0].BLOCK_HEIGHT}`);
    } else {
      console.log("No events found. This could mean:");
      console.log("1. No events occurred in the last", hoursAgo, "hours");
      console.log("2. Snowflake data is delayed beyond", hoursAgo, "hours");
      console.log("3. Events are in a different contract or have different event types");
    }
    
    // First, map all events
    const rawEvents = (result || []).map(row => {
      // Snowflake returns fields in uppercase (NFT_ID, TO_ADDR, etc.)
      const nftId = row.NFT_ID || row.nft_id || null;
      const eventType = row.EVENT_TYPE || row.event_type;
      const fromAddr = row.FROM_ADDR || row.from_addr || null;
      const toAddr = row.TO_ADDR || row.to_addr || null;
      // Snowflake timestamps are in UTC but may come as string without 'Z' or as Date object
      let timestamp = row.BLOCK_TIMESTAMP || row.block_timestamp;
      if (timestamp) {
        if (timestamp instanceof Date) {
          // Already a Date object - convert to ISO string
          timestamp = timestamp.toISOString();
        } else if (typeof timestamp === 'string' && !timestamp.endsWith('Z') && !timestamp.includes('+')) {
          // String without timezone - append 'Z' for proper ISO format
          timestamp = timestamp.replace(' ', 'T') + 'Z';
        }
      }
      const blockHeight = row.BLOCK_HEIGHT || row.block_height;
      const txId = row.TX_ID || row.tx_id;
      const eventIndex = row.EVENT_INDEX || row.event_index;
      
      return {
        type: eventType,
        nftId: nftId,
        from: fromAddr,
        to: toAddr,
        timestamp: timestamp,
        blockHeight: blockHeight,
        txId: txId,
        eventIndex: eventIndex
      };
    });
    
    // Group events by transaction ID and NFT ID to combine Withdraw/Deposit pairs
    const eventMap = new Map();
    
    for (const event of rawEvents) {
      const key = `${event.txId}-${event.nftId}`;
      
      if (!eventMap.has(key)) {
        eventMap.set(key, {
          withdraw: null,
          deposit: null,
          nftId: event.nftId,
          txId: event.txId,
          timestamp: event.timestamp,
          blockHeight: event.blockHeight
        });
      }
      
      const group = eventMap.get(key);
      if (event.type === 'Withdraw') {
        group.withdraw = event;
      } else if (event.type === 'Deposit') {
        group.deposit = event;
      }
    }
    
    // Convert grouped events into combined "Sold" events
    const events = [];
    for (const group of eventMap.values()) {
      // If we have both Withdraw and Deposit, it's a sale
      if (group.withdraw && group.deposit) {
        events.push({
          type: 'Sold',
          nftId: group.nftId,
          from: group.withdraw.from, // Seller
          to: group.deposit.to, // Buyer
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId
        });
      } else if (group.withdraw) {
        // Only Withdraw (moment left a wallet, but no deposit found - might be to marketplace)
        events.push({
          type: 'Listed',
          nftId: group.nftId,
          from: group.withdraw.from,
          to: null,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId
        });
      } else if (group.deposit) {
        // Only Deposit (moment entered a wallet, but no withdraw found - might be from marketplace)
        events.push({
          type: 'Purchased',
          nftId: group.nftId,
          from: null,
          to: group.deposit.to,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId
        });
      }
    }
    
    // Sort by timestamp descending (most recent first)
    events.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    // Now enrich events with wallet names and moment details
    const enrichedEvents = [];
    for (const event of events) {
      const enriched = { ...event };
      
      // Get wallet names for seller and buyer
      if (event.from) {
        try {
          const sellerResult = await pgQuery(
            `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
            [event.from]
          );
          enriched.sellerName = sellerResult.rows[0]?.display_name || null;
        } catch (err) {
          console.error(`Error fetching seller name for ${event.from}:`, err.message);
        }
      }
      
      if (event.to) {
        try {
          const buyerResult = await pgQuery(
            `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
            [event.to]
          );
          enriched.buyerName = buyerResult.rows[0]?.display_name || null;
        } catch (err) {
          console.error(`Error fetching buyer name for ${event.to}:`, err.message);
        }
      }
      
      // Get moment details (player, team, etc.)
      if (event.nftId) {
        try {
          const momentResult = await pgQuery(
            `SELECT 
              first_name, 
              last_name, 
              team_name, 
              position,
              tier,
              set_name,
              series_name
            FROM nft_core_metadata 
            WHERE nft_id = $1 LIMIT 1`,
            [event.nftId]
          );
          const moment = momentResult.rows[0];
          if (moment) {
            enriched.moment = {
              playerName: moment.first_name && moment.last_name 
                ? `${moment.first_name} ${moment.last_name}` 
                : null,
              teamName: moment.team_name,
              position: moment.position,
              tier: moment.tier,
              setName: moment.set_name,
              seriesName: moment.series_name
            };
          }
        } catch (err) {
          console.error(`Error fetching moment details for ${event.nftId}:`, err.message);
        }
      }
      
      enrichedEvents.push(enriched);
    }
    
    // Calculate the freshness of the data
    let newestEventTime = null;
    let dataAgeHours = null;
    if (enrichedEvents.length > 0) {
      newestEventTime = enrichedEvents[0].timestamp; // Already sorted newest first
      const ageMs = Date.now() - new Date(newestEventTime).getTime();
      dataAgeHours = Math.round(ageMs / (1000 * 60 * 60) * 10) / 10; // Round to 1 decimal
    }
    
    return res.json({
      ok: true,
      events: enrichedEvents,
      source: "snowflake",
      hoursAgo: hoursAgo,
      cutoffTime: cutoffStr,
      currentTime: new Date().toISOString(),
      newestEventTime: newestEventTime,
      dataAgeHours: dataAgeHours,
      note: dataAgeHours !== null 
        ? `Snowflake data is ${dataAgeHours} hours old (newest event: ${newestEventTime})`
        : "No events found in the specified time window"
    });
  } catch (err) {
    console.error("GET /api/recent-events-snowflake error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch recent events from Snowflake: " + (err.message || String(err))
    });
  }
});

// Flow REST API - free public endpoint (no Light Node needed!)
const FLOW_LIGHT_NODE_URL = "https://rest-mainnet.onflow.org";

// Cache for latest block height (refreshes every 10 seconds)
let cachedLatestBlockHeight = null;
let lastBlockHeightFetch = 0;

async function getLatestBlockHeight() {
  const now = Date.now();
  // Cache for 10 seconds
  if (cachedLatestBlockHeight && (now - lastBlockHeightFetch) < 10000) {
    return cachedLatestBlockHeight;
  }
  
  try {
    // Query the public Flow REST API for the latest block
    const res = await fetch("https://rest-mainnet.onflow.org/v1/blocks?height=sealed");
    if (res.ok) {
      const data = await res.json();
      const block = Array.isArray(data) ? data[0] : data;
      if (block && block.header && block.header.height) {
        cachedLatestBlockHeight = parseInt(block.header.height, 10);
        lastBlockHeightFetch = now;
        return cachedLatestBlockHeight;
      }
    }
  } catch (err) {
    console.error("Failed to fetch latest block height:", err.message);
  }
  
  // Fallback to cached or a reasonable estimate
  return cachedLatestBlockHeight || 134160000;
}

app.get("/api/lightnode-events", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 100);
    
    // Get the actual latest block height from the network
    const latestHeight = await getLatestBlockHeight();
    
    // Query last ~100 blocks for AllDay events (about 80 seconds of blocks)
    // The light node will proxy this to the upstream access node
    const endHeight = latestHeight;
    const startHeight = latestHeight - 100;
    
    // Query both Deposit and Withdraw events
    // The light node proxies these requests to the upstream access node
    console.log(`Querying light node for events in blocks ${startHeight}-${endHeight}`);
    
    const [depositRes, withdrawRes] = await Promise.all([
      fetch(`${FLOW_LIGHT_NODE_URL}/v1/events?type=A.e4cf4bdc1751c65d.AllDay.Deposit&start_height=${startHeight}&end_height=${endHeight}`),
      fetch(`${FLOW_LIGHT_NODE_URL}/v1/events?type=A.e4cf4bdc1751c65d.AllDay.Withdraw&start_height=${startHeight}&end_height=${endHeight}`)
    ]);
    
    const depositData = depositRes.ok ? await depositRes.json() : [];
    const withdrawData = withdrawRes.ok ? await withdrawRes.json() : [];
    
    // Extract events from the block-grouped response
    const allEvents = [];
    
    for (const block of depositData) {
      if (block.events && block.events.length > 0) {
        for (const event of block.events) {
          allEvents.push({
            type: 'Deposit',
            blockHeight: parseInt(block.block_height, 10),
            blockTimestamp: block.block_timestamp,
            txId: event.transaction_id,
            eventIndex: event.event_index,
            payload: event.payload
          });
        }
      }
    }
    
    for (const block of withdrawData) {
      if (block.events && block.events.length > 0) {
        for (const event of block.events) {
          allEvents.push({
            type: 'Withdraw',
            blockHeight: parseInt(block.block_height, 10),
            blockTimestamp: block.block_timestamp,
            txId: event.transaction_id,
            eventIndex: event.event_index,
            payload: event.payload
          });
        }
      }
    }
    
    // Parse event payloads to extract NFT IDs and addresses
    const parsedEvents = allEvents.map(event => {
      let nftId = null;
      let fromAddr = null;
      let toAddr = null;
      
      if (event.payload) {
        try {
          // Flow REST API returns base64-encoded JSON-CDC payloads
          let payloadStr = event.payload;
          if (typeof payloadStr === 'string') {
            // Decode base64
            try {
              payloadStr = Buffer.from(payloadStr, 'base64').toString('utf-8');
            } catch (e) {
              // Not base64, try as-is
            }
          }
          
          const payload = typeof payloadStr === 'string' ? JSON.parse(payloadStr) : payloadStr;
          
          if (payload.value && payload.value.fields) {
            for (const field of payload.value.fields) {
              if (field.name === 'id' && field.value) {
                // NFT ID: value.value for UInt64
                nftId = field.value.value || field.value;
              }
              if (field.name === 'from' && field.value) {
                // Address might be nested in Optional: value.value.value
                if (field.value.value && field.value.value.value) {
                  fromAddr = field.value.value.value.toString().toLowerCase();
                } else if (field.value.value) {
                  fromAddr = field.value.value.toString().toLowerCase();
                }
              }
              if (field.name === 'to' && field.value) {
                // Address might be nested in Optional: value.value.value
                if (field.value.value && field.value.value.value) {
                  toAddr = field.value.value.value.toString().toLowerCase();
                } else if (field.value.value) {
                  toAddr = field.value.value.toString().toLowerCase();
                }
              }
            }
          }
        } catch (e) {
          console.error("Error parsing event payload:", e.message);
        }
      }
      
      return {
        type: event.type,
        nftId: nftId ? nftId.toString() : null,
        from: fromAddr || null,
        to: toAddr || null,
        timestamp: event.blockTimestamp,
        blockHeight: event.blockHeight,
        txId: event.txId,
        source: 'lightnode'
      };
    });
    
    // Group events by txId+nftId to combine Withdraw/Deposit pairs into "Sold"
    const eventMap = new Map();
    for (const event of parsedEvents) {
      const key = `${event.txId}-${event.nftId}`;
      if (!eventMap.has(key)) {
        eventMap.set(key, { withdraw: null, deposit: null, ...event });
      }
      const group = eventMap.get(key);
      if (event.type === 'Withdraw') group.withdraw = event;
      if (event.type === 'Deposit') group.deposit = event;
    }
    
    // Convert to combined events
    const combinedEvents = [];
    for (const group of eventMap.values()) {
      if (group.withdraw && group.deposit) {
        combinedEvents.push({
          type: 'Sold',
          nftId: group.nftId,
          from: group.withdraw.from,
          to: group.deposit.to,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId,
          source: 'lightnode'
        });
      } else if (group.withdraw) {
        combinedEvents.push({ ...group.withdraw, type: 'Listed' });
      } else if (group.deposit) {
        combinedEvents.push({ ...group.deposit, type: 'Purchased' });
      }
    }
    
    // Sort by timestamp descending
    combinedEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    // Enrich with wallet names and moment details (limit to first 20 to avoid slowdown)
    const enrichedEvents = [];
    for (const event of combinedEvents.slice(0, Math.min(limit, 20))) {
      const enriched = { ...event };
      
      if (event.from) {
        try {
          const result = await pgQuery(`SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`, [event.from]);
          enriched.sellerName = result.rows[0]?.display_name || null;
        } catch (e) { /* ignore */ }
      }
      
      if (event.to) {
        try {
          const result = await pgQuery(`SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`, [event.to]);
          enriched.buyerName = result.rows[0]?.display_name || null;
        } catch (e) { /* ignore */ }
      }
      
      if (event.nftId) {
        try {
          const result = await pgQuery(
            `SELECT first_name, last_name, team_name, position, tier, set_name, series_name FROM nft_core_metadata WHERE nft_id = $1 LIMIT 1`,
            [event.nftId]
          );
          const moment = result.rows[0];
          if (moment) {
            enriched.moment = {
              playerName: moment.first_name && moment.last_name ? `${moment.first_name} ${moment.last_name}` : null,
              teamName: moment.team_name,
              position: moment.position,
              tier: moment.tier,
              setName: moment.set_name,
              seriesName: moment.series_name
            };
          }
        } catch (e) { /* ignore */ }
      }
      
      enrichedEvents.push(enriched);
    }
    
    return res.json({
      ok: true,
      events: enrichedEvents,
      source: "lightnode",
      latestBlockHeight: latestHeight,
      blocksQueried: 100,
      startHeight: startHeight,
      endHeight: endHeight,
      currentTime: new Date().toISOString()
    });
  } catch (err) {
    console.error("GET /api/lightnode-events error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to get events from light node: " + (err.message || String(err))
    });
  }
});

// Get live events from WebSocket stream (real-time, ~seconds delay)
app.get("/api/live-events", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 200);
    const sinceTimestamp = req.query.since ? new Date(req.query.since) : null;
    
    let events = liveEventsCache.slice(0, limit);
    
    // Filter to events newer than 'since' timestamp if provided
    if (sinceTimestamp && !isNaN(sinceTimestamp.getTime())) {
      events = events.filter(e => new Date(e.timestamp) > sinceTimestamp);
    }
    
    // Group Withdraw/Deposit pairs into "Sold" events (same logic as Snowflake)
    const eventMap = new Map();
    for (const event of events) {
      const key = `${event.txId}-${event.nftId}`;
      if (!eventMap.has(key)) {
        eventMap.set(key, {
          withdraw: null,
          deposit: null,
          nftId: event.nftId,
          txId: event.txId,
          timestamp: event.timestamp,
          blockHeight: event.blockHeight
        });
      }
      const group = eventMap.get(key);
      if (event.type === 'Withdraw') {
        group.withdraw = event;
      } else if (event.type === 'Deposit') {
        group.deposit = event;
      }
    }
    
    // Convert to combined events
    const combinedEvents = [];
    for (const group of eventMap.values()) {
      if (group.withdraw && group.deposit) {
        combinedEvents.push({
          type: 'Sold',
          nftId: group.nftId,
          from: group.withdraw.from,
          to: group.deposit.to,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId,
          sellerName: group.withdraw.sellerName,
          buyerName: group.deposit.buyerName,
          moment: group.withdraw.moment || group.deposit.moment,
          source: 'live'
        });
      } else if (group.withdraw) {
        combinedEvents.push({
          type: 'Listed',
          nftId: group.nftId,
          from: group.withdraw.from,
          to: null,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId,
          sellerName: group.withdraw.sellerName,
          moment: group.withdraw.moment,
          source: 'live'
        });
      } else if (group.deposit) {
        combinedEvents.push({
          type: 'Purchased',
          nftId: group.nftId,
          from: null,
          to: group.deposit.to,
          timestamp: group.timestamp,
          blockHeight: group.blockHeight,
          txId: group.txId,
          buyerName: group.deposit.buyerName,
          moment: group.deposit.moment,
          source: 'live'
        });
      }
    }
    
    // Sort by timestamp descending
    combinedEvents.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return res.json({
      ok: true,
      events: combinedEvents,
      source: "live-websocket",
      wsConnected: flowWsConnected,
      cacheSize: liveEventsCache.length,
      lastEventTime: lastFlowEventTime ? lastFlowEventTime.toISOString() : null,
      currentTime: new Date().toISOString()
    });
  } catch (err) {
    console.error("GET /api/live-events error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to get live events: " + (err.message || String(err))
    });
  }
});

// Get WebSocket connection status
app.get("/api/live-status", (req, res) => {
  return res.json({
    ok: true,
    wsConnected: flowWsConnected,
    cacheSize: liveEventsCache.length,
    lastEventTime: lastFlowEventTime ? lastFlowEventTime.toISOString() : null,
    currentTime: new Date().toISOString()
  });
});

// Get latest block height
app.get("/api/flow-latest-block", async (req, res) => {
  try {
    const FLOW_ACCESS_NODE = "https://rest-mainnet.onflow.org";
    const url = `${FLOW_ACCESS_NODE}/v1/blocks?height=final`;
    
    console.log("Fetching latest block from:", url);
    
    const flowRes = await fetch(url);
    const responseText = await flowRes.text();
    
    if (!flowRes.ok) {
      console.error("Flow API error:", flowRes.status, responseText.substring(0, 500));
      throw new Error(`Flow API error: ${flowRes.status} - ${responseText.substring(0, 200)}`);
    }

    let data;
    try {
      data = JSON.parse(responseText);
    } catch (parseErr) {
      console.error("Failed to parse Flow API response:", responseText.substring(0, 500));
      throw new Error("Invalid JSON response from Flow API");
    }
    
    // Flow API returns blocks in an array, or a single block object
    let block = data;
    if (Array.isArray(data) && data.length > 0) {
      block = data[0];
    }
    
    // Flow API returns height in block.header.height as a string
    const heightStr = block.header?.height || block.height;
    const height = parseInt(heightStr, 10);
    
    if (!height || height <= 0 || isNaN(height)) {
      console.error("Invalid block height from Flow API. Response:", JSON.stringify(block).substring(0, 500));
      throw new Error("Invalid block height returned from Flow API (got: " + heightStr + ")");
    }
    
    return res.json({
      ok: true,
      height: height,
      timestamp: block.timestamp || block.header?.timestamp || block.block?.header?.timestamp
    });
  } catch (err) {
    console.error("GET /api/flow-latest-block error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch latest block: " + (err.message || String(err))
    });
  }
});

// ============================================================
// SNIPER TOOL - Real-time marketplace deals
// ============================================================

// NFTStorefrontV2 contract for marketplace events
const STOREFRONT_CONTRACT = "A.4eb8a10cb9f87357.NFTStorefrontV2";

// ============================================================
// FLOOR PRICE CACHE - Stores known floor prices for editions
// ============================================================

const floorPriceCache = new Map(); // editionId -> { floor, updatedAt }
const FLOOR_CACHE_TTL = 5 * 60 * 1000; // 5 minutes before refreshing

// Scrape floor price from NFL All Day website
async function scrapeFloorPrice(editionId) {
  try {
    const url = `https://nflallday.com/listing/moment/${editionId}`;
    const res = await fetch(url, {
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml"
      }
    });
    
    if (!res.ok) return null;
    
    const html = await res.text();
    
    // Parse "Lowest Ask $X.XX" from the page
    const lowAskMatch = html.match(/Lowest\s+Ask[^$]*\$\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i);
    if (!lowAskMatch) return null;
    
    const floor = Number(lowAskMatch[1].replace(/,/g, ''));
    return isNaN(floor) ? null : floor;
  } catch (err) {
    console.error(`[Scrape] Error for edition ${editionId}:`, err.message);
    return null;
  }
}

// Get cached floor, or scrape if stale/missing
async function getCachedFloor(editionId) {
  const cached = floorPriceCache.get(editionId);
  if (cached && Date.now() - cached.updatedAt < FLOOR_CACHE_TTL) {
    return cached.floor;
  }
  
  const floor = await scrapeFloorPrice(editionId);
  if (floor !== null) {
    floorPriceCache.set(editionId, { floor, updatedAt: Date.now() });
  }
  return floor;
}

// Get floor without updating cache (for comparison)
function getStoredFloor(editionId) {
  const cached = floorPriceCache.get(editionId);
  return cached ? cached.floor : null;
}

// Update floor in cache
function updateFloorCache(editionId, newFloor) {
  const existing = floorPriceCache.get(editionId);
  // Only update if we don't have a floor OR if it's been a while
  if (!existing || Date.now() - existing.updatedAt > 60000) { // 1 min
    floorPriceCache.set(editionId, { floor: newFloor, updatedAt: Date.now() });
  }
}

// ============================================================
// LIVE SNIPER - Watch for listings below floor
// ============================================================

const sniperListings = []; // Array of ALL listings
const seenListingNfts = new Set(); // Track seen nftIds to prevent duplicates
const soldNfts = new Set(); // Track sold NFTs
const MAX_SNIPER_LISTINGS = 500;

function addSniperListing(listing) {
  // Dedupe by nftId - same NFT can't be listed twice
  if (seenListingNfts.has(listing.nftId)) return;
  seenListingNfts.add(listing.nftId);
  
  // Mark as sold if already in sold set
  if (soldNfts.has(listing.nftId)) {
    listing.isSold = true;
  }
  
  // Add to front of array
  sniperListings.unshift(listing);
  
  // Keep only last N listings
  if (sniperListings.length > MAX_SNIPER_LISTINGS) {
    const removed = sniperListings.pop();
    if (removed) seenListingNfts.delete(removed.nftId);
  }
  
  // Log deals (below floor)
  if (listing.dealPercent > 0 && !listing.isSold) {
    console.log(`[SNIPER] 🎯 DEAL: ${listing.playerName} #${listing.serialNumber || '?'} - $${listing.listingPrice} (floor $${listing.floor}) - ${listing.dealPercent.toFixed(1)}% off!`);
  }
}

async function markListingAsSold(nftId, buyerAddr = null) {
  soldNfts.add(nftId);
  
  // Get buyer name if we have buyer address
  let buyerName = buyerAddr;
  if (buyerAddr) {
    try {
      const result = await pgQuery(
        `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
        [buyerAddr]
      );
      buyerName = result.rows[0]?.display_name || buyerAddr;
    } catch (e) {
      buyerName = buyerAddr;
    }
  }
  
  // Mark existing listings as sold
  for (const listing of sniperListings) {
    if (listing.nftId === nftId) {
      listing.isSold = true;
      if (buyerAddr) {
        listing.buyerAddr = buyerAddr;
        listing.buyerName = buyerName;
      }
    }
  }
}

// Process a new listing event from the Light Node
async function processListingEvent(event) {
  try {
    const { nftId, listingPrice, sellerAddr, timestamp, editionId: eventEditionId } = event;
    
    if (!nftId || !listingPrice) return;
    
    // Get edition info from our database
    let editionId = eventEditionId;
    let momentData = null;
    
    if (!editionId) {
      try {
        const result = await pgQuery(
          `SELECT edition_id, serial_number, first_name, last_name, team_name, tier, set_name, series_name, jersey_number
           FROM nft_core_metadata WHERE nft_id = $1 LIMIT 1`,
          [nftId]
        );
        if (result.rows[0]) {
          momentData = result.rows[0];
          editionId = momentData.edition_id;
        }
      } catch (e) { /* ignore */ }
    }
    
    if (!editionId) return;
    
    // Get the PREVIOUS floor price (before this listing)
    let previousFloor = getStoredFloor(editionId);
    
    // If we don't have a cached floor, scrape it now
    if (previousFloor === null) {
      previousFloor = await getCachedFloor(editionId);
    }
    
    if (!previousFloor) return;
    
    // Skip non-whole-dollar listings (likely from Flowty, not official NFL All Day)
    // NFL All Day only allows whole dollar amounts ($1 minimum)
    if (listingPrice < 1 || listingPrice !== Math.floor(listingPrice)) {
      return; // Skip fractional/sub-$1 listings
    }
    
    // Calculate deal percent (positive = below floor = deal!)
    let dealPercent = 0;
    if (previousFloor && previousFloor > 0) {
      dealPercent = ((previousFloor - listingPrice) / previousFloor) * 100;
    }
    
    // Get moment metadata if we don't have it, or if we're missing series_name
    if (!momentData || !momentData.series_name) {
      try {
        const result = await pgQuery(
          `SELECT serial_number, first_name, last_name, team_name, tier, set_name, series_name, position, jersey_number
           FROM nft_core_metadata WHERE nft_id = $1 LIMIT 1`,
          [nftId]
        );
        if (result.rows[0]) {
          // Merge with existing momentData if it exists
          momentData = momentData ? { ...momentData, ...result.rows[0] } : result.rows[0];
        } else if (!momentData) {
          momentData = {};
        }
      } catch (e) { 
        if (!momentData) momentData = {};
      }
    }
    
    // Get ASP (Average Sale Price) from edition_price_scrape
    let avgSale = null;
    if (editionId) {
      try {
        const priceResult = await pgQuery(
          `SELECT avg_sale_usd FROM edition_price_scrape WHERE edition_id = $1 LIMIT 1`,
          [editionId]
        );
        avgSale = priceResult.rows[0]?.avg_sale_usd ? Number(priceResult.rows[0].avg_sale_usd) : null;
      } catch (e) { /* ignore */ }
    }
    
    // Get seller name
    let sellerName = sellerAddr;
    if (sellerAddr) {
      try {
        const result = await pgQuery(
          `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
          [sellerAddr]
        );
        sellerName = result.rows[0]?.display_name || sellerAddr;
      } catch (e) { /* ignore */ }
    }
    
    // Check if this listing is sold and get buyer info
    let buyerAddr = null;
    let buyerName = null;
    if (soldNfts.has(nftId)) {
      // Find the sold listing to get buyer info
      const soldListing = sniperListings.find(l => l.nftId === nftId && l.isSold);
      if (soldListing) {
        buyerAddr = soldListing.buyerAddr;
        buyerName = soldListing.buyerName;
      }
    }
    
    const listing = {
      nftId,
      editionId,
      serialNumber: momentData?.serial_number,
      listingPrice,
      floor: previousFloor,
      avgSale,
      dealPercent: Math.round(dealPercent * 10) / 10,
      playerName: momentData?.first_name && momentData?.last_name 
        ? `${momentData.first_name} ${momentData.last_name}` : null,
      teamName: momentData?.team_name,
      tier: momentData?.tier,
      setName: momentData?.set_name,
      seriesName: momentData?.series_name,
      position: momentData?.position,
      jerseyNumber: momentData?.jersey_number ? Number(momentData.jersey_number) : null,
      sellerName,
      sellerAddr,
      buyerAddr,
      buyerName,
      isLowSerial: momentData?.serial_number && momentData.serial_number <= 100,
      isSold: soldNfts.has(nftId),
      listedAt: timestamp || new Date().toISOString(),
      listingUrl: `https://nflallday.com/listing/moment/${editionId}`
    };
    
    addSniperListing(listing);
    
    // Update our floor cache with new floor (this listing might be the new floor)
    if (listingPrice < (previousFloor || Infinity)) {
      updateFloorCache(editionId, listingPrice);
    }
    
  } catch (err) {
    console.error("[Sniper] Error processing listing event:", err.message);
  }
}

// ============================================================
// LIVE WATCHER - Poll Flow REST API for new listing events
// ============================================================

const FLOW_REST_API = "https://rest-mainnet.onflow.org";
let lastCheckedBlock = 0;
let isWatchingListings = false;

async function watchForListings() {
  if (isWatchingListings) return;
  isWatchingListings = true;
  
  console.log("[Sniper] 🔴 Starting LIVE listing watcher (using Flow REST API)...");
  
  const checkForNewListings = async () => {
    try {
      // Get latest block height from Flow REST API
      const heightRes = await fetch(`${FLOW_REST_API}/v1/blocks?height=sealed`);
      if (!heightRes.ok) return;
      
      const heightData = await heightRes.json();
      const latestHeight = parseInt(heightData[0]?.header?.height || 0);
      
      if (lastCheckedBlock === 0) {
        lastCheckedBlock = latestHeight - 5; // Start from 5 blocks ago
        console.log(`[Sniper] Starting from block ${lastCheckedBlock}`);
      }
      
      if (latestHeight <= lastCheckedBlock) return;
      
      // Query for ListingAvailable and ListingCompleted events in new blocks
      const startHeight = lastCheckedBlock + 1;
      const endHeight = Math.min(latestHeight, startHeight + 50); // Max 50 blocks at a time
      
      // Check for new listings
      const listingUrl = `${FLOW_REST_API}/v1/events?type=A.4eb8a10cb9f87357.NFTStorefront.ListingAvailable&start_height=${startHeight}&end_height=${endHeight}`;
      const listingRes = await fetch(listingUrl);
      
      // Check for completed listings (sales)
      const completedUrl = `${FLOW_REST_API}/v1/events?type=A.4eb8a10cb9f87357.NFTStorefront.ListingCompleted&start_height=${startHeight}&end_height=${endHeight}`;
      const completedRes = await fetch(completedUrl);
      
      let newListingCount = 0;
      let alldayCount = 0;
      let soldCount = 0;
      
      // Process new listings
      if (listingRes.ok) {
        const eventData = await listingRes.json();
        
        for (const block of eventData) {
          if (!block.events) continue;
          
          for (const event of block.events) {
            try {
              // Decode the payload
              let payload = event.payload;
              if (typeof payload === 'string') {
                payload = JSON.parse(Buffer.from(payload, 'base64').toString());
              }
              
              if (!payload?.value?.fields) continue;
              
              const fields = payload.value.fields;
              const getField = (name) => {
                const f = fields.find(x => x.name === name);
                if (!f) return null;
                if (f.value?.value?.value) return f.value.value.value;
                if (f.value?.value) return f.value.value;
                return f.value;
              };
              
              // Check if this is an AllDay NFT
              const nftType = fields.find(f => f.name === 'nftType');
              const typeId = nftType?.value?.staticType?.typeID || 
                            nftType?.value?.value?.staticType?.typeID || '';
              
              if (!typeId.includes('AllDay')) continue;
              
              const nftId = getField('nftID')?.toString();
              const priceStr = getField('price');
              const listingPrice = priceStr ? parseFloat(priceStr) : null;
              const sellerAddr = getField('storefrontAddress')?.toString()?.toLowerCase();
              
              if (!nftId || !listingPrice) continue;
              
              alldayCount++;
              
              // Process this listing
              await processListingEvent({
                nftId,
                listingPrice,
                sellerAddr,
                timestamp: block.block_timestamp
              });
              
              newListingCount++;
              
            } catch (e) {
              // Skip malformed events
            }
          }
        }
      }
      
      // Process completed listings (mark as sold)
      if (completedRes.ok) {
        const eventData = await completedRes.json();
        
        for (const block of eventData) {
          if (!block.events) continue;
          
          for (const event of block.events) {
            try {
              // Decode the payload
              let payload = event.payload;
              if (typeof payload === 'string') {
                payload = JSON.parse(Buffer.from(payload, 'base64').toString());
              }
              
              if (!payload?.value?.fields) continue;
              
              const fields = payload.value.fields;
              const getField = (name) => {
                const f = fields.find(x => x.name === name);
                if (!f) return null;
                if (f.value?.value?.value) return f.value.value.value;
                if (f.value?.value) return f.value.value;
                return f.value;
              };
              
              // Check if this is an AllDay NFT
              const nftType = fields.find(f => f.name === 'nftType');
              const typeId = nftType?.value?.staticType?.typeID || 
                            nftType?.value?.value?.staticType?.typeID || '';
              
              if (!typeId.includes('AllDay')) continue;
              
              const nftId = getField('nftID')?.toString();
              
              if (!nftId) continue;
              
              // Extract buyer address from the event
              // Try multiple possible field names
              let buyerAddr = getField('purchaser')?.toString()?.toLowerCase() || 
                             getField('buyer')?.toString()?.toLowerCase() ||
                             getField('recipient')?.toString()?.toLowerCase() ||
                             null;
              
              // If not in event fields, try to get from transaction authorizer
              // The buyer is typically the transaction authorizer (the account that signed)
              if (!buyerAddr) {
                // Try to get transaction ID from various locations
                let txId = event.transaction_id || event.transactionId || 
                          block.transaction_id || block.transactions?.[0]?.id;
                
                // Also check if transaction info is in the block structure
                if (!txId && block.transactions && block.transactions.length > 0) {
                  txId = block.transactions[0].id;
                }
                
                // Try to get from block by querying the block directly
                if (!txId && block.block_height) {
                  try {
                    const blockRes = await fetch(`${FLOW_REST_API}/v1/blocks?height=${block.block_height}`);
                    if (blockRes.ok) {
                      const blockData = await blockRes.json();
                      if (blockData?.[0]?.collection_guarantees && blockData[0].collection_guarantees.length > 0) {
                        const collectionId = blockData[0].collection_guarantees[0].collection_id;
                        if (collectionId) {
                          const collectionRes = await fetch(`${FLOW_REST_API}/v1/collections/${collectionId}`);
                          if (collectionRes.ok) {
                            const collectionData = await collectionRes.json();
                            if (collectionData?.transactions && collectionData.transactions.length > 0) {
                              // Find the transaction that contains this ListingCompleted event
                              // We'll try the first transaction (usually correct for single-event transactions)
                              txId = collectionData.transactions[0];
                            }
                          }
                        }
                      }
                    }
                  } catch (e) {
                    // Ignore block fetch errors
                  }
                }
                
                if (txId) {
                  try {
                    // Fetch transaction to get authorizer
                    const txRes = await fetch(`${FLOW_REST_API}/v1/transactions/${txId}`);
                    if (txRes.ok) {
                      const txData = await txRes.json();
                      // The authorizer is typically the first account in the authorization list
                      // For purchases, the payer/authorizer is the buyer
                      if (txData?.payload?.authorizers && txData.payload.authorizers.length > 0) {
                        buyerAddr = txData.payload.authorizers[0]?.toLowerCase();
                      } else if (txData?.payload?.payer) {
                        buyerAddr = txData.payload.payer?.toLowerCase();
                      } else if (txData?.payload?.proposalKey?.address) {
                        buyerAddr = txData.payload.proposalKey.address?.toLowerCase();
                      }
                    }
                  } catch (e) {
                    console.error(`[Sniper] Error fetching transaction ${txId} for buyer:`, e.message);
                  }
                }
              }
              
              // If we still don't have buyer, try to get from Snowflake as fallback
              // This adds a small delay but ensures we get the buyer info
              if (!buyerAddr) {
                try {
                  await ensureSnowflakeConnected();
                  // Query Snowflake for the transaction that completed this listing
                  // The buyer is typically the transaction authorizer (first signer)
                  // We'll get the TX_ID and then fetch the transaction to get the authorizer
                  const snowflakeQuery = `
                    SELECT TX_ID, TX_PAYER_ADDRESS
                    FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
                    WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
                      AND EVENT_TYPE = 'ListingCompleted'
                      AND EVENT_DATA:nftID::STRING = '${nftId}'
                      AND TX_SUCCEEDED = TRUE
                    ORDER BY BLOCK_TIMESTAMP DESC
                    LIMIT 1
                  `;
                  
                  const snowflakeResult = await snowflake.execute({ sqlText: snowflakeQuery });
                  const rows = await snowflakeResult.fetchAll();
                  if (rows.length > 0) {
                    const txId = rows[0].TX_ID;
                    if (txId) {
                      // Fetch transaction from Flow REST API to get authorizer
                      try {
                        const txRes = await fetch(`${FLOW_REST_API}/v1/transactions/${txId}`);
                        if (txRes.ok) {
                          const txData = await txRes.json();
                          if (txData?.payload?.authorizers && txData.payload.authorizers.length > 0) {
                            buyerAddr = txData.payload.authorizers[0]?.toLowerCase();
                            console.log(`[Sniper] Got buyer ${buyerAddr} from transaction ${txId} for ${nftId}`);
                          }
                        }
                      } catch (e) {
                        // Fallback to payer address if transaction fetch fails
                        if (rows[0].TX_PAYER_ADDRESS) {
                          buyerAddr = rows[0].TX_PAYER_ADDRESS.toLowerCase();
                          console.log(`[Sniper] Using payer address ${buyerAddr} as buyer for ${nftId}`);
                        }
                      }
                    }
                  }
                } catch (e) {
                  console.error(`[Sniper] Error querying Snowflake for buyer:`, e.message);
                }
              }
              
              // Mark this NFT as sold with buyer info
              await markListingAsSold(nftId, buyerAddr);
              if (buyerAddr) {
                console.log(`[Sniper] Marked ${nftId} as sold to ${buyerAddr}`);
              } else {
                console.log(`[Sniper] Marked ${nftId} as sold but could not determine buyer`);
              }
              soldCount++;
              
            } catch (e) {
              // Skip malformed events
            }
          }
        }
      }
      
      if (!listingRes.ok && !completedRes.ok) {
        lastCheckedBlock = endHeight;
        return;
      }
      
      if (alldayCount > 0 || soldCount > 0) {
        console.log(`[Sniper] Block ${startHeight}-${endHeight}: ${alldayCount} new listings, ${soldCount} sold`);
      }
      
      lastCheckedBlock = endHeight;
      
    } catch (err) {
      console.error("[Sniper] Error checking for listings:", err.message);
    }
  };
  
  // Check every 2 seconds for real-time sniping
  setInterval(checkForNewListings, 2000);
  checkForNewListings(); // Run immediately
}

// Start watching after server is ready
setTimeout(() => {
  watchForListings();
}, 5000);

// API endpoint to get sniper listings with filtering
app.get("/api/sniper-deals", async (req, res) => {
  try {
    // Get filter params
    const { team, player, tier, minDiscount, maxPrice, maxSerial, dealsOnly } = req.query;
    
    let filtered = [...sniperListings];
    
    // Apply filters
    if (team) {
      const teamLower = team.toLowerCase();
      filtered = filtered.filter(l => l.teamName?.toLowerCase().includes(teamLower));
    }
    
    if (player) {
      const playerLower = player.toLowerCase();
      filtered = filtered.filter(l => l.playerName?.toLowerCase().includes(playerLower));
    }
    
    if (tier) {
      const tierUpper = tier.toUpperCase();
      filtered = filtered.filter(l => l.tier === tierUpper);
    }
    
    if (minDiscount) {
      const minDisc = parseFloat(minDiscount);
      filtered = filtered.filter(l => l.dealPercent >= minDisc);
    }
    
    if (maxPrice) {
      const maxP = parseFloat(maxPrice);
      filtered = filtered.filter(l => l.listingPrice <= maxP);
    }
    
    if (maxSerial) {
      const maxS = parseInt(maxSerial);
      filtered = filtered.filter(l => l.serialNumber && l.serialNumber <= maxS);
    }
    
    if (dealsOnly === 'true') {
      filtered = filtered.filter(l => l.dealPercent > 0);
    }
    
    // Enrich listings with missing ASP and Series data (batch queries for performance)
    // Get all nftIds and editionIds that need enrichment
    const allNftIds = [...new Set(filtered.map(l => l.nftId).filter(Boolean))];
    const allEditionIds = [...new Set(filtered.map(l => l.editionId).filter(Boolean))];
    
    const seriesMapByNftId = new Map();
    const seriesMapByEditionId = new Map();
    const jerseyMapByNftId = new Map();
    const aspMap = new Map();
    
    // Batch fetch series names and jersey numbers by nftId
    if (allNftIds.length > 0) {
      try {
        const metaResult = await pgQuery(
          `SELECT nft_id, series_name, jersey_number FROM nft_core_metadata WHERE nft_id = ANY($1::text[])`,
          [allNftIds]
        );
        metaResult.rows.forEach(row => {
          if (row.series_name) {
            seriesMapByNftId.set(row.nft_id, row.series_name);
          }
          if (row.jersey_number) {
            jerseyMapByNftId.set(row.nft_id, Number(row.jersey_number));
          }
        });
      } catch (e) {
        console.error("[Sniper] Error fetching series names by nftId:", e.message);
      }
    }
    
    // Batch fetch series names by editionId (fallback for listings without nftId)
    if (allEditionIds.length > 0) {
      try {
        const metaResult = await pgQuery(
          `SELECT edition_id, series_name FROM nft_core_metadata WHERE edition_id = ANY($1::text[]) AND series_name IS NOT NULL`,
          [allEditionIds]
        );
        metaResult.rows.forEach(row => {
          if (row.series_name) {
            seriesMapByEditionId.set(row.edition_id, row.series_name);
          }
        });
      } catch (e) {
        console.error("[Sniper] Error fetching series names by editionId:", e.message);
      }
    }
    
    // Batch fetch ASP for all listings
    if (allEditionIds.length > 0) {
      try {
        const priceResult = await pgQuery(
          `SELECT edition_id, avg_sale_usd FROM edition_price_scrape WHERE edition_id = ANY($1::text[]) AND avg_sale_usd IS NOT NULL`,
          [allEditionIds]
        );
        priceResult.rows.forEach(row => {
          const aspValue = row.avg_sale_usd;
          if (aspValue != null) {
            const numValue = Number(aspValue);
            if (!isNaN(numValue) && numValue > 0) {
              aspMap.set(row.edition_id, numValue);
            }
          }
        });
        console.log(`[Sniper API] Fetched ASP for ${aspMap.size} editions out of ${allEditionIds.length} requested`);
      } catch (e) {
        console.error("[Sniper] Error fetching ASP:", e.message);
      }
    }
    
    // Enrich all listings with fetched data
    const enrichedListings = filtered.map(listing => {
      const enriched = { ...listing };
      
      // Fill in seriesName if missing - try nftId first, then editionId
      if (!enriched.seriesName) {
        if (enriched.nftId && seriesMapByNftId.has(enriched.nftId)) {
          enriched.seriesName = seriesMapByNftId.get(enriched.nftId);
        } else if (enriched.editionId && seriesMapByEditionId.has(enriched.editionId)) {
          enriched.seriesName = seriesMapByEditionId.get(enriched.editionId);
        }
      }
      
      // Fill in jerseyNumber if missing
      if (!enriched.jerseyNumber && enriched.nftId && jerseyMapByNftId.has(enriched.nftId)) {
        enriched.jerseyNumber = jerseyMapByNftId.get(enriched.nftId);
      }
      
      // Always try to fill in avgSale if we have an editionId (even if it was previously null)
      if (enriched.editionId) {
        if (aspMap.has(enriched.editionId)) {
          enriched.avgSale = aspMap.get(enriched.editionId);
        } else if (enriched.avgSale === undefined) {
          // Only set to null if it was undefined, don't overwrite if it was explicitly set to null before
          enriched.avgSale = null;
        }
      }
      
      return enriched;
    });
    
    // Debug: Log enrichment stats
    if (enrichedListings.length > 0) {
      const withSeries = enrichedListings.filter(l => l.seriesName).length;
      const withASP = enrichedListings.filter(l => l.avgSale != null && l.avgSale > 0).length;
      const withEditionId = enrichedListings.filter(l => l.editionId).length;
      console.log(`[Sniper API] Enriched ${enrichedListings.length} listings: ${withSeries} with series, ${withASP} with ASP (${withEditionId} have editionId)`);
    }
    
    // Get unique teams and tiers for filter dropdowns
    const allTeams = [...new Set(sniperListings.map(l => l.teamName).filter(Boolean))].sort();
    const allTiers = [...new Set(sniperListings.map(l => l.tier).filter(Boolean))];
    
    // Count deals
    const dealsCount = enrichedListings.filter(l => l.dealPercent > 0).length;
    
    return res.json({
      ok: true,
      listings: enrichedListings,
      total: sniperListings.length,
      filtered: enrichedListings.length,
      dealsCount,
      watching: isWatchingListings,
      lastCheckedBlock,
      floorCacheSize: floorPriceCache.size,
      availableTeams: allTeams,
      availableTiers: allTiers,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error("[Sniper] Error getting listings:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Pre-populate floor cache for active editions
app.get("/api/sniper-warmup", async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    
    // Get editions with recent activity from Snowflake
    await ensureSnowflakeConnected();
    
    const sql = `
      SELECT DISTINCT EVENT_DATA:nftID::STRING AS nft_id
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
        AND EVENT_TYPE = 'ListingAvailable'
        AND EVENT_DATA:nftType:typeID::STRING = 'A.e4cf4bdc1751c65d.AllDay.NFT'
        AND TX_SUCCEEDED = TRUE
        AND BLOCK_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
      LIMIT ${limit}
    `;
    
    const result = await executeSql(sql);
    const nftIds = result.map(r => r.NFT_ID || r.nft_id).filter(Boolean);
    
    // Get edition IDs
    let editionIds = [];
    if (nftIds.length > 0) {
      const metaResult = await pgQuery(
        `SELECT DISTINCT edition_id FROM nft_core_metadata WHERE nft_id = ANY($1::text[])`,
        [nftIds]
      );
      editionIds = metaResult.rows.map(r => r.edition_id).filter(Boolean);
    }
    
    console.log(`[Sniper Warmup] Scraping floors for ${editionIds.length} editions...`);
    
    // Scrape floors in parallel
    let scraped = 0;
    const BATCH_SIZE = 10;
    for (let i = 0; i < editionIds.length; i += BATCH_SIZE) {
      const batch = editionIds.slice(i, i + BATCH_SIZE);
      await Promise.all(batch.map(async (editionId) => {
        const floor = await getCachedFloor(editionId);
        if (floor) scraped++;
      }));
    }
    
    console.log(`[Sniper Warmup] Cached ${scraped} floor prices`);
    
    return res.json({
      ok: true,
      editionsFound: editionIds.length,
      floorsCached: scraped,
      cacheSize: floorPriceCache.size
    });
    
  } catch (err) {
    console.error("[Sniper Warmup] Error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Debug endpoint to find marketplace events with prices
app.get("/api/debug-marketplace-events", async (req, res) => {
  try {
    await ensureSnowflakeConnected();
    
    // Find event types that have price data
    const sql = `
      SELECT DISTINCT
        EVENT_CONTRACT,
        EVENT_TYPE,
        COUNT(*) as event_count
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE (
        EVENT_DATA:price IS NOT NULL 
        OR EVENT_DATA:salePrice IS NOT NULL
        OR EVENT_TYPE ILIKE '%listing%'
        OR EVENT_TYPE ILIKE '%sale%'
        OR EVENT_TYPE ILIKE '%purchase%'
        OR EVENT_TYPE ILIKE '%order%'
      )
      AND TX_SUCCEEDED = TRUE
      AND BLOCK_TIMESTAMP >= DATEADD(day, -3, CURRENT_TIMESTAMP())
      GROUP BY EVENT_CONTRACT, EVENT_TYPE
      ORDER BY event_count DESC
      LIMIT 50
    `;
    
    const result = await executeSql(sql);
    
    return res.json({
      ok: true,
      eventTypes: result
    });
  } catch (err) {
    console.error("Debug marketplace events error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Get sample event data for a specific event type
app.get("/api/debug-event-sample", async (req, res) => {
  try {
    const { contract, type } = req.query;
    if (!contract || !type) {
      return res.status(400).json({ ok: false, error: "Need ?contract=X&type=Y" });
    }
    
    await ensureSnowflakeConnected();
    
    const sql = `
      SELECT 
        EVENT_DATA,
        BLOCK_TIMESTAMP,
        TX_ID
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE EVENT_CONTRACT = '${contract}'
        AND EVENT_TYPE = '${type}'
        AND TX_SUCCEEDED = TRUE
      ORDER BY BLOCK_TIMESTAMP DESC
      LIMIT 5
    `;
    
    const result = await executeSql(sql);
    
    return res.json({
      ok: true,
      samples: result
    });
  } catch (err) {
    console.error("Debug event sample error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/sniper-listings", async (req, res) => {
  try {
    const limitNum = Math.min(parseInt(req.query.limit, 10) || 100, 200);
    const hoursAgo = parseInt(req.query.hours, 10) || 1; // Last 1 hour of listings by default
    
    await ensureSnowflakeConnected();
    
    const cutoffTime = new Date(Date.now() - hoursAgo * 60 * 60 * 1000);
    const cutoffStr = cutoffTime.toISOString().replace('T', ' ').substring(0, 19);
    
    // Query ListingAvailable events for NFL All Day NFTs
    // Filter for nftType containing AllDay contract
    // Also get ListingCompleted to filter out sold listings
    const eventsSql = `
      WITH listings AS (
        SELECT
          EVENT_DATA:nftID::STRING AS nft_id,
          EVENT_DATA:listingResourceID::STRING AS listing_id,
          TRY_TO_DOUBLE(EVENT_DATA:price::STRING) AS listing_price,
          LOWER(EVENT_DATA:storefrontAddress::STRING) AS seller_addr,
          BLOCK_TIMESTAMP AS block_timestamp,
          BLOCK_HEIGHT AS block_height,
          TX_ID AS tx_id
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
        WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
          AND EVENT_TYPE = 'ListingAvailable'
          AND EVENT_DATA:nftType:typeID::STRING = 'A.e4cf4bdc1751c65d.AllDay.NFT'
          AND TX_SUCCEEDED = TRUE
          AND BLOCK_TIMESTAMP >= '${cutoffStr}'
      ),
      completed AS (
        SELECT DISTINCT
          EVENT_DATA:listingResourceID::STRING AS listing_id
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
        WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
          AND EVENT_TYPE = 'ListingCompleted'
          AND TX_SUCCEEDED = TRUE
          AND BLOCK_TIMESTAMP >= '${cutoffStr}'
      )
      SELECT 
        l.nft_id,
        l.listing_id,
        l.listing_price,
        l.seller_addr,
        l.block_timestamp,
        l.block_height,
        l.tx_id,
        CASE WHEN c.listing_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_sold
      FROM listings l
      LEFT JOIN completed c ON l.listing_id = c.listing_id
      WHERE c.listing_id IS NULL  -- Only show unsold listings
      ORDER BY l.block_timestamp DESC
      LIMIT ${limitNum}
    `;
    
    console.log(`[Sniper] Querying active listings from last ${hoursAgo} hours...`);
    
    const salesResult = await executeSql(eventsSql);
    console.log(`[Sniper] Found ${salesResult?.length || 0} active listings`);
    
    if (!salesResult || salesResult.length === 0) {
      return res.json({
        ok: true,
        listings: [],
        message: "No active listings found"
      });
    }
    
    // Get NFT IDs for metadata lookup
    const nftIds = salesResult.map(r => r.NFT_ID || r.nft_id).filter(Boolean);
    
    // Get moment metadata by NFT ID
    let momentData = {}; // keyed by nft_id
    let editionIds = new Set();
    
    if (nftIds.length > 0) {
      try {
        const metaResult = await pgQuery(
          `SELECT nft_id, edition_id, serial_number, first_name, last_name, 
                  team_name, position, tier, set_name, series_name
           FROM nft_core_metadata 
           WHERE nft_id = ANY($1::text[])`,
          [nftIds]
        );
        for (const row of metaResult.rows) {
          momentData[row.nft_id] = row;
          if (row.edition_id) editionIds.add(row.edition_id);
        }
      } catch (err) {
        console.error("[Sniper] Error fetching moment metadata:", err.message);
      }
    }
    
    // Scrape REAL-TIME prices from NFL All Day website
    // This is the key to accurate sniper data!
    let scrapedData = {};
    const editionList = [...editionIds];
    
    if (editionList.length > 0) {
      console.log(`[Sniper] Scraping real-time prices for ${editionList.length} editions...`);
      
      // Scrape in parallel batches of 5 to avoid overwhelming the server
      const BATCH_SIZE = 5;
      for (let i = 0; i < editionList.length; i += BATCH_SIZE) {
        const batch = editionList.slice(i, i + BATCH_SIZE);
        const results = await Promise.all(
          batch.map(async (editionId) => {
            const data = await getCachedEditionData(editionId);
            return { editionId, data };
          })
        );
        
        for (const { editionId, data } of results) {
          if (data) {
            scrapedData[editionId] = data;
          }
        }
      }
      
      console.log(`[Sniper] Got real-time prices for ${Object.keys(scrapedData).length} editions`);
    }
    
    // Get average sale prices from our database to compare with scraped low asks
    let avgSalePrices = {};
    if (editionList.length > 0) {
      try {
        const priceResult = await pgQuery(
          `SELECT edition_id, avg_sale_usd 
           FROM public.edition_price_scrape 
           WHERE edition_id = ANY($1::text[])`,
          [editionList]
        );
        for (const row of priceResult.rows) {
          avgSalePrices[row.edition_id] = row.avg_sale_usd ? Number(row.avg_sale_usd) : null;
        }
      } catch (err) {
        console.error("[Sniper] Error fetching avg sale prices:", err.message);
      }
    }
    
    // Build floorPrices from scraped data, with avgSale for deal comparison
    const floorPrices = {};
    for (const [editionId, data] of Object.entries(scrapedData)) {
      if (data && data.lowAsk) {
        floorPrices[editionId] = { 
          floor: data.lowAsk,
          avgSale: avgSalePrices[editionId] || null
        };
      }
    }
    
    // Get wallet names for sellers
    const sellerAddrs = [...new Set(salesResult.map(r => r.SELLER_ADDR || r.seller_addr).filter(Boolean))];
    
    let walletNames = {};
    if (sellerAddrs.length > 0) {
      try {
        const nameResult = await pgQuery(
          `SELECT wallet_address, display_name FROM wallet_profiles WHERE wallet_address = ANY($1::text[])`,
          [sellerAddrs]
        );
        for (const row of nameResult.rows) {
          walletNames[row.wallet_address] = row.display_name;
        }
      } catch (err) { /* ignore */ }
    }
    
    // Build listing objects showing NEW LISTING PRICE vs CURRENT FLOOR
    const listings = [];
    
    for (const row of salesResult) {
      const nftId = row.NFT_ID || row.nft_id;
      const snowflakeListingPrice = row.LISTING_PRICE || row.listing_price; // Price from Snowflake event
      const sellerAddr = row.SELLER_ADDR || row.seller_addr;
      
      // Get moment data
      const moment = momentData[nftId] || {};
      const editionId = moment.edition_id;
      if (!editionId) continue;
      
      const prices = floorPrices[editionId] || {};
      const currentFloor = prices.floor; // REAL-TIME scraped floor
      
      if (!currentFloor || !snowflakeListingPrice) continue;
      
      // Deal % = how much below CURRENT FLOOR this listing is
      // If floor is $40 and this was listed at $35, that's 12.5% below = DEAL!
      let dealPercent = null;
      if (currentFloor > 0) {
        dealPercent = Math.round(((currentFloor - snowflakeListingPrice) / currentFloor) * 1000) / 10;
      }
      
      const serialNumber = moment.serial_number;
      const isLowSerial = serialNumber && serialNumber <= 100;
      
      // Parse timestamp
      let timestamp = row.BLOCK_TIMESTAMP || row.block_timestamp;
      if (timestamp) {
        if (timestamp instanceof Date) {
          timestamp = timestamp.toISOString();
        } else if (typeof timestamp === 'string' && !timestamp.endsWith('Z')) {
          timestamp = timestamp.replace(' ', 'T') + 'Z';
        }
      }
      
      listings.push({
        nftId,
        editionId,
        serialNumber,
        listingPrice: snowflakeListingPrice, // What this was listed for (from Snowflake)
        currentFloor,                         // Current floor price (scraped)
        dealPercent,                          // % below floor (positive = deal!)
        
        playerName: moment.first_name && moment.last_name 
          ? `${moment.first_name} ${moment.last_name}` 
          : null,
        teamName: moment.team_name,
        tier: moment.tier,
        setName: moment.set_name,
        seriesName: moment.series_name,
        position: moment.position,
        
        sellerName: walletNames[sellerAddr] || sellerAddr,
        sellerAddr,
        isLowSerial,
        listedAt: timestamp,
        
        // Direct link to buy
        listingUrl: `https://nflallday.com/listing/moment/${editionId}`
      });
    }
    
    // Sort by deal percentage (best deals first - highest positive %)
    listings.sort((a, b) => {
      const aDeal = a.dealPercent ?? -999;
      const bDeal = b.dealPercent ?? -999;
      if (bDeal !== aDeal) return bDeal - aDeal;
      return new Date(b.listedAt) - new Date(a.listedAt);
    });
    
    // Count deals (listings below floor)
    const dealsCount = listings.filter(l => l.dealPercent && l.dealPercent > 0).length;
    
    return res.json({
      ok: true,
      listings,
      count: listings.length,
      dealsCount,
      hoursQueried: hoursAgo,
      timestamp: new Date().toISOString()
    });
    
  } catch (err) {
    console.error("[Sniper] Error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch sniper listings: " + (err.message || String(err))
    });
  }
});

// Get active NFTStorefront listings (real-time from Light Node)
app.get("/api/sniper-active-listings", async (req, res) => {
  try {
    const limitNum = Math.min(parseInt(req.query.limit, 10) || 50, 100);
    
    // Get latest block height from Flow
    const latestHeight = await getLatestBlockHeight();
    const endHeight = latestHeight;
    const startHeight = latestHeight - 500; // ~6-7 minutes of blocks
    
    console.log(`[Sniper Active] Querying blocks ${startHeight}-${endHeight} for listing events...`);
    
    // Query NFTStorefront ListingAvailable events
    // These fire when someone creates a new marketplace listing
    const listingUrl = `${FLOW_LIGHT_NODE_URL}/v1/events?type=${STOREFRONT_CONTRACT}.ListingAvailable&start_height=${startHeight}&end_height=${endHeight}`;
    
    const listingRes = await fetch(listingUrl);
    const listingData = listingRes.ok ? await listingRes.json() : [];
    
    // Also query for ListingCompleted to filter out sold items
    const completedUrl = `${FLOW_LIGHT_NODE_URL}/v1/events?type=${STOREFRONT_CONTRACT}.ListingCompleted&start_height=${startHeight}&end_height=${endHeight}`;
    const completedRes = await fetch(completedUrl);
    const completedData = completedRes.ok ? await completedRes.json() : [];
    
    // Build set of completed listing IDs
    const completedListingIds = new Set();
    for (const block of completedData) {
      if (block.events) {
        for (const event of block.events) {
          try {
            const payload = typeof event.payload === 'string' 
              ? JSON.parse(Buffer.from(event.payload, 'base64').toString())
              : event.payload;
            if (payload?.value?.fields) {
              const listingId = payload.value.fields.find(f => f.name === 'listingResourceID')?.value?.value;
              if (listingId) completedListingIds.add(listingId.toString());
            }
          } catch (e) { /* ignore parse errors */ }
        }
      }
    }
    
    // Parse listing events
    const activeListings = [];
    
    for (const block of listingData) {
      if (!block.events) continue;
      
      for (const event of block.events) {
        try {
          const payload = typeof event.payload === 'string'
            ? JSON.parse(Buffer.from(event.payload, 'base64').toString())
            : event.payload;
          
          if (!payload?.value?.fields) continue;
          
          const fields = payload.value.fields;
          const getField = (name) => {
            const f = fields.find(x => x.name === name);
            return f?.value?.value || f?.value;
          };
          
          const listingId = getField('listingResourceID')?.toString();
          
          // Skip if already sold
          if (listingId && completedListingIds.has(listingId)) continue;
          
          // Extract listing data
          const nftId = getField('nftID')?.toString();
          const price = getField('salePrice');
          const seller = getField('storefrontAddress')?.toString()?.toLowerCase();
          
          if (!nftId || !price) continue;
          
          activeListings.push({
            listingId,
            nftId,
            listingPrice: parseFloat(price),
            seller,
            blockHeight: parseInt(block.block_height, 10),
            blockTimestamp: block.block_timestamp
          });
          
        } catch (e) {
          console.error("[Sniper Active] Parse error:", e.message);
        }
      }
    }
    
    // Enrich with metadata and floor prices
    const nftIds = activeListings.map(l => l.nftId).filter(Boolean);
    let momentData = {};
    let editionPrices = {};
    
    if (nftIds.length > 0) {
      try {
        const metaResult = await pgQuery(
          `SELECT nft_id, edition_id, serial_number, first_name, last_name,
                  team_name, position, tier, set_name
           FROM nft_core_metadata WHERE nft_id = ANY($1::text[])`,
          [nftIds]
        );
        
        const editionIds = [];
        for (const row of metaResult.rows) {
          momentData[row.nft_id] = row;
          if (row.edition_id) editionIds.push(row.edition_id);
        }
        
        if (editionIds.length > 0) {
          const priceResult = await pgQuery(
            `SELECT edition_id, lowest_ask_usd FROM edition_price_scrape 
             WHERE edition_id = ANY($1::text[])`,
            [[...new Set(editionIds)]]
          );
          for (const row of priceResult.rows) {
            editionPrices[row.edition_id] = Number(row.lowest_ask_usd);
          }
        }
      } catch (e) {
        console.error("[Sniper Active] Metadata error:", e.message);
      }
    }
    
    // Enrich listings
    const enrichedListings = activeListings.map(listing => {
      const moment = momentData[listing.nftId] || {};
      const floorPrice = editionPrices[moment.edition_id];
      
      let dealPercent = null;
      if (floorPrice && listing.listingPrice && floorPrice > 0) {
        dealPercent = ((floorPrice - listing.listingPrice) / floorPrice) * 100;
      }
      
      return {
        ...listing,
        editionId: moment.edition_id,
        serialNumber: moment.serial_number,
        playerName: moment.first_name && moment.last_name 
          ? `${moment.first_name} ${moment.last_name}` : null,
        teamName: moment.team_name,
        tier: moment.tier,
        setName: moment.set_name,
        floorPrice,
        dealPercent: dealPercent ? Math.round(dealPercent * 10) / 10 : null,
        listingUrl: moment.edition_id 
          ? `https://nflallday.com/listing/moment/${moment.edition_id}` : null
      };
    });
    
    // Sort by deal %
    enrichedListings.sort((a, b) => (b.dealPercent ?? -999) - (a.dealPercent ?? -999));
    
    return res.json({
      ok: true,
      listings: enrichedListings.slice(0, limitNum),
      count: enrichedListings.length,
      blocksQueried: endHeight - startHeight,
      timestamp: new Date().toISOString()
    });
    
  } catch (err) {
    console.error("[Sniper Active] Error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to fetch active listings: " + (err.message || String(err))
    });
  }
});

// Simple health check
app.get("/api/health", async (req, res) => {
  try {
    const pgRes = await pgQuery("SELECT NOW() AS now");

    return res.json({
      ok: true,
      postgres: {
        ok: true,
        now: pgRes.rows[0].now,
      },
      snowflake: {
        connected: snowflakeConnected,
      },
    });
  } catch (err) {
    console.error("Error in /api/health:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
    });
  }
});

// ------------------ Start server ------------------

const port = process.env.PORT || 3000;

// Auto-refresh insights snapshot on startup if missing, then every 6 hours
async function setupInsightsRefresh() {
  try {
    await ensureInsightsSnapshotTable();
    
    // Check if snapshot exists
    const check = await pool.query(`SELECT id FROM insights_snapshot WHERE id = 1;`);
    
    if (!check.rows.length) {
      console.log("📊 No insights snapshot found. Refreshing on startup...");
      // Trigger refresh via internal fetch
      try {
        const response = await fetch(`http://localhost:${port}/api/insights/refresh`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        if (response.ok) {
          console.log("✅ Initial insights snapshot created");
        }
      } catch (err) {
        console.log("⚠️  Could not auto-refresh insights on startup (server not ready yet). Run POST /api/insights/refresh manually.");
      }
    } else {
      const age = await pool.query(`SELECT EXTRACT(EPOCH FROM (now() - updated_at)) / 3600 AS hours_old FROM insights_snapshot WHERE id = 1;`);
      const hoursOld = age.rows[0]?.hours_old || 0;
      console.log(`📊 Insights snapshot exists (${hoursOld.toFixed(1)} hours old). Will auto-refresh every 6 hours.`);
    }
    
    // Set up periodic refresh (every 6 hours)
    setInterval(async () => {
      try {
        console.log("🔄 Auto-refreshing insights snapshot...");
        const response = await fetch(`http://localhost:${port}/api/insights/refresh`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        if (response.ok) {
          const result = await response.json();
          console.log(`✅ Insights snapshot auto-refreshed: ${result.message}`);
        } else {
          console.error("❌ Failed to auto-refresh insights snapshot");
        }
      } catch (err) {
        console.error("❌ Error auto-refreshing insights snapshot:", err.message);
      }
    }, 6 * 60 * 60 * 1000); // 6 hours
    
  } catch (err) {
    console.error("Error setting up insights refresh:", err);
  }
}

app.listen(port, async () => {
  console.log(`NFL ALL DAY collection viewer running on http://localhost:${port}`);
  
  // Set up insights refresh after server starts
  setTimeout(() => {
    setupInsightsRefresh();
  }, 2000); // Wait 2 seconds for server to be fully ready
});
