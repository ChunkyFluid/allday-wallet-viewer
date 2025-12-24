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
import bcrypt from "bcryptjs";
import WebSocket from "ws";
import * as eventProcessor from "./services/event-processor.js";
import { syncLeaderboards } from "./scripts/sync_leaderboards.js";
import crypto from 'crypto';
import * as sniperService from "./services/sniper-service.js";
import { registerSearchRoutes } from "./routes/search.js";
import { registerRarityRoutes } from "./routes/rarity.js";
import { registerSetRoutes } from "./routes/sets.js";
import { registerUtilityRoutes } from "./routes/utilities.js";
import { registerAnalyticsRoutes, initVisitCounterTable } from "./routes/analytics.js";
import { registerInsightsRoutes, ensureInsightsSnapshotTable } from "./routes/insights.js";



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

// Register modular routes
registerSearchRoutes(app);
registerRarityRoutes(app);
registerSetRoutes(app);
registerUtilityRoutes(app);
registerAnalyticsRoutes(app);
registerInsightsRoutes(app);
// Initialize tables
initVisitCounterTable();



// ------------------ Snowflake connection ------------------

let connection = null;
let snowflakeConnected = false;

function createSnowflakeConnection() {
  return snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    role: process.env.SNOWFLAKE_ROLE
  });
}

// Initial creation
connection = createSnowflakeConnection();

function ensureSnowflakeConnected() {
  return new Promise((resolve, reject) => {
    // If we think we're connected, verify it hasn't been terminated
    if (snowflakeConnected && connection && connection.isUp()) {
      return resolve();
    }

    // If connection object exists but is down/terminated, destroy it and recreate
    if (connection && !connection.isUp()) {
      console.log("Snowflake connection appears down, recreating...");
      snowflakeConnected = false;
      try {
        // Create new connection instance
        connection = createSnowflakeConnection();
      } catch (err) {
        return reject(new Error("Failed to recreate Snowflake connection: " + err.message));
      }
    }

    // If connection is null (shouldn't happen with logic above but safe check)
    if (!connection) {
      connection = createSnowflakeConnection();
    }

    connection.connect((err, conn) => {
      if (err) {
        console.error("Snowflake connect error:", err);
        snowflakeConnected = false;
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

// ------------------ Flow WebSocket Stream API for Live Events ------------------

const FLOW_WS_URL = "wss://rest-mainnet.onflow.org/v1/ws";
const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";

// ------------------ FindLab API Integration ------------------
// Alternative data source for Flow blockchain data - often faster and more reliable than direct Flow REST API

const FINDLAB_API_BASE = "https://api.find.xyz";
const FINDLAB_API_TIMEOUT = 10000; // 10 seconds
const FINDLAB_ENABLED = (process.env.FINDLAB_ENABLED || "").toLowerCase() === "true";

// FindLab API wrapper functions with error handling and timeout
async function findlabRequest(endpoint, options = {}) {
  if (!FINDLAB_ENABLED) {
    throw new Error("FindLab disabled");
  }
  const url = `${FINDLAB_API_BASE}${endpoint}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FINDLAB_API_TIMEOUT);

  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...options.headers
      }
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      // Try to get error details from response body
      let errorBody = '';
      try {
        errorBody = await response.text();
      } catch (e) {
        // Ignore
      }

      const errorMsg = `FindLab API error: ${response.status} ${response.statusText}`;
      const error = new Error(errorMsg);
      error.response = response;
      error.body = errorBody;
      throw error;
    }

    const data = await response.json();
    return data;
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === 'AbortError') {
      throw new Error('FindLab API request timed out');
    }
    throw err;
  }
}

// Get latest block height from FindLab API (faster alternative to Flow REST API)
async function getLatestBlockHeightFindLab() {
  if (!FINDLAB_ENABLED) return null;
  try {
    const data = await findlabRequest('/flow/v1/block?limit=1');
    if (data && data.data && data.data.length > 0) {
      const height = parseInt(data.data[0].height || 0);
      if (height > 0) {
        return height;
      }
    }
    console.warn('[FindLab] Block height data structure unexpected:', JSON.stringify(data).substring(0, 200));
    return null;
  } catch (err) {
    console.warn('[FindLab] Failed to get latest block height:', err.message);
    return null;
  }
}

// Get NFT holdings for an address using FindLab API
async function getWalletNFTsFindLab(address, nftType = 'A.e4cf4bdc1751c65d.AllDay') {
  if (!FINDLAB_ENABLED) return null;
  try {
    // Try multiple endpoint formats - FindLab API might use different paths
    const encodedAddress = encodeURIComponent(address);
    const encodedNftType = encodeURIComponent(nftType);

    // Try different endpoint formats
    const endpoints = [
      `/flow/v1/account/${encodedAddress}/nft/${encodedNftType}?limit=1000`,
      `/flow/v1/accounts/${encodedAddress}/nfts?contract=${encodedNftType}&limit=1000`,
      `/flow/v1/nft/${encodedNftType}/owner/${encodedAddress}?limit=1000`,
      `/api/flow/v1/account/${encodedAddress}/nft/${encodedNftType}?limit=1000`
    ];

    for (const endpoint of endpoints) {
      try {
        const data = await findlabRequest(endpoint);

        // Handle different response structures
        let nftArray = null;
        if (data.data && Array.isArray(data.data)) {
          nftArray = data.data;
        } else if (Array.isArray(data)) {
          nftArray = data;
        } else if (data.nfts && Array.isArray(data.nfts)) {
          nftArray = data.nfts;
        } else if (data.items && Array.isArray(data.items)) {
          nftArray = data.items;
        }

        if (nftArray) {
          // Extract NFT IDs from the response
          const nftIds = nftArray.map(item => {
            if (typeof item === 'string' || typeof item === 'number') return String(item);
            return item.id || item.nft_id || item.identifier || item.tokenId || String(item);
          }).filter(Boolean);

          if (nftIds.length > 0 || nftArray.length === 0) {
            // Success - return the IDs (empty array is valid - wallet has no NFTs)
            return nftIds;
          }
        }
      } catch (endpointErr) {
        // Try next endpoint format
        continue;
      }
    }

    // All endpoints failed
    console.warn(`[FindLab] All endpoint formats failed for ${address.substring(0, 10)}...`);
    return null;
  } catch (err) {
    // Log full error details for debugging
    const errorDetails = err.response ? await err.response.text().catch(() => '') : '';
    console.warn(`[FindLab] Failed to get NFTs for ${address.substring(0, 10)}...:`, err.message, errorDetails ? `\nResponse: ${errorDetails.substring(0, 200)}` : '');
    return null;
  }
}

// Get specific NFT item data from FindLab API
async function getNFTItemFindLab(nftType, nftId) {
  if (!FINDLAB_ENABLED) return null;
  try {
    const encodedNftType = encodeURIComponent(nftType);
    const data = await findlabRequest(`/flow/v1/nft/${encodedNftType}/item/${nftId}`);

    if (data.data) {
      return data.data;
    }

    return null;
  } catch (err) {
    console.warn(`[FindLab] Failed to get NFT item ${nftType}/${nftId}:`, err.message);
    return null;
  }
}

// Get account transaction history from FindLab API
async function getAccountTransactionsFindLab(address, limit = 50) {
  if (!FINDLAB_ENABLED) return null;
  try {
    const encodedAddress = encodeURIComponent(address);
    const data = await findlabRequest(`/flow/v1/account/${encodedAddress}/transaction?limit=${Math.min(limit, 100)}`);

    if (data.data && Array.isArray(data.data)) {
      return data.data;
    }

    return [];
  } catch (err) {
    console.warn(`[FindLab] Failed to get transactions for ${address}:`, err.message);
    return null;
  }
}

// Get NFT transfers from FindLab API
async function getNFTTransfersFindLab(nftType, limit = 50) {
  if (!FINDLAB_ENABLED) return null;
  try {
    const encodedNftType = encodeURIComponent(nftType);
    const data = await findlabRequest(`/flow/v1/nft/${encodedNftType}/transfer?limit=${Math.min(limit, 100)}`);

    if (data.data && Array.isArray(data.data)) {
      return data.data;
    }

    return [];
  } catch (err) {
    console.warn(`[FindLab] Failed to get NFT transfers for ${nftType}:`, err.message);
    return null;
  }
}

// Cache of recent live events (keep last 200)
let liveEventsCache = [];
const MAX_LIVE_EVENTS = 200;
let flowWsConnection = null;
let flowWsConnected = false;
let flowWsReconnectTimer = null;
let lastFlowEventTime = null;

// Event types we care about - expanded to include metadata events for self-managed data
const ALLDAY_EVENT_TYPES = eventProcessor.ALLDAY_EVENT_TYPES;

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
        subscription_id: `ad-${Date.now().toString().slice(-12)}`, // Max 20 chars: "ad-" + 12 digits = 15 chars
        action: "subscribe",
        topic: "events",
        arguments: {
          event_types: ALLDAY_EVENT_TYPES
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

        // Handle subscription confirmation (silently)
        if (msg.subscription_id && !msg.events) {
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

    // Process event for new normalized tables (series, sets, plays, editions, nfts, holdings)
    await eventProcessor.processBlockchainEvent(event);

    // Update holdings table in real-time
    if (nftId && eventType === 'Deposit' && toAddr) {
      await updateWalletHoldingOnDeposit(toAddr, nftId, timestamp, blockHeight);
    } else if (nftId && eventType === 'Withdraw' && fromAddr) {
      await updateWalletHoldingOnWithdraw(fromAddr, nftId);
    }

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
           FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`,
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

const NFTLOCKER_CONTRACT = '0xb6f2481eba4df97b';

// Update wallet holdings in real-time when we see Deposit events
async function updateWalletHoldingOnDeposit(walletAddress, nftId, timestamp, blockHeight) {
  try {
    if (!walletAddress || !nftId) return;

    const walletAddr = walletAddress.toLowerCase();

    // SAFEGUARD: If depositing to the NFTLocker, don't update ownership.
    // The NFT still "belongs" to the original owner, just with is_locked=true (handled by NFTLocked event).
    if (walletAddr === NFTLOCKER_CONTRACT) {
      console.log(`[Wallet Sync] Deposit to Locker for NFT ${nftId} - skipping ownership change`);
      return;
    }

    const ts = timestamp ? new Date(timestamp) : new Date();

    // 1. Remove from ANY other wallet (Transfer logic)
    // This is safer than deleting on Withdraw because it preserves locked NFTs during the lock process.
    await pgQuery(
      `DELETE FROM holdings WHERE nft_id = $1 AND wallet_address != $2`,
      [nftId.toString(), walletAddr]
    );

    // 2. Upsert: add NFT to new wallet holdings
    await pgQuery(
      `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (wallet_address, nft_id) 
       DO UPDATE SET 
         is_locked = FALSE,
         acquired_at = CASE 
           WHEN holdings.acquired_at IS NULL OR holdings.acquired_at < EXCLUDED.acquired_at 
           THEN EXCLUDED.acquired_at 
           ELSE holdings.acquired_at 
         END`,
      [walletAddr, nftId.toString(), false, ts]
    );

    console.log(`[Wallet Sync] ✅ Transferred NFT ${nftId} to wallet ${walletAddr.substring(0, 8)}...`);

    // NEW: Fetch metadata for this NFT if not already in database
    await fetchAndCacheNFTMetadata(nftId.toString());

  } catch (err) {
    // Don't spam logs for expected errors (e.g., duplicate key during race conditions)
    if (!err.message.includes('duplicate') && !err.message.includes('already exists')) {
      console.error(`[Wallet Sync] Error updating wallet holding for Deposit:`, err.message);
    }
  }
}

// Fetch NFT metadata on-demand and cache in nft_core_metadata_v2 table
// Uses Snowflake for metadata lookup (most reliable source)
async function fetchAndCacheNFTMetadata(nftId) {
  try {
    // Check if metadata already exists
    const existing = await pgQuery(
      `SELECT 1 FROM nft_core_metadata_v2 WHERE nft_id = $1`,
      [nftId]
    );

    if (existing.rowCount > 0) {
      return; // Already have metadata
    }

    console.log(`[Metadata Sync] Fetching metadata for new NFT ${nftId}...`);

    // Try Snowflake first (most complete data)
    try {
      await ensureSnowflakeConnected();

      const sql = `
        SELECT
          m.NFT_ID AS nft_id,
          m.EDITION_ID AS edition_id,
          m.PLAY_ID AS play_id,
          m.SERIES_ID AS series_id,
          m.SET_ID AS set_id,
          m.TIER AS tier,
          TRY_TO_NUMBER(m.SERIAL_NUMBER) AS serial_number,
          TRY_TO_NUMBER(m.MAX_MINT_SIZE) AS max_mint_size,
          m.FIRST_NAME AS first_name,
          m.LAST_NAME AS last_name,
          m.TEAM_NAME AS team_name,
          m.POSITION AS position,
          TRY_TO_NUMBER(m.JERSEY_NUMBER) AS jersey_number,
          m.SERIES_NAME AS series_name,
          m.SET_NAME AS set_name
        FROM ALLDAY_CORE_NFT_METADATA m
        WHERE m.NFT_ID = '${nftId}'
        LIMIT 1
      `;

      const rows = await executeSnowflakeQuery(sql);

      if (rows && rows.length > 0) {
        const row = rows[0];

        // Sanitize integer fields
        const sanitizeInt = (val) => {
          if (val === null || val === undefined || val === '' || val === 'null') return null;
          const parsed = parseInt(val, 10);
          return isNaN(parsed) ? null : parsed;
        };

        const serialNumber = sanitizeInt(row.SERIAL_NUMBER || row.serial_number);
        const maxMintSize = sanitizeInt(row.MAX_MINT_SIZE || row.max_mint_size);
        const jerseyNumber = sanitizeInt(row.JERSEY_NUMBER || row.jersey_number);

        await pgQuery(`
          INSERT INTO nft_core_metadata_v2 (
            nft_id, edition_id, play_id, series_id, set_id, tier,
            serial_number, max_mint_size, first_name, last_name,
            team_name, position, jersey_number, series_name, set_name
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
          ON CONFLICT (nft_id) DO UPDATE SET
            edition_id = EXCLUDED.edition_id,
            tier = EXCLUDED.tier,
            serial_number = EXCLUDED.serial_number,
            max_mint_size = EXCLUDED.max_mint_size,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            team_name = EXCLUDED.team_name,
            series_name = EXCLUDED.series_name,
            set_name = EXCLUDED.set_name
        `, [
          nftId,
          row.EDITION_ID || row.edition_id,
          row.PLAY_ID || row.play_id,
          row.SERIES_ID || row.series_id,
          row.SET_ID || row.set_id,
          row.TIER || row.tier,
          serialNumber,
          maxMintSize,
          row.FIRST_NAME || row.first_name,
          row.LAST_NAME || row.last_name,
          row.TEAM_NAME || row.team_name,
          row.POSITION || row.position,
          jerseyNumber,
          row.SERIES_NAME || row.series_name,
          row.SET_NAME || row.set_name
        ]);

        console.log(`[Metadata Sync] ✅ Cached metadata for NFT ${nftId} (${row.FIRST_NAME || row.first_name} ${row.LAST_NAME || row.last_name})`);
      } else {
        console.log(`[Metadata Sync] ⚠️ No metadata found for NFT ${nftId} in Snowflake (may be very new)`);
      }
    } catch (sfErr) {
      console.warn(`[Metadata Sync] Snowflake query failed for NFT ${nftId}:`, sfErr.message);
    }
  } catch (err) {
    console.error(`[Metadata Sync] Error fetching metadata for NFT ${nftId}:`, err.message);
  }
}

// Helper to execute Snowflake queries (used for on-demand metadata fetch)
async function executeSnowflakeQuery(sql) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows || []);
        }
      }
    });
  });
}

// Remove wallet holding when we see Withdraw events
// MODIFIED: We no longer delete on Withdraw to prevent losing locked NFTs.
// Ownership is now updated in handleDeposit (Transfer on Deposit).
async function updateWalletHoldingOnWithdraw(walletAddress, nftId) {
  try {
    if (!walletAddress || !nftId) return;
    const walletAddr = walletAddress.toLowerCase();
    // We just log it for debug, but don't delete. 
    // If it's a sale, the Deposit to the buyer will handle the removal.
    // If it's a lock, it stays in the wallet (correctly).
    console.log(`[Wallet Sync] Withdrawal detected for NFT ${nftId} from ${walletAddr.substring(0, 8)}... (waiting for Deposit to change ownership)`);
  } catch (err) {
    console.error(`[Wallet Sync] Error updating wallet holding for Withdraw:`, err.message);
  }
}

// Manually refresh a wallet's holdings using FindLab API (catch-up mechanism)
app.post("/api/wallet/refresh", async (req, res) => {
  try {
    const { wallet } = req.body;
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing wallet address" });
    }

    const walletAddr = wallet.toString().trim().toLowerCase();
    if (!/^0x[0-9a-f]{4,64}$/.test(walletAddr)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    console.log(`[Wallet Refresh] Fetching current holdings for ${walletAddr.substring(0, 8)}...`);

    // Try new Flow blockchain service first (most reliable)
    let nftIds = null;

    try {
      const flowService = await import("./services/flow-blockchain.js");
      const nftIdNumbers = await flowService.getWalletNFTIds(walletAddr);
      if (nftIdNumbers && nftIdNumbers.length >= 0) {
        nftIds = nftIdNumbers.map(id => id.toString());
        console.log(`[Wallet Refresh] Flow blockchain service returned ${nftIds.length} NFTs`);
      }
    } catch (err) {
      console.warn(`[Wallet Refresh] Flow blockchain service failed, trying FindLab API...`, err.message);
    }

    // Fallback to FindLab API
    if (!nftIds || nftIds.length === 0) {
      try {
        nftIds = await getWalletNFTsFindLab(walletAddr, 'A.e4cf4bdc1751c65d.AllDay');
        if (nftIds && nftIds.length >= 0) {
          console.log(`[Wallet Refresh] FindLab API returned ${nftIds.length} NFTs`);
        }
      } catch (err) {
        console.warn(`[Wallet Refresh] FindLab API failed, trying Flow REST API...`);
      }
    }

    // Final fallback to Flow REST API
    if (!nftIds || nftIds.length === 0) {
      try {
        nftIds = await fetchWalletNFTsViaFlowAPI(walletAddr);
        if (nftIds && nftIds.length >= 0) {
          console.log(`[Wallet Refresh] Flow REST API returned ${nftIds.length} NFTs`);
        }
      } catch (err) {
        console.warn(`[Wallet Refresh] Flow REST API failed:`, err.message);
      }
    }

    // If still no NFTs, treat as empty wallet
    if (!nftIds || nftIds.length === 0) {
      // Wallet has no NFTs - remove all holdings
      const deleteResult = await pgQuery(
        `DELETE FROM holdings WHERE wallet_address = $1`,
        [walletAddr]
      );

      return res.json({
        ok: true,
        wallet: walletAddr,
        message: "Wallet refreshed - no NFTs found",
        added: 0,
        removed: deleteResult.rowCount,
        current: 0
      });
    }

    // Get current holdings from database
    const currentResult = await pgQuery(
      `SELECT nft_id FROM holdings WHERE wallet_address = $1`,
      [walletAddr]
    );
    const currentNftIds = new Set(currentResult.rows.map(r => r.nft_id));
    const newNftIds = new Set(nftIds.map(id => id.toString()));

    // Find NFTs to add (in new list but not in current)
    const toAdd = nftIds.filter(id => !currentNftIds.has(id.toString()));

    // Find NFTs to remove (in current but not in new)
    const toRemove = Array.from(currentNftIds).filter(id => !newNftIds.has(id));

    // Add new NFTs
    let added = 0;
    if (toAdd.length > 0) {
      const now = new Date();
      const values = toAdd.map((nftId, idx) =>
        `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4})`
      ).join(', ');

      const params = toAdd.flatMap(nftId => [walletAddr, nftId.toString(), false, now]);

      await pgQuery(
        `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
         VALUES ${values}
         ON CONFLICT (wallet_address, nft_id) 
         DO UPDATE SET 
           is_locked = FALSE,
           acquired_at = EXCLUDED.acquired_at`,
        params
      );
      added = toAdd.length;
    }

    // Remove NFTs that are no longer in the wallet
    let removed = 0;
    if (toRemove.length > 0) {
      const result = await pgQuery(
        `DELETE FROM holdings 
         WHERE wallet_address = $1 AND nft_id = ANY($2:: text[])`,
        [walletAddr, toRemove]
      );
      removed = result.rowCount;
    }

    console.log(`[Wallet Refresh] ✅ Updated wallet ${walletAddr.substring(0, 8)}... - Added: ${added}, Removed: ${removed}, Total: ${newNftIds.size}`);

    // NEW: Fetch missing metadata for all NFTs in wallet
    const nftIdsArray = Array.from(newNftIds);
    let metadataFetched = 0;

    // Check which NFTs are missing metadata
    const metadataCheck = await pgQuery(
      `SELECT nft_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1:: text[])`,
      [nftIdsArray]
    );
    const existingMetadata = new Set(metadataCheck.rows.map(r => r.nft_id));
    const missingMetadata = nftIdsArray.filter(id => !existingMetadata.has(id));

    if (missingMetadata.length > 0) {
      console.log(`[Wallet Refresh]Fetching metadata for ${missingMetadata.length} NFTs missing metadata...`);

      // Fetch in parallel but limit concurrency
      const BATCH_SIZE = 10;
      for (let i = 0; i < missingMetadata.length; i += BATCH_SIZE) {
        const batch = missingMetadata.slice(i, i + BATCH_SIZE);
        await Promise.all(batch.map(nftId => fetchAndCacheNFTMetadata(nftId)));
        metadataFetched += batch.length;
      }
    }

    return res.json({
      ok: true,
      wallet: walletAddr,
      message: "Wallet refreshed successfully",
      added,
      removed,
      current: newNftIds.size,
      metadataFetched,
      details: {
        addedNftIds: toAdd,
        removedNftIds: toRemove
      }
    });
  } catch (err) {
    console.error("[Wallet Refresh] Error:", err.message);
    return res.status(500).json({
      ok: false,
      error: "Failed to refresh wallet: " + (err.message || String(err))
    });
  }
});

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
    // const passwordHash = "DISABLED_BCRYPT_MISSING";

    const insertSql = `
      INSERT INTO public.users(email, password_hash)
VALUES($1, $2)
      ON CONFLICT(email) DO NOTHING
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
    // const match = false; console.error("Login disabled: bcrypt missing");
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
    // const valid = false;
    if (!valid) {
      return res.status(400).json({ ok: false, error: "Current password is incorrect" });
    }

    // Hash new password and update
    const newHash = await bcrypt.hash(new_password, 10);
    // const newHash = "DISABLED";
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

    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const pattern = `% ${qRaw}% `;
    const result = await pgQuery(
      `
      SELECT wallet_address, display_name
      FROM wallet_profiles
      WHERE display_name ILIKE $1
      ORDER BY display_name ASC, wallet_address ASC
      LIMIT $2;
`,
      [pattern, limit]
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

// Edition search (explorer) - groups by edition_id
app.get("/api/search-editions", async (req, res) => {
  try {
    const { player = "", team = "", tier = "", series = "", set = "", position = "", limit } = req.query;

    const rawLimit = parseInt(limit, 10);
    const safeLimit = Math.min(Math.max(rawLimit || 5000, 1), 10000);

    // Parse comma-separated values for multi-select filters
    const tiers = tier ? tier.split(',').map(t => t.trim()).filter(Boolean) : [];
    const seriesList = series ? series.split(',').map(s => s.trim()).filter(Boolean) : [];
    const sets = set ? set.split(',').map(s => s.trim()).filter(Boolean) : [];
    const positions = position ? position.split(',').map(p => p.trim()).filter(Boolean) : [];

    const conditionsSnapshot = [];
    const conditionsLive = [];
    const params = [];
    let idx = 1;

    if (player) {
      conditionsSnapshot.push(`LOWER(first_name || ' ' || last_name) LIKE LOWER($${idx})`);
      conditionsLive.push(`LOWER(e.first_name || ' ' || e.last_name) LIKE LOWER($${idx})`);
      params.push(`% ${player}% `);
      idx++;
    }

    if (team) {
      conditionsSnapshot.push(`LOWER(team_name) = LOWER($${idx})`);
      conditionsLive.push(`LOWER(e.team_name) = LOWER($${idx})`);
      params.push(team);
      idx++;
    }

    // Tier filter (multi-select)
    if (tiers.length > 0) {
      const placeholders = tiers.map((_, i) => `$${idx + i} `).join(', ');
      conditionsSnapshot.push(`LOWER(tier) IN(${placeholders.split(', ').map(p => `LOWER(${p})`).join(', ')})`);
      conditionsLive.push(`LOWER(e.tier) IN(${placeholders.split(', ').map(p => `LOWER(${p})`).join(', ')})`);
      params.push(...tiers);
      idx += tiers.length;
    }

    // Series filter (multi-select)
    if (seriesList.length > 0) {
      const placeholders = seriesList.map((_, i) => `$${idx + i} `).join(', ');
      conditionsSnapshot.push(`series_name IN(${placeholders})`);
      conditionsLive.push(`e.series_name IN(${placeholders})`);
      params.push(...seriesList);
      idx += seriesList.length;
    }

    // Set filter (multi-select)
    if (sets.length > 0) {
      const placeholders = sets.map((_, i) => `$${idx + i} `).join(', ');
      conditionsSnapshot.push(`set_name IN(${placeholders})`);
      conditionsLive.push(`e.set_name IN(${placeholders})`);
      params.push(...sets);
      idx += sets.length;
    }

    // Position filter (multi-select)
    if (positions.length > 0) {
      const placeholders = positions.map((_, i) => `$${idx + i} `).join(', ');
      conditionsSnapshot.push(`position IN(${placeholders})`);
      conditionsLive.push(`e.position IN(${placeholders})`);
      params.push(...positions);
      idx += positions.length;
    }

    const whereClauseSnapshot = conditionsSnapshot.length ? `WHERE ${conditionsSnapshot.join(" AND ")} ` : "";
    const whereClauseLive = conditionsLive.length ? `WHERE ${conditionsLive.join(" AND ")} ` : "";

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
          FROM nft_core_metadata_v2 e
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
      FROM nft_core_metadata_v2
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
    const safeLimit = Math.min(Math.max(rawLimit || 5000, 1), 20000);

    const conditions = [];
    const params = [];
    let idx = 1;

    if (player) {
      conditions.push(`LOWER(first_name || ' ' || last_name) LIKE LOWER($${idx++})`);
      params.push(`% ${player}% `);
    }

    if (team) {
      conditions.push(`team_name = $${idx++} `);
      params.push(team);
    }

    if (tier) {
      conditions.push(`LOWER(tier) = LOWER($${idx++})`);
      params.push(tier);
    }

    if (series) {
      conditions.push(`series_name = $${idx++} `);
      params.push(series);
    }

    if (set) {
      conditions.push(`set_name = $${idx++} `);
      params.push(set);
    }

    if (position) {
      conditions.push(`position = $${idx++} `);
      params.push(position);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")} ` : "";

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
      FROM nft_core_metadata_v2
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
        FROM nft_core_metadata_v2
        WHERE first_name IS NOT NULL
          AND first_name <> ''
          AND last_name IS NOT NULL
          AND last_name <> ''
        ORDER BY last_name, first_name
        LIMIT 5000;
`),
      pgQuery(`
        SELECT DISTINCT team_name
        FROM nft_core_metadata_v2
        WHERE team_name IS NOT NULL
          AND team_name <> ''
        ORDER BY team_name
        LIMIT 1000;
`),
      pgQuery(`
        SELECT DISTINCT series_name
        FROM nft_core_metadata_v2
        WHERE series_name IS NOT NULL
          AND series_name <> ''
        ORDER BY series_name
        LIMIT 1000;
`),
      pgQuery(`
        SELECT DISTINCT set_name
        FROM nft_core_metadata_v2
        WHERE set_name IS NOT NULL
          AND set_name <> ''
        ORDER BY set_name
        LIMIT 2000;
`),
      pgQuery(`
        SELECT DISTINCT position
        FROM nft_core_metadata_v2
        WHERE position IS NOT NULL
          AND position <> ''
        ORDER BY position
        LIMIT 100;
`),
      pgQuery(`
        SELECT DISTINCT tier
        FROM nft_core_metadata_v2
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
  if let collectionRef = account.capabilities.borrow <& { NonFungibleToken.CollectionPublic } > (
    /public/AllDayNFTCollection
  ) {
    return collectionRef.getIDs()
  }

  return []
}
`;

// Fetch wallet's AllDay NFTs using Flow REST API with Cadence script
async function fetchWalletNFTsViaFlowAPI(walletAddress) {
  try {
    // Convert wallet address to Flow format
    // Flow addresses need to be in hex format with 0x prefix, padded to 16 characters (8 bytes)
    let flowAddress = walletAddress.toLowerCase().trim();
    if (flowAddress.startsWith('0x')) {
      flowAddress = flowAddress.substring(2);
    }

    // Ensure address is properly formatted (remove any extra characters, pad if needed)
    flowAddress = flowAddress.replace(/[^0-9a-f]/g, '');

    // Flow REST API expects arguments in a specific format
    // The address needs to be in the correct format
    const flowAddressWithPrefix = `0x${flowAddress} `;

    // Flow REST API v1 scripts endpoint expects arguments as an array
    // Each argument is a JSON-CDC encoded value
    // For Address, we pass it as: { "type": "Address", "value": "0x..." }
    const addressArg = {
      type: "Address",
      value: flowAddressWithPrefix
    };

    const scriptResponse = await fetch(`${FLOW_REST_API} /v1/scripts`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        script: GET_ALLDAY_NFTS_SCRIPT,
        arguments: [addressArg] // Array of argument objects
      }),
      signal: AbortSignal.timeout(20000) // 20 second timeout (scripts can be slow)
    });

    if (!scriptResponse.ok) {
      const errorText = await scriptResponse.text().catch(() => '');
      throw new Error(`Flow API error: ${scriptResponse.status} ${scriptResponse.statusText} - ${errorText.substring(0, 200)} `);
    }

    const scriptData = await scriptResponse.json();

    // The response should contain the NFT IDs array (as UInt64 values)
    if (scriptData && Array.isArray(scriptData)) {
      const nftIds = scriptData.map(id => String(id)).filter(Boolean);
      return nftIds;
    } else if (scriptData?.value) {
      // Sometimes the response is wrapped in a value field
      const value = scriptData.value;
      if (Array.isArray(value)) {
        const nftIds = value.map(id => String(id)).filter(Boolean);
        return nftIds;
      }
    }

    return [];
  } catch (err) {
    console.warn(`[LiveWallet] Flow REST API script execution failed for ${walletAddress.substring(0, 10)}...: `, err.message);
    return null;
  }
}

// Fetch wallet's AllDay NFTs - try multiple methods
async function fetchLiveWalletNFTs(walletAddress) {
  // Try FindLab API first (fastest if it works)
  try {
    const nftIds = await getWalletNFTsFindLab(walletAddress, 'A.e4cf4bdc1751c65d.AllDay');
    if (nftIds && nftIds.length >= 0) {
      console.log(`[LiveWallet] FindLab API returned ${nftIds.length} NFTs for ${walletAddress.substring(0, 10)}...`);
      return nftIds;
    }
  } catch (err) {
    // Continue to next method
  }

  // Fallback to Flow REST API with Cadence script
  try {
    const nftIds = await fetchWalletNFTsViaFlowAPI(walletAddress);
    if (nftIds && nftIds.length >= 0) {
      console.log(`[LiveWallet] Flow REST API returned ${nftIds.length} NFTs for ${walletAddress.substring(0, 10)}...`);
      return nftIds;
    }
  } catch (err) {
    console.warn(`[LiveWallet] Flow REST API failed: `, err.message);
  }

  // Final fallback: query recent Deposit events for this wallet (last 7 days)
  // This is a compromise - we can't get ALL NFTs this way, but we can get recently received ones
  try {
    const recentNfts = await fetchRecentWalletNFTsViaEvents(walletAddress);
    if (recentNfts && recentNfts.length > 0) {
      console.log(`[LiveWallet] Found ${recentNfts.length} recent NFTs via events for ${walletAddress.substring(0, 10)}...`);
      // Combine with database holdings for complete picture
      return recentNfts;
    }
  } catch (err) {
    console.warn(`[LiveWallet] Event - based fetch failed: `, err.message);
  }

  // All methods failed - return null to use database
  return null;
}

// Fetch recent NFTs by querying Deposit events for the wallet
async function fetchRecentWalletNFTsViaEvents(walletAddress) {
  try {
    // Get latest block height
    const latestHeight = await getLatestBlockHeightFindLab() || await getLatestBlockHeight();
    if (!latestHeight) return null;

    // Query last 7 days of blocks (~1.2M blocks = 7 days * 24 hours * 60 min * 60 sec * 2 blocks/sec)
    const blocksPerDay = 24 * 60 * 60 * 2; // ~172,800 blocks per day
    const startHeight = Math.max(0, latestHeight - (7 * blocksPerDay));

    const walletAddr = walletAddress.toLowerCase();

    // Query Deposit events where this wallet is the recipient
    const depositRes = await fetch(
      `${FLOW_REST_API} /v1/events ? type = A.e4cf4bdc1751c65d.AllDay.Deposit & start_height=${startHeight}& end_height=${latestHeight} `,
      { signal: AbortSignal.timeout(10000) }
    );

    if (!depositRes.ok) return null;

    const depositData = await depositRes.json();
    const nftIds = new Set();

    for (const block of depositData) {
      if (!block.events) continue;
      for (const event of block.events) {
        try {
          let payload = event.payload;
          if (typeof payload === 'string') {
            payload = JSON.parse(Buffer.from(payload, 'base64').toString());
          }

          if (payload?.value?.fields) {
            const fields = payload.value.fields;
            const toField = fields.find(f => f.name === 'to');
            const idField = fields.find(f => f.name === 'id');

            if (toField && idField) {
              const toAddr = (toField.value?.value || toField.value || '').toString().toLowerCase();
              const nftId = (idField.value?.value || idField.value || '').toString();

              if (toAddr === walletAddr && nftId) {
                nftIds.add(nftId);
              }
            }
          }
        } catch (e) {
          // Skip malformed events
        }
      }
    }

    return Array.from(nftIds);
  } catch (err) {
    console.warn(`[LiveWallet] Event - based fetch error: `, err.message);
    return null;
  }
}

// Wallet summary - OPTIMIZED FOR FAST LOADING!
// Default: database (fast), use ?live=true for real-time blockchain data
app.get("/api/wallet-summary", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    // Default to database (fast) - use ?live=true for real-time blockchain data
    const useLive = req.query.live === 'true';


    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    // Basic Flow/Dapper-style address check
    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // Try to fetch live data from blockchain using new Flow blockchain service
    let liveNftIds = null;
    let dataSource = 'database';
    const useBlockchain = req.query.source === 'blockchain' || useLive;

    if (useBlockchain) {
      try {
        console.log(`[Wallet Summary] Fetching live data from Flow blockchain for ${wallet}...`);
        const flowService = await import("./services/flow-blockchain.js");

        // Fetch BOTH unlocked (in wallet) and locked (in NFTLocker) NFT IDs
        const [unlockedIds, blockchainLockedIds] = await Promise.all([
          flowService.getWalletNFTIds(wallet).catch(() => []),
          flowService.getLockedNFTIds ? flowService.getLockedNFTIds(wallet).catch(() => []) : Promise.resolve([])
        ]);

        // If blockchain doesn't return locked NFTs, fall back to database
        let lockedIds = blockchainLockedIds;
        if (lockedIds.length === 0) {
          // Get locked NFT IDs from database (synced from Snowflake)
          const lockedResult = await pgQuery(
            `SELECT nft_id FROM holdings WHERE wallet_address = $1 AND is_locked = true`,
            [wallet]
          );
          lockedIds = lockedResult.rows.map(r => r.nft_id);
          if (lockedIds.length > 0) {
            console.log(`[Wallet Summary] Got ${lockedIds.length} locked NFTs from database(fallback)`);
          }
        }

        // Combine both, tracking which are locked
        // ACTUALLY deduplicate - an NFT might appear in both unlocked (blockchain) and locked (db) during sync
        const lockedSet = new Set(lockedIds.map(id => id.toString()));
        const seenIds = new Set();
        const allNftIds = [];

        // Add unlocked first (from blockchain - fresh data)
        for (const id of unlockedIds) {
          const idStr = id.toString();
          if (!seenIds.has(idStr)) {
            seenIds.add(idStr);
            allNftIds.push({ id: idStr, is_locked: false });
          }
        }

        // Add locked (from database) - only if not already seen
        for (const id of lockedIds) {
          const idStr = id.toString();
          if (!seenIds.has(idStr)) {
            seenIds.add(idStr);
            allNftIds.push({ id: idStr, is_locked: true });
          }
        }

        liveNftIds = allNftIds.map(n => n.id);
        // Store locked status for the query
        req.lockedNftIds = lockedSet;

        dataSource = 'blockchain';
        console.log(`[Wallet Summary] Got ${unlockedIds.length} unlocked + ${lockedIds.length} locked = ${allNftIds.length} total NFTs(deduplicated)`);
      } catch (blockchainErr) {
        console.warn(`[Wallet Summary] Flow blockchain query failed, falling back to database: `, blockchainErr.message);
        // Try fallback to FindLab API
        try {
          liveNftIds = await fetchLiveWalletNFTs(wallet);
          if (liveNftIds !== null) {
            dataSource = 'blockchain';
            console.log(`[Wallet Summary] Got ${liveNftIds.length} NFTs from FindLab API(fallback)`);
          }
        } catch (fallbackErr) {
          console.warn(`[Wallet Summary] All blockchain queries failed, using database`);
        }
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
      // ALWAYS use blockchain NFT IDs as source of truth for which NFTs exist
      statsResult = await pgQuery(
        `
SELECT
          $1 AS wallet_address,

  COUNT(*)::int AS moments_total,
COUNT(*) FILTER(WHERE COALESCE(h.is_locked, false))::int AS locked_count,
  COUNT(*) FILTER(WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count,

    COUNT(*) FILTER(WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int     AS common_count,
      COUNT(*) FILTER(WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int   AS uncommon_count,
        COUNT(*) FILTER(WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int       AS rare_count,
          COUNT(*) FILTER(WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int  AS legendary_count,
            COUNT(*) FILTER(WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int   AS ultimate_count,

              COALESCE(SUM(COALESCE(eps.lowest_ask_usd, 0)), 0)::numeric AS floor_value,
                COALESCE(SUM(COALESCE(eps.avg_sale_usd, 0)), 0)::numeric AS asp_value,
                  COUNT(*) FILTER(WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL)::int AS priced_moments

FROM(SELECT unnest(ARRAY[${nftIdPlaceholders}]:: text[]) AS nft_id) nft_ids
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id:: text
        LEFT JOIN holdings h 
          ON h.nft_id = nft_ids.nft_id:: text 
          AND LOWER(h.wallet_address) = LOWER($1)
        LEFT JOIN public.edition_price_scrape eps
          ON eps.edition_id = m.edition_id;
`,
        [wallet, ...liveNftIds]
      );

      console.log(`[Wallet Summary] Using blockchain data: ${statsResult.rows[0]?.moments_total || 0} total, ${statsResult.rows[0]?.locked_count || 0} locked(from db join)`);

      // Override locked/unlocked counts with blockchain data (more accurate)
      if (req.lockedNftIds && req.lockedNftIds.size > 0) {
        const lockedCount = req.lockedNftIds.size;
        const unlockedCount = liveNftIds.length - lockedCount;
        statsResult.rows[0].locked_count = lockedCount;
        statsResult.rows[0].unlocked_count = unlockedCount;
        console.log(`[Wallet Summary] Overriding with blockchain locked data: ${lockedCount} locked, ${unlockedCount} unlocked`);
      }
    } else if (liveNftIds !== null && liveNftIds.length === 0) {
      // Live data returned empty wallet
      statsResult = {
        rows: [{
          wallet_address: wallet,
          moments_total: 0, locked_count: 0, unlocked_count: 0,
          common_count: 0, uncommon_count: 0, rare_count: 0, legendary_count: 0, ultimate_count: 0,
          floor_value: 0, asp_value: 0, priced_moments: 0
        }]
      };
    } else {
      // Fall back to database snapshot - USING LIVE HOLDINGS TABLE
      statsResult = await pgQuery(
        `
SELECT
h.wallet_address,

  COUNT(*)::int AS moments_total,
    COUNT(*) FILTER(WHERE COALESCE(h.is_locked, false))::int AS locked_count,
      COUNT(*) FILTER(WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count,

        COUNT(*) FILTER(WHERE UPPER(m.tier) = 'COMMON')::int     AS common_count,
          COUNT(*) FILTER(WHERE UPPER(m.tier) = 'UNCOMMON')::int   AS uncommon_count,
            COUNT(*) FILTER(WHERE UPPER(m.tier) = 'RARE')::int       AS rare_count,
              COUNT(*) FILTER(WHERE UPPER(m.tier) = 'LEGENDARY')::int  AS legendary_count,
                COUNT(*) FILTER(WHERE UPPER(m.tier) = 'ULTIMATE')::int   AS ultimate_count,

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

                      COUNT(*) FILTER(WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL):: int
          AS priced_moments

      FROM holdings h
      LEFT JOIN nft_core_metadata_v2 m
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
MAX(acquired_at)  AS last_event_ts,
  MAX(last_synced_at) AS last_synced_at
      FROM holdings
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
      WHERE edition_id = ANY($1:: text[])
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

// Full wallet query - OPTIMIZED FOR FAST LOADING!
// Default: database (fast cached data), use ?source=blockchain for real-time (slower)
app.get("/api/query", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    const forceRefresh = req.query.refresh === 'true' || req.query.refresh === '1';
    // Default to database source for FAST loading (cached data from sync scripts)
    // Use source=blockchain explicitly for real-time accuracy when needed
    const useBlockchain = req.query.source === 'blockchain';

    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    // NEW: Use blockchain query if requested
    if (useBlockchain) {
      try {
        const flowService = await import("./services/flow-blockchain.js");
        const startTime = Date.now();

        console.log(`[Wallet Query] Using blockchain source for ${wallet.substring(0, 8)}...`);

        // Get UNLOCKED NFT IDs from blockchain
        const unlockedIds = await flowService.getWalletNFTIds(wallet);

        // Get LOCKED NFT IDs from database
        // NOTE: Use holdings table as it is the live one
        const lockedResult = await pgQuery(
          `SELECT nft_id FROM holdings WHERE wallet_address = $1 AND is_locked = true`,
          [wallet]
        );
        const lockedIds = lockedResult.rows.map(r => r.nft_id);

        // Combine unlocked (from blockchain) + locked (from database)
        // Use a Set to avoid duplicates (an NFT could appear in both if sync is in progress)
        const unlockedStrings = unlockedIds.map(id => id.toString());
        const lockedStrings = lockedIds.map(id => id.toString());
        const lockedSet = new Set(lockedStrings);

        // Deduplicate: use Set then convert back to array
        const allNftIdsSet = new Set([...unlockedStrings, ...lockedStrings]);
        const nftIds = Array.from(allNftIdsSet);

        console.log(`[Wallet Query] Found ${unlockedIds.length} unlocked + ${lockedIds.length} locked = ${nftIds.length} total NFTs(deduplicated)`);

        if (nftIds.length === 0) {
          return res.json({
            ok: true,
            wallet,
            count: 0,
            rows: [],
            source: 'blockchain',
            queryTime: Date.now() - startTime
          });
        }

        // Get metadata from database - start from blockchain NFT IDs, then join to metadata
        // This ensures we get ALL NFTs from blockchain, even if metadata doesn't exist yet
        // Also join with holdings to preserve locked status
        const nftIdPlaceholders = nftIds.map((_, idx) => `$${idx + 1} `).join(', ');
        const result = await pgQuery(
          `
SELECT
            $${nftIds.length + 1}::text AS wallet_address,
  COALESCE(h.is_locked, false) AS is_locked,
    COALESCE(h.acquired_at, NOW()) AS last_event_ts,
      nft_ids.nft_id,
      m.edition_id,
      m.play_id,
      m.series_id,
      m.set_id,
      m.tier,
      m.serial_number,
      m.max_mint_size,
      m.first_name,
      m.last_name,
      COALESCE(
        NULLIF(TRIM(m.first_name || ' ' || m.last_name), ''),
        m.team_name,
        m.set_name,
        '(unknown)'
      ) AS player_name,
        m.team_name,
        m.position,
        m.jersey_number,
        m.series_name,
        m.set_name
FROM(SELECT unnest(ARRAY[${nftIdPlaceholders}]:: text[]) AS nft_id) nft_ids
          LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id
          LEFT JOIN holdings h 
            ON h.nft_id = nft_ids.nft_id 
            AND h.wallet_address = $${nftIds.length + 1}:: text
          ORDER BY COALESCE(m.last_name, '') NULLS LAST, COALESCE(m.first_name, '') NULLS LAST, nft_ids.nft_id;
`,
          [...nftIds.map(id => id.toString()), wallet]
        );

        const queryTime = Date.now() - startTime;
        console.log(`[Wallet Query] ✅ Completed in ${queryTime} ms - Found ${result.rowCount} moments`);

        // Post-process to ensure is_locked is set correctly from our lockedSet
        const rows = result.rows.map(row => ({
          ...row,
          is_locked: lockedSet.has(row.nft_id) || row.is_locked
        }));

        return res.json({
          ok: true,
          wallet,
          count: result.rowCount,
          rows: rows,
          source: 'blockchain',
          queryTime: queryTime
        });
      } catch (blockchainErr) {
        console.error(`[Wallet Query] Blockchain query failed, falling back to database: `, blockchainErr.message);
        // Fall through to database query
      }
    }

    // Check if we should auto-refresh (if data is stale or forced)
    // NOTE: Live API calls are currently unreliable, so we primarily rely on real-time WebSocket updates
    // Only refresh if explicitly forced or if no data exists (new wallet)
    let shouldRefresh = forceRefresh;
    if (!shouldRefresh) {
      // Check if wallet has any holdings in database
      const countResult = await pgQuery(
        `SELECT COUNT(*) as count 
         FROM holdings 
         WHERE wallet_address = $1`,
        [wallet]
      );
      const count = parseInt(countResult.rows[0]?.count || 0);

      // Only refresh if wallet has no holdings (might be a new wallet that needs initial sync)
      // Otherwise, trust the database which is kept up-to-date via WebSocket real-time events
      if (count === 0) {
        shouldRefresh = true;
        console.log(`[Wallet Query] Wallet ${wallet.substring(0, 8)}... has no holdings in database, will attempt refresh`);
      }
    }

    // Auto-refresh from Flow blockchain if needed (PHASE 1 FIX: Use reliable Flow service)
    if (shouldRefresh) {
      try {
        console.log(`[Wallet Query] REFRESHING from Flow blockchain for ${wallet.substring(0, 8)}...`);
        const flowService = await import("./services/flow-blockchain.js");
        const nftIds = await flowService.getWalletNFTIds(wallet);

        if (nftIds && nftIds.length >= 0) {
          // Get current holdings from database
          const currentResult = await pgQuery(
            `SELECT nft_id FROM holdings WHERE wallet_address = $1`,
            [wallet]
          );
          const currentNftIds = new Set(currentResult.rows.map(r => r.nft_id));
          const newNftIds = new Set(nftIds.map(id => id.toString()));

          // Find NFTs to add
          const toAdd = nftIds.filter(id => !currentNftIds.has(id.toString()));

          // Find NFTs to remove
          const toRemove = Array.from(currentNftIds).filter(id => !newNftIds.has(id));

          // Add new NFTs
          if (toAdd.length > 0) {
            const now = new Date();
            const values = toAdd.map((nftId, idx) =>
              `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4})`
            ).join(', ');

            const params = toAdd.flatMap(nftId => [wallet, nftId.toString(), false, now]);

            await pgQuery(
              `INSERT INTO holdings(wallet_address, nft_id, is_locked, acquired_at)
               VALUES ${values}
               ON CONFLICT(wallet_address, nft_id) 
               DO UPDATE SET
is_locked = COALESCE(holdings.is_locked, FALSE),
  acquired_at = COALESCE(holdings.acquired_at, EXCLUDED.acquired_at)`,
              params
            );
          }

          // Remove NFTs that are no longer in the wallet (BUT NEVER DELETE LOCKED ONES!)
          if (toRemove.length > 0) {
            await pgQuery(
              `DELETE FROM holdings 
               WHERE wallet_address = $1 
                 AND nft_id = ANY($2:: text[])
                 AND is_locked IS NOT TRUE`,
              [wallet, toRemove]
            );
          }

          if (toAdd.length > 0 || toRemove.length > 0) {
            console.log(`[Wallet Query] ✅ Refreshed wallet ${wallet.substring(0, 8)}... - Added: ${toAdd.length}, Removed: ${toRemove.length} `);
          }

          // NEW: Fetch missing metadata for NFTs we just added
          if (nftIds.length > 0) {
            const nftIdStrings = nftIds.map(id => id.toString());
            const metadataCheck = await pgQuery(
              `SELECT nft_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1:: text[])`,
              [nftIdStrings]
            );
            const existingMetadata = new Set(metadataCheck.rows.map(r => r.nft_id));
            const missingMetadata = nftIdStrings.filter(id => !existingMetadata.has(id));

            if (missingMetadata.length > 0) {
              console.log(`[Wallet Query] Fetching metadata for ${missingMetadata.length} NFTs...`);
              // Fetch in parallel but limit concurrency
              const BATCH_SIZE = 5;
              for (let i = 0; i < missingMetadata.length; i += BATCH_SIZE) {
                const batch = missingMetadata.slice(i, i + BATCH_SIZE);
                await Promise.all(batch.map(nftId => fetchAndCacheNFTMetadata(nftId)));
              }
            }
          }
        }
      } catch (refreshErr) {
        // Don't fail the request if refresh fails - just log and continue with database data
        console.warn(`[Wallet Query]Auto - refresh failed for ${wallet}: `, refreshErr.message);
      }
    }

    // Query the database (now with fresh data if refresh happened)
    // Use LEFT JOIN so we show all holdings even if metadata is missing
    // JOIN with holdings table to get proper acquired_at date (not last_event_ts which updates on sync)
    const result = await pgQuery(
      `
SELECT
h.wallet_address,
  h.is_locked,
  h.acquired_at,
    h.nft_id,
    m.edition_id,
    m.play_id,
    m.series_id,
    m.set_id,
    m.tier,
    m.serial_number,
    m.max_mint_size,
    m.first_name,
    m.last_name,
    COALESCE(
      NULLIF(TRIM(m.first_name || ' ' || m.last_name), ''),
      m.team_name,
      m.set_name,
      '(unknown)'
    ) AS player_name,
      m.team_name,
      m.position,
      m.jersey_number,
      m.series_name,
      m.set_name
      FROM holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      ORDER BY COALESCE(h.acquired_at, NOW()) DESC;
`,
      [wallet]
    );

    return res.json({
      ok: true,
      wallet,
      count: result.rowCount,
      rows: result.rows,
      refreshed: shouldRefresh,
      source: 'database'
    });
  } catch (err) {
    console.error("Error in /api/query (Render):", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
});

// NEW: Blockchain-based wallet query (direct Cadence/Flow integration)
// This endpoint uses direct blockchain queries and automatically syncs to database
app.get("/api/query-blockchain", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();

    if (!wallet) {
      return res.status(400).json({ ok: false, error: "Missing ?wallet=0x..." });
    }

    if (!/^0x[0-9a-f]{4,64}$/.test(wallet)) {
      return res.status(400).json({ ok: false, error: "Invalid wallet format" });
    }

    const startTime = Date.now();
    console.log(`[Blockchain Query] Fetching wallet ${wallet.substring(0, 8)}... from Flow blockchain`);

    try {
      const flowService = await import("./services/flow-blockchain.js");

      // Get NFT IDs directly from blockchain
      const nftIds = await flowService.getWalletNFTIds(wallet);

      // Auto-sync to database for future fast queries (non-blocking)
      if (nftIds.length > 0) {
        try {
          const syncService = await import("./scripts/sync_wallets_from_blockchain.js");
          syncService.syncWallet(wallet).catch(err => {
            console.warn(`[Blockchain Query] Background sync failed: `, err.message);
          });
        } catch (syncErr) {
          // Continue even if sync fails
        }
      } else {
        // Empty wallet - remove old holdings
        try {
          await pgQuery(`DELETE FROM holdings WHERE wallet_address = $1`, [wallet]);
        } catch (e) {
          // Ignore
        }
      }

      if (nftIds.length === 0) {
        return res.json({
          ok: true,
          wallet,
          count: 0,
          rows: [],
          source: 'blockchain',
          queryTime: Date.now() - startTime,
          message: 'Wallet has no NFTs or collection not set up'
        });
      }

      console.log(`[Blockchain Query] Found ${nftIds.length} NFT IDs, fetching metadata from database...`);

      // Get metadata from database - start from blockchain NFT IDs, then join to metadata
      // This ensures we get ALL NFTs from blockchain, even if metadata doesn't exist yet
      const nftIdPlaceholders = nftIds.map((_, idx) => `$${idx + 1}`).join(', ');
      const result = await pgQuery(
        `
SELECT
          $${nftIds.length + 1}::text AS wallet_address,
  COALESCE(h.is_locked, false) AS is_locked,
    COALESCE(h.acquired_at, NOW()) AS last_event_ts,
      nft_ids.nft_id,
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
FROM(SELECT unnest(ARRAY[${nftIdPlaceholders}]:: text[]) AS nft_id) nft_ids
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id
        LEFT JOIN holdings h 
          ON h.nft_id = nft_ids.nft_id 
          AND h.wallet_address = $${nftIds.length + 1}:: text
        ORDER BY COALESCE(m.last_name, '') NULLS LAST, COALESCE(m.first_name, '') NULLS LAST, nft_ids.nft_id;
`,
        [...nftIds.map(id => id.toString()), wallet]
      );

      const queryTime = Date.now() - startTime;
      console.log(`[Blockchain Query] ✅ Completed in ${queryTime} ms - Found ${result.rowCount} moments with metadata`);

      return res.json({
        ok: true,
        wallet,
        count: result.rowCount,
        rows: result.rows,
        source: 'blockchain',
        queryTime: queryTime,
        blockchainNftCount: nftIds.length,
        metadataMatched: result.rowCount
      });
    } catch (blockchainErr) {
      console.error(`[Blockchain Query]Error: `, blockchainErr);
      return res.status(500).json({
        ok: false,
        error: blockchainErr.message || String(blockchainErr),
        source: 'blockchain',
        queryTime: Date.now() - startTime
      });
    }
  } catch (err) {
    console.error("Error in /api/query-blockchain:", err);
    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
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
          COALESCE(COUNT(*) FILTER(WHERE s.is_locked = FALSE), 0) AS unlocked_moments,
            COALESCE(COUNT(*) FILTER(WHERE s.is_locked = TRUE), 0) AS locked_moments,
              COALESCE(COUNT(*) FILTER(WHERE s.tier_norm = 'common'), 0)    AS tier_common,
                COALESCE(COUNT(*) FILTER(WHERE s.tier_norm = 'uncommon'), 0)  AS tier_uncommon,
                  COALESCE(COUNT(*) FILTER(WHERE s.tier_norm = 'rare'), 0)      AS tier_rare,
                    COALESCE(COUNT(*) FILTER(WHERE s.tier_norm = 'legendary'), 0) AS tier_legendary,
                      COALESCE(COUNT(*) FILTER(WHERE s.tier_norm = 'ultimate'), 0)  AS tier_ultimate
      FROM(
        SELECT
          wp.display_name,
        wp.wallet_address,
        wh.nft_id,
        wh.is_locked,
        LOWER(ncm.tier) AS tier_norm
        FROM public.wallet_profiles AS wp
        LEFT JOIN public.holdings AS wh
          ON wh.wallet_address = wp.wallet_address
        LEFT JOIN public.nft_core_metadata_v2 AS ncm
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
      WHERE wallet_address NOT IN(
          '0xe4cf4bdc1751c65d', --AllDay contract
        '0xb6f2481eba4df97b'  -- huge custodial / system wallet
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
        FROM nft_core_metadata_v2
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

// Insights logic moved to routes/insights.js

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
        COUNT(*) OVER()::int AS total_count
      FROM holdings h
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      ORDER BY COALESCE(h.acquired_at, NOW()) DESC
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
    console.error("Error in /api/query-paged (Render):", err);
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
    const syntheticEmail = `dapper:${wallet_address} `;

    const upsertSql = `
      INSERT INTO public.users(email, password_hash, default_wallet_address)
      VALUES($1, NULL, $2)
      ON CONFLICT(email)
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

// POST /api/login-flow - Authenticate with Dapper wallet only
app.post("/api/login-flow", async (req, res) => {
  try {
    let { wallet_address, signed_message, signature, wallet_provider, dapper_username } = req.body || {};

    // Only allow Dapper wallets
    if (wallet_provider !== "dapper") {
      console.log("[Flow Login] Rejected non-Dapper wallet attempt");
      return res.status(403).json({
        ok: false,
        error: "Only Dapper wallets are supported. Please use Dapper Wallet to sign in."
      });
    }

    console.log("[Flow Login] Received request:", {
      wallet_address: wallet_address ? wallet_address.substring(0, 20) + '...' : 'missing',
      has_signed_message: !!signed_message,
      has_signature: !!signature,
      dapper_username: dapper_username || null
    });

    if (!wallet_address || typeof wallet_address !== "string") {
      console.error("[Flow Login] Missing wallet_address");
      return res.status(400).json({ ok: false, error: "Missing wallet_address" });
    }

    wallet_address = wallet_address.trim().toLowerCase();
    if (!wallet_address.startsWith("0x")) {
      wallet_address = "0x" + wallet_address;
    }

    // Flow addresses are 16 hex characters after 0x, but be more lenient
    // Some wallets might return addresses in different formats
    const flowAddressPattern = /^0x[0-9a-f]{8,16}$/i;
    if (!flowAddressPattern.test(wallet_address)) {
      console.error("[Flow Login] Invalid address format:", wallet_address);
      return res.status(400).json({ ok: false, error: `Invalid Flow wallet address format: ${wallet_address} ` });
    }

    // Normalize to 16 characters (pad with zeros if needed)
    const addressPart = wallet_address.substring(2);
    const normalizedAddress = "0x" + addressPart.padStart(16, '0');

    // TODO: Verify signature if provided (for production security)
    // For now, we'll trust the wallet address from FCL

    // Use synthetic email format: flow:wallet_address
    const syntheticEmail = `flow:${normalizedAddress} `;

    console.log("[Flow Login] Creating/updating user:", syntheticEmail);

    // Ensure password_hash column is nullable (run migration if needed)
    // This is a one-time migration that will be skipped if already done
    try {
      const checkResult = await pool.query(`
        SELECT is_nullable 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'users' 
        AND column_name = 'password_hash';
      `);

      if (checkResult.rows.length > 0 && checkResult.rows[0].is_nullable === 'NO') {
        await pool.query(`
          ALTER TABLE public.users 
          ALTER COLUMN password_hash DROP NOT NULL;
      `);
        console.log("[Flow Login] Made password_hash nullable (migration applied)");
      }
    } catch (alterErr) {
      // If migration fails, log but continue - might be permission issue
      console.warn("[Flow Login] Could not check/alter password_hash column:", alterErr.message);
      console.warn("[Flow Login] You may need to run: ALTER TABLE public.users ALTER COLUMN password_hash DROP NOT NULL;");
    }

    // Update display_name if dapper_username is provided
    const upsertSql = dapper_username
      ? `
        INSERT INTO public.users(email, password_hash, default_wallet_address, display_name)
      VALUES($1, NULL, $2, $3)
        ON CONFLICT(email)
        DO UPDATE SET
      default_wallet_address = EXCLUDED.default_wallet_address,
        display_name = COALESCE(EXCLUDED.display_name, users.display_name)
        RETURNING id, email, default_wallet_address, display_name;
      `
      : `
        INSERT INTO public.users(email, password_hash, default_wallet_address)
      VALUES($1, NULL, $2)
        ON CONFLICT(email)
        DO UPDATE SET
      default_wallet_address = EXCLUDED.default_wallet_address
        RETURNING id, email, default_wallet_address, display_name;
      `;

    const queryParams = dapper_username
      ? [syntheticEmail, normalizedAddress, dapper_username]
      : [syntheticEmail, normalizedAddress];

    const { rows } = await pool.query(upsertSql, queryParams);

    if (!rows || rows.length === 0) {
      console.error("[Flow Login] Database query returned no rows");
      return res.status(500).json({ ok: false, error: "Failed to create user account" });
    }
    const user = rows[0];

    // Set session
    req.session.user = {
      id: user.id,
      email: user.email,
      default_wallet_address: user.default_wallet_address,
      display_name: user.display_name,
      auth_provider: "flow"
    };

    return res.json({
      ok: true,
      user: req.session.user
    });
  } catch (err) {
    console.error("POST /api/login-flow error:", err);
    console.error("Error details:", {
      message: err.message,
      stack: err.stack,
      code: err.code
    });
    return res.status(500).json({
      ok: false,
      error: "Failed to log in with Flow wallet: " + (err.message || String(err))
    });
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

        const url = `${FLOW_ACCESS_NODE} /v1/events ? ${urlParams.toString()} `;
        console.log(`Fetching ${eventType} from blocks ${start_height} -${end_height} `);

        const flowRes = await fetch(url);
        const responseText = await flowRes.text();

        if (!flowRes.ok) {
          if (flowRes.status === 500) {
            console.warn(`Flow API 500 error for ${eventType}(blocks ${start_height} - ${end_height})`);
            continue; // Skip this event type
          } else {
            console.error(`Flow API error for ${eventType}: `, flowRes.status);
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
        console.error(`Error fetching ${eventType}: `, err.message);
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

    console.log(`Fetched ${events.length} events from blocks ${start_height} to ${end_height} `);

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
      EVENT_DATA: id::STRING AS nft_id,
        LOWER(EVENT_DATA: to:: STRING) AS to_addr,
          LOWER(EVENT_DATA: from:: STRING) AS from_addr,
            EVENT_TYPE AS event_type,
              BLOCK_TIMESTAMP AS block_timestamp,
                BLOCK_HEIGHT AS block_height,
                  TX_ID AS tx_id,
                    EVENT_INDEX AS event_index
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
      WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
        AND EVENT_TYPE IN('Deposit', 'Withdraw', 'MomentNFTMinted', 'MomentNFTBurned')
        AND TX_SUCCEEDED = TRUE
        AND BLOCK_TIMESTAMP >= '${cutoffStr}'
      ORDER BY BLOCK_TIMESTAMP DESC, BLOCK_HEIGHT DESC, EVENT_INDEX DESC
      LIMIT ${limitNum}
      `;

    console.log(`Querying Snowflake for events in last ${hoursAgo} hours(since ${cutoffStr})`);
    console.log(`Current time: ${new Date().toISOString()} `);

    const result = await executeSql(sql);
    console.log(`Snowflake returned ${result ? result.length : 0} events`);

    if (result && result.length > 0) {
      console.log(`Sample event: `, JSON.stringify(result[0]).substring(0, 300));
      console.log(`Most recent event timestamp: ${result[0].block_timestamp || result[0].BLOCK_TIMESTAMP} `);
      console.log(`Most recent event block: ${result[0].block_height || result[0].BLOCK_HEIGHT} `);
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
      const key = `${event.txId} -${event.nftId} `;

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
          console.error(`Error fetching seller name for ${event.from}: `, err.message);
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
          console.error(`Error fetching buyer name for ${event.to}: `, err.message);
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
            FROM nft_core_metadata_v2 
            WHERE nft_id = $1 LIMIT 1`,
            [event.nftId]
          );
          const moment = momentResult.rows[0];
          if (moment) {
            enriched.moment = {
              playerName: moment.first_name && moment.last_name
                ? `${moment.first_name} ${moment.last_name} `
                : null,
              teamName: moment.team_name,
              position: moment.position,
              tier: moment.tier,
              setName: moment.set_name,
              seriesName: moment.series_name
            };
          }
        } catch (err) {
          console.error(`Error fetching moment details for ${event.nftId}: `, err.message);
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
        ? `Snowflake data is ${dataAgeHours} hours old(newest event: ${newestEventTime})`
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

  // Try FindLab API first (often faster and more reliable)
  try {
    const height = await getLatestBlockHeightFindLab();
    if (height && height > 0) {
      cachedLatestBlockHeight = height;
      lastBlockHeightFetch = now;
      return height;
    }
  } catch (err) {
    console.warn("[BlockHeight] FindLab API failed, trying Flow REST API:", err.message);
  }

  // Fallback to Flow REST API
  try {
    // Query the public Flow REST API for the latest block
    const res = await fetch("https://rest-mainnet.onflow.org/v1/blocks?height=sealed", {
      signal: AbortSignal.timeout(5000)
    });
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
    console.log(`Querying light node for events in blocks ${startHeight} -${endHeight} `);

    const [depositRes, withdrawRes] = await Promise.all([
      fetch(`${FLOW_LIGHT_NODE_URL} /v1/events ? type = A.e4cf4bdc1751c65d.AllDay.Deposit & start_height=${startHeight}& end_height=${endHeight} `),
      fetch(`${FLOW_LIGHT_NODE_URL} /v1/events ? type = A.e4cf4bdc1751c65d.AllDay.Withdraw & start_height=${startHeight}& end_height=${endHeight} `)
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
      const key = `${event.txId} -${event.nftId} `;
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
            `SELECT first_name, last_name, team_name, position, tier, set_name, series_name FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`,
            [event.nftId]
          );
          const moment = result.rows[0];
          if (moment) {
            enriched.moment = {
              playerName: moment.first_name && moment.last_name ? `${moment.first_name} ${moment.last_name} ` : null,
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
      const key = `${event.txId} -${event.nftId} `;
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
    const url = `${FLOW_ACCESS_NODE} /v1/blocks ? height = final`;

    console.log("Fetching latest block from:", url);

    const flowRes = await fetch(url);
    const responseText = await flowRes.text();

    if (!flowRes.ok) {
      console.error("Flow API error:", flowRes.status, responseText.substring(0, 500));
      throw new Error(`Flow API error: ${flowRes.status} - ${responseText.substring(0, 200)} `);
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


// Sniper API Endpoints

// API endpoint to get sniper listings with filtering
// Debug endpoint to find a listing by player name and serial
// Debug endpoint to find a listing by player name and serial
app.get("/api/sniper/find-listing", async (req, res) => {
  try {
    const listings = await sniperService.findListings(req.query);
    if (!listings || listings.length === 0) {
      return res.json({ ok: true, found: false, message: "No listings found matching criteria" });
    }
    return res.json({ ok: true, found: true, count: listings.length, listings });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to find listing: " + err.message });
  }
});

// Manual verification endpoint for a specific listing
app.get("/api/sniper/verify-listing", async (req, res) => {
  try {
    const { nftId, listingId } = req.query;
    const result = await sniperService.verifyListing(nftId, listingId);
    return res.json(result);
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to verify listing: " + err.message });
  }
});

// Reset all listings to unsold
app.post("/api/sniper/reset-all-unsold", async (req, res) => {
  try {
    const result = await sniperService.resetAllListingsToUnsold();
    return res.json({ ok: true, ...result, message: "All listings reset to unsold" });
  } catch (err) {
    return res.status(500).json({ ok: false, error: "Failed to reset listings: " + err.message });
  }
});

app.get("/api/sniper-deals", async (req, res) => {
  try {
    const listings = await sniperService.getSniperDeals(req.query);
    const allTeams = [...new Set(sniperService.sniperListings.map(l => l.teamName).filter(Boolean))].sort();
    const allTiers = [...new Set(sniperService.sniperListings.map(l => l.tier).filter(Boolean))];
    const dealsCount = listings.filter(l => l.dealPercent > 0).length;

    return res.json({
      ok: true,
      listings,
      total: sniperService.sniperListings.length,
      filtered: listings.length,
      dealsCount,
      watching: sniperService.isWatchingListings,
      lastCheckedBlock: sniperService.lastCheckedBlock,
      availableTeams: allTeams,
      availableTiers: allTiers,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/sniper-warmup", async (req, res) => {
  try {
    const result = await sniperService.warmupFloorCache(req.query.limit, executeSql, ensureSnowflakeConnected);
    return res.json({ ok: true, ...result });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Endpoint to get active listings only (smaller response)
app.get("/api/sniper-active-listings", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const listings = await sniperService.getActiveListings(limit);
    return res.json({ ok: true, count: listings.length, listings });
  } catch (err) {
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
      WITH listings AS(
          SELECT
          EVENT_DATA: nftID:: STRING AS nft_id,
          EVENT_DATA: listingResourceID:: STRING AS listing_id,
          TRY_TO_DOUBLE(EVENT_DATA: price:: STRING) AS listing_price,
          LOWER(EVENT_DATA: storefrontAddress:: STRING) AS seller_addr,
          BLOCK_TIMESTAMP AS block_timestamp,
          BLOCK_HEIGHT AS block_height,
          TX_ID AS tx_id
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
        WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
          AND EVENT_TYPE = 'ListingAvailable'
          AND EVENT_DATA: nftType: typeID:: STRING = 'A.e4cf4bdc1751c65d.AllDay.NFT'
          AND TX_SUCCEEDED = TRUE
          AND BLOCK_TIMESTAMP >= '${cutoffStr}'
        ),
        completed AS(
          SELECT DISTINCT
          EVENT_DATA: listingResourceID:: STRING AS listing_id
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
      WHERE c.listing_id IS NULL-- Only show unsold listings
      ORDER BY l.block_timestamp DESC
      LIMIT ${limitNum}
      `;

    sniperLog(`[Sniper] Querying active listings from last ${hoursAgo} hours...`);

    const salesResult = await executeSql(eventsSql);
    sniperLog(`[Sniper] Found ${salesResult?.length || 0} active listings`);

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
           FROM nft_core_metadata_v2 
           WHERE nft_id = ANY($1:: text[])`,
          [nftIds]
        );
        for (const row of metaResult.rows) {
          momentData[row.nft_id] = row;
          if (row.edition_id) editionIds.add(row.edition_id);
        }
      } catch (err) {
        sniperError("[Sniper] Error fetching moment metadata:", err.message);
      }
    }

    // Scrape REAL-TIME prices from NFL All Day website
    // This is the key to accurate sniper data!
    let scrapedData = {};
    const editionList = [...editionIds];

    if (editionList.length > 0) {
      sniperLog(`[Sniper] Scraping real - time prices for ${editionList.length} editions...`);

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

      sniperLog(`[Sniper] Got real - time prices for ${Object.keys(scrapedData).length} editions`);
    }

    // Get average sale prices from our database to compare with scraped low asks
    let avgSalePrices = {};
    if (editionList.length > 0) {
      try {
        const priceResult = await pgQuery(
          `SELECT edition_id, avg_sale_usd 
           FROM public.edition_price_scrape 
           WHERE edition_id = ANY($1:: text[])`,
          [editionList]
        );
        for (const row of priceResult.rows) {
          avgSalePrices[row.edition_id] = row.avg_sale_usd ? Number(row.avg_sale_usd) : null;
        }
      } catch (err) {
        sniperError("[Sniper] Error fetching avg sale prices:", err.message);
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
          `SELECT wallet_address, display_name FROM wallet_profiles WHERE wallet_address = ANY($1:: text[])`,
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
          ? `${moment.first_name} ${moment.last_name} `
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

        // Direct moment link for faster buying (one click to buy page)
        listingUrl: `https://nflallday.com/moments/${nftId}`
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
    sniperError("[Sniper] Error:", err);
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
                  team_name, position, tier, set_name, jersey_number, max_mint_size
           FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
          [nftIds]
        );

        const editionIds = [];
        for (const row of metaResult.rows) {
          momentData[row.nft_id] = row;
          if (row.edition_id) editionIds.push(row.edition_id);
        }

        if (editionIds.length > 0) {
          const priceResult = await pgQuery(
            `SELECT edition_id, lowest_ask_usd, avg_sale_usd FROM edition_price_scrape 
             WHERE edition_id = ANY($1::text[])`,
            [[...new Set(editionIds)]]
          );
          for (const row of priceResult.rows) {
            editionPrices[row.edition_id] = {
              floor: Number(row.lowest_ask_usd),
              avgSale: row.avg_sale_usd ? Number(row.avg_sale_usd) : null
            };
          }
        }
      } catch (e) {
        console.error("[Sniper Active] Metadata error:", e.message);
      }
    }

    // Enrich listings
    const enrichedListings = activeListings.map(listing => {
      const moment = momentData[listing.nftId] || {};
      const priceData = editionPrices[moment.edition_id] || {};
      const floorPrice = priceData.floor;
      const avgSale = priceData.avgSale;

      const enriched = {
        ...listing,
        editionId: moment.edition_id,
        serialNumber: moment.serial_number,
        jerseyNumber: moment.jersey_number,
        maxMint: moment.max_mint_size,
        playerName: moment.first_name && moment.last_name
          ? `${moment.first_name} ${moment.last_name}` : null,
        teamName: moment.team_name,
        tier: moment.tier,
        setName: moment.set_name,
        floor: floorPrice,
        floorPrice,
        avgSale,
        // Calculate REAL deal score
        dealPercent: 0,
        // Direct moment link for faster buying (one click to buy page)
        listingUrl: listing.nftId ? `https://nflallday.com/moments/${listing.nftId}` : null
      };

      enriched.dealPercent = calculateRealDealScore(enriched);
      return enriched;
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
      const hoursOld = parseFloat(age.rows[0]?.hours_old || 0);
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











// ================== SERVER START ==================

app.listen(port, async () => {
  console.log(`NFL ALL DAY collection viewer running on http://localhost:${port}`);

  // Initialize database tables

  // Set up insights refresh after server starts
  setTimeout(() => {
    setupInsightsRefresh();
  }, 2000); // Wait 2 seconds for server to be fully ready

  // Initialize sniper system (loads from DB and starts watcher)
  sniperService.initializeSniper();
});
