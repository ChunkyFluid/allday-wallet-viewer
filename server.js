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
import * as eventProcessor from "./services/event-processor.js";

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

    // Update legacy wallet_holdings table in real-time (keep for backward compatibility)
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

// Update wallet holdings in real-time when we see Deposit events
async function updateWalletHoldingOnDeposit(walletAddress, nftId, timestamp, blockHeight) {
  try {
    if (!walletAddress || !nftId) return;

    const walletAddr = walletAddress.toLowerCase();
    const ts = timestamp ? new Date(timestamp) : new Date();

    // Upsert: add NFT to wallet holdings (or update timestamp if already exists)
    // Only update if the new timestamp is newer (avoid overwriting with old data)
    await pgQuery(
      `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (wallet_address, nft_id) 
       DO UPDATE SET 
         is_locked = FALSE,
         last_event_ts = CASE 
           WHEN wallet_holdings.last_event_ts IS NULL OR wallet_holdings.last_event_ts < EXCLUDED.last_event_ts 
           THEN EXCLUDED.last_event_ts 
           ELSE wallet_holdings.last_event_ts 
         END,
         last_synced_at = NOW()`,
      [walletAddr, nftId.toString(), false, ts]
    );

    console.log(`[Wallet Sync] ✅ Added NFT ${nftId} to wallet ${walletAddr.substring(0, 8)}...`);

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
          row.SERIAL_NUMBER || row.serial_number,
          row.MAX_MINT_SIZE || row.max_mint_size,
          row.FIRST_NAME || row.first_name,
          row.LAST_NAME || row.last_name,
          row.TEAM_NAME || row.team_name,
          row.POSITION || row.position,
          row.JERSEY_NUMBER || row.jersey_number,
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
async function updateWalletHoldingOnWithdraw(walletAddress, nftId) {
  try {
    if (!walletAddress || !nftId) return;

    const walletAddr = walletAddress.toLowerCase();

    // Delete the NFT from wallet holdings
    const result = await pgQuery(
      `DELETE FROM wallet_holdings 
       WHERE wallet_address = $1 AND nft_id = $2`,
      [walletAddr, nftId.toString()]
    );

    if (result.rowCount > 0) {
      console.log(`[Wallet Sync] ✅ Removed NFT ${nftId} from wallet ${walletAddr.substring(0, 8)}...`);
    }
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
        `DELETE FROM wallet_holdings WHERE wallet_address = $1`,
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
      `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1`,
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
        `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
         VALUES ${values}
         ON CONFLICT (wallet_address, nft_id) 
         DO UPDATE SET 
           is_locked = FALSE,
           last_event_ts = EXCLUDED.last_event_ts,
           last_synced_at = NOW()`,
        params
      );
      added = toAdd.length;
    }

    // Remove NFTs that are no longer in the wallet
    let removed = 0;
    if (toRemove.length > 0) {
      const result = await pgQuery(
        `DELETE FROM wallet_holdings 
         WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
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
      `SELECT nft_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
      [nftIdsArray]
    );
    const existingMetadata = new Set(metadataCheck.rows.map(r => r.nft_id));
    const missingMetadata = nftIdsArray.filter(id => !existingMetadata.has(id));

    if (missingMetadata.length > 0) {
      console.log(`[Wallet Refresh] Fetching metadata for ${missingMetadata.length} NFTs missing metadata...`);

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

    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const pattern = `%${qRaw}%`;
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
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
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
      params.push(`%${player}%`);
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
      const placeholders = tiers.map((_, i) => `$${idx + i}`).join(', ');
      conditionsSnapshot.push(`LOWER(tier) IN (${placeholders.split(', ').map(p => `LOWER(${p})`).join(', ')})`);
      conditionsLive.push(`LOWER(e.tier) IN (${placeholders.split(', ').map(p => `LOWER(${p})`).join(', ')})`);
      params.push(...tiers);
      idx += tiers.length;
    }

    // Series filter (multi-select)
    if (seriesList.length > 0) {
      const placeholders = seriesList.map((_, i) => `$${idx + i}`).join(', ');
      conditionsSnapshot.push(`series_name IN (${placeholders})`);
      conditionsLive.push(`e.series_name IN (${placeholders})`);
      params.push(...seriesList);
      idx += seriesList.length;
    }

    // Set filter (multi-select)
    if (sets.length > 0) {
      const placeholders = sets.map((_, i) => `$${idx + i}`).join(', ');
      conditionsSnapshot.push(`set_name IN (${placeholders})`);
      conditionsLive.push(`e.set_name IN (${placeholders})`);
      params.push(...sets);
      idx += sets.length;
    }

    // Position filter (multi-select)
    if (positions.length > 0) {
      const placeholders = positions.map((_, i) => `$${idx + i}`).join(', ');
      conditionsSnapshot.push(`position IN (${placeholders})`);
      conditionsLive.push(`e.position IN (${placeholders})`);
      params.push(...positions);
      idx += positions.length;
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
      params.push(`%${player}%`);
    }

    if (team) {
      conditions.push(`team_name = $${idx++}`);
      params.push(team);
    }

    if (tier) {
      conditions.push(`LOWER(tier) = LOWER($${idx++})`);
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
    if let collectionRef = account.capabilities.borrow<&{NonFungibleToken.CollectionPublic}>(
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
    const flowAddressWithPrefix = `0x${flowAddress}`;

    // Flow REST API v1 scripts endpoint expects arguments as an array
    // Each argument is a JSON-CDC encoded value
    // For Address, we pass it as: { "type": "Address", "value": "0x..." }
    const addressArg = {
      type: "Address",
      value: flowAddressWithPrefix
    };

    const scriptResponse = await fetch(`${FLOW_REST_API}/v1/scripts`, {
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
      throw new Error(`Flow API error: ${scriptResponse.status} ${scriptResponse.statusText} - ${errorText.substring(0, 200)}`);
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
    console.warn(`[LiveWallet] Flow REST API script execution failed for ${walletAddress.substring(0, 10)}...:`, err.message);
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
    console.warn(`[LiveWallet] Flow REST API failed:`, err.message);
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
    console.warn(`[LiveWallet] Event-based fetch failed:`, err.message);
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
      `${FLOW_REST_API}/v1/events?type=A.e4cf4bdc1751c65d.AllDay.Deposit&start_height=${startHeight}&end_height=${latestHeight}`,
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
    console.warn(`[LiveWallet] Event-based fetch error:`, err.message);
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
            `SELECT nft_id FROM holdings WHERE wallet_address = $1 AND is_locked = true
             UNION
             SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
            [wallet]
          );
          lockedIds = lockedResult.rows.map(r => r.nft_id);
          if (lockedIds.length > 0) {
            console.log(`[Wallet Summary] Got ${lockedIds.length} locked NFTs from database (fallback)`);
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
        console.log(`[Wallet Summary] Got ${unlockedIds.length} unlocked + ${lockedIds.length} locked = ${allNftIds.length} total NFTs (deduplicated)`);
      } catch (blockchainErr) {
        console.warn(`[Wallet Summary] Flow blockchain query failed, falling back to database:`, blockchainErr.message);
        // Try fallback to FindLab API
        try {
          liveNftIds = await fetchLiveWalletNFTs(wallet);
          if (liveNftIds !== null) {
            dataSource = 'blockchain';
            console.log(`[Wallet Summary] Got ${liveNftIds.length} NFTs from FindLab API (fallback)`);
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
      // Join with wallet_holdings only for locked status
      const nftIdPlaceholders = liveNftIds.map((_, idx) => `$${idx + 2}`).join(', ');
      statsResult = await pgQuery(
        `
        SELECT
          $1 AS wallet_address,

          COUNT(*)::int AS moments_total,
          -- Use new holdings table first (from Snowflake sync), fallback to old wallet_holdings
          COUNT(*) FILTER (WHERE COALESCE(hn.is_locked, h.is_locked, false))::int AS locked_count,
          COUNT(*) FILTER (WHERE NOT COALESCE(hn.is_locked, h.is_locked, false))::int AS unlocked_count,

          COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int     AS common_count,
          COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int   AS uncommon_count,
          COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int       AS rare_count,
          COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int  AS legendary_count,
          COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int   AS ultimate_count,

          COALESCE(SUM(COALESCE(eps.lowest_ask_usd, 0)), 0)::numeric AS floor_value,
          COALESCE(SUM(COALESCE(eps.avg_sale_usd, 0)), 0)::numeric AS asp_value,
          COUNT(*) FILTER (WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL)::int AS priced_moments

        FROM (SELECT unnest(ARRAY[${nftIdPlaceholders}]::text[]) AS nft_id) nft_ids
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id::text
        LEFT JOIN wallet_holdings h 
          ON h.nft_id = nft_ids.nft_id::text 
          AND LOWER(h.wallet_address) = LOWER($1)
        LEFT JOIN holdings hn
          ON hn.nft_id = nft_ids.nft_id::text
          AND LOWER(hn.wallet_address) = LOWER($1)
        LEFT JOIN public.edition_price_scrape eps
          ON eps.edition_id = m.edition_id;
        `,
        [wallet, ...liveNftIds]
      );

      console.log(`[Wallet Summary] Using blockchain data: ${statsResult.rows[0]?.moments_total || 0} total, ${statsResult.rows[0]?.locked_count || 0} locked (from db join)`);

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
      JOIN nft_core_metadata_v2 m
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
        // NOTE: Only use wallet_holdings - the 'holdings' table has stale/corrupted data
        const lockedResult = await pgQuery(
          `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
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

        console.log(`[Wallet Query] Found ${unlockedIds.length} unlocked + ${lockedIds.length} locked = ${nftIds.length} total NFTs (deduplicated)`);

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
        // Also join with wallet_holdings to preserve locked status
        const nftIdPlaceholders = nftIds.map((_, idx) => `$${idx + 1}`).join(', ');
        const result = await pgQuery(
          `
          SELECT
            $${nftIds.length + 1}::text AS wallet_address,
            COALESCE(h.is_locked, false) AS is_locked,
            COALESCE(h.last_event_ts, NOW()) AS last_event_ts,
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
          FROM (SELECT unnest(ARRAY[${nftIdPlaceholders}]::text[]) AS nft_id) nft_ids
          LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id
          LEFT JOIN wallet_holdings h 
            ON h.nft_id = nft_ids.nft_id 
            AND h.wallet_address = $${nftIds.length + 1}::text
          ORDER BY COALESCE(m.last_name, '') NULLS LAST, COALESCE(m.first_name, '') NULLS LAST, nft_ids.nft_id;
          `,
          [...nftIds.map(id => id.toString()), wallet]
        );

        const queryTime = Date.now() - startTime;
        console.log(`[Wallet Query] ✅ Completed in ${queryTime}ms - Found ${result.rowCount} moments`);

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
        console.error(`[Wallet Query] Blockchain query failed, falling back to database:`, blockchainErr.message);
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
         FROM wallet_holdings 
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

    // Auto-refresh from FindLab API if needed
    if (shouldRefresh) {
      try {
        console.log(`[Wallet Query] Auto-refreshing wallet ${wallet.substring(0, 8)}... (stale data or forced)`);
        const nftIds = await getWalletNFTsFindLab(wallet, 'A.e4cf4bdc1751c65d.AllDay');

        if (nftIds && nftIds.length >= 0) {
          // Get current holdings from database
          const currentResult = await pgQuery(
            `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1`,
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
              `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
               VALUES ${values}
               ON CONFLICT (wallet_address, nft_id) 
               DO UPDATE SET 
                 is_locked = FALSE,
                 last_event_ts = EXCLUDED.last_event_ts,
                 last_synced_at = NOW()`,
              params
            );
          }

          // Remove NFTs that are no longer in the wallet
          if (toRemove.length > 0) {
            await pgQuery(
              `DELETE FROM wallet_holdings 
               WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
              [wallet, toRemove]
            );
          }

          if (toAdd.length > 0 || toRemove.length > 0) {
            console.log(`[Wallet Query] ✅ Refreshed wallet ${wallet.substring(0, 8)}... - Added: ${toAdd.length}, Removed: ${toRemove.length}`);
          }

          // NEW: Fetch missing metadata for NFTs we just added
          if (nftIds.length > 0) {
            const nftIdStrings = nftIds.map(id => id.toString());
            const metadataCheck = await pgQuery(
              `SELECT nft_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
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
        console.warn(`[Wallet Query] Auto-refresh failed for ${wallet}:`, refreshErr.message);
      }
    }

    // Query the database (now with fresh data if refresh happened)
    // Use LEFT JOIN so we show all holdings even if metadata is missing
    const result = await pgQuery(
      `
      SELECT
        h.wallet_address,
        h.is_locked,
        h.last_event_ts,
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
        m.team_name,
        m.position,
        m.jersey_number,
        m.series_name,
        m.set_name
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1
      ORDER BY h.is_locked DESC, h.last_event_ts DESC;
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
            console.warn(`[Blockchain Query] Background sync failed:`, err.message);
          });
        } catch (syncErr) {
          // Continue even if sync fails
        }
      } else {
        // Empty wallet - remove old holdings
        try {
          await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1`, [wallet]);
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
      // Also join with wallet_holdings to preserve locked status
      const nftIdPlaceholders = nftIds.map((_, idx) => `$${idx + 1}`).join(', ');
      const result = await pgQuery(
        `
        SELECT
          $${nftIds.length + 1}::text AS wallet_address,
          COALESCE(h.is_locked, false) AS is_locked,
          COALESCE(h.last_event_ts, NOW()) AS last_event_ts,
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
        FROM (SELECT unnest(ARRAY[${nftIdPlaceholders}]::text[]) AS nft_id) nft_ids
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = nft_ids.nft_id
        LEFT JOIN wallet_holdings h 
          ON h.nft_id = nft_ids.nft_id 
          AND h.wallet_address = $${nftIds.length + 1}::text
        ORDER BY COALESCE(m.last_name, '') NULLS LAST, COALESCE(m.first_name, '') NULLS LAST, nft_ids.nft_id;
        `,
        [...nftIds.map(id => id.toString()), wallet]
      );

      const queryTime = Date.now() - startTime;
      console.log(`[Blockchain Query] ✅ Completed in ${queryTime}ms - Found ${result.rowCount} moments with metadata`);

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
      console.error(`[Blockchain Query] Error:`, blockchainErr);
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
        FROM nft_core_metadata_v2;
      `),

      // Top 5 teams
      pool.query(`
        SELECT team_name, COUNT(*)::bigint AS count
        FROM nft_core_metadata_v2
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
        FROM nft_core_metadata_v2
        WHERE first_name IS NOT NULL AND last_name IS NOT NULL
        GROUP BY first_name, last_name, team_name
        ORDER BY count DESC
        LIMIT 5;
      `),

      // Top 5 sets
      pool.query(`
        SELECT set_name, COUNT(*)::bigint AS count
        FROM nft_core_metadata_v2
        WHERE set_name IS NOT NULL AND set_name != ''
        GROUP BY set_name
        ORDER BY count DESC
        LIMIT 5;
      `),

      // Position breakdown
      pool.query(`
        SELECT position, COUNT(*)::bigint AS count
        FROM nft_core_metadata_v2
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
        FROM nft_core_metadata_v2
        WHERE series_name IS NOT NULL AND series_name != ''
        GROUP BY series_name
        ORDER BY series_name;
      `),

      // 🔢 Popular jersey numbers
      pool.query(`
        SELECT jersey_number, COUNT(*)::bigint AS count
        FROM nft_core_metadata_v2
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
        FROM nft_core_metadata_v2
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
        JOIN nft_core_metadata_v2 m ON m.edition_id = e.edition_id
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
        FROM nft_core_metadata_v2
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
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
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
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
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
      return res.status(400).json({ ok: false, error: `Invalid Flow wallet address format: ${wallet_address}` });
    }

    // Normalize to 16 characters (pad with zeros if needed)
    const addressPart = wallet_address.substring(2);
    const normalizedAddress = "0x" + addressPart.padStart(16, '0');

    // TODO: Verify signature if provided (for production security)
    // For now, we'll trust the wallet address from FCL

    // Use synthetic email format: flow:wallet_address
    const syntheticEmail = `flow:${normalizedAddress}`;

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
        INSERT INTO public.users (email, password_hash, default_wallet_address, display_name)
        VALUES ($1, NULL, $2, $3)
        ON CONFLICT (email)
        DO UPDATE SET
          default_wallet_address = EXCLUDED.default_wallet_address,
          display_name = COALESCE(EXCLUDED.display_name, users.display_name)
        RETURNING id, email, default_wallet_address, display_name;
      `
      : `
        INSERT INTO public.users (email, password_hash, default_wallet_address)
        VALUES ($1, NULL, $2)
        ON CONFLICT (email)
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

// Test endpoint for FindLab API integration
app.get("/api/test-findlab", async (req, res) => {
  try {
    const testType = req.query.type || 'block';
    const results = {};

    // Test 1: Get latest block height
    if (testType === 'block' || testType === 'all') {
      try {
        const height = await getLatestBlockHeightFindLab();
        results.blockHeight = {
          success: true,
          height: height,
          source: 'FindLab API'
        };
      } catch (err) {
        results.blockHeight = {
          success: false,
          error: err.message
        };
      }
    }

    // Test 2: Get wallet NFTs (if address provided)
    if ((testType === 'wallet' || testType === 'all') && req.query.address) {
      try {
        const address = req.query.address.trim();
        const nftIds = await getWalletNFTsFindLab(address, 'A.e4cf4bdc1751c65d.AllDay');
        results.walletNFTs = {
          success: true,
          address: address,
          count: nftIds ? nftIds.length : 0,
          nftIds: nftIds ? nftIds.slice(0, 10) : null, // First 10 only
          source: 'FindLab API'
        };
      } catch (err) {
        results.walletNFTs = {
          success: false,
          error: err.message
        };
      }
    }

    // Test 3: Get NFT item (if nftId provided)
    if ((testType === 'nft' || testType === 'all') && req.query.nftId) {
      try {
        const nftId = req.query.nftId.trim();
        const nftData = await getNFTItemFindLab('A.e4cf4bdc1751c65d.AllDay', nftId);
        results.nftItem = {
          success: true,
          nftId: nftId,
          data: nftData,
          source: 'FindLab API'
        };
      } catch (err) {
        results.nftItem = {
          success: false,
          error: err.message
        };
      }
    }

    // Test 4: Get account transactions (if address provided)
    if ((testType === 'transactions' || testType === 'all') && req.query.address) {
      try {
        const address = req.query.address.trim();
        const transactions = await getAccountTransactionsFindLab(address, 10);
        results.transactions = {
          success: true,
          address: address,
          count: transactions ? transactions.length : 0,
          transactions: transactions ? transactions.slice(0, 5) : null, // First 5 only
          source: 'FindLab API'
        };
      } catch (err) {
        results.transactions = {
          success: false,
          error: err.message
        };
      }
    }

    return res.json({
      ok: true,
      message: 'FindLab API test results',
      tests: results,
      usage: {
        blockHeight: '/api/test-findlab?type=block',
        walletNFTs: '/api/test-findlab?type=wallet&address=0x...',
        nftItem: '/api/test-findlab?type=nft&nftId=123456',
        transactions: '/api/test-findlab?type=transactions&address=0x...',
        all: '/api/test-findlab?type=all&address=0x...&nftId=123456'
      }
    });
  } catch (err) {
    console.error("GET /api/test-findlab error:", err);
    return res.status(500).json({
      ok: false,
      error: "Failed to test FindLab API: " + (err.message || String(err))
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
            FROM nft_core_metadata_v2 
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
            `SELECT first_name, last_name, team_name, position, tier, set_name, series_name FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`,
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

// Enable/disable sniper logging (disabled by default to reduce console spam)
const SNIPER_LOGGING_ENABLED = false;

// Helper to conditionally log sniper messages
function sniperLog(...args) {
  if (SNIPER_LOGGING_ENABLED) console.log(...args);
}
function sniperWarn(...args) {
  if (SNIPER_LOGGING_ENABLED) console.warn(...args);
}
function sniperError(...args) {
  if (SNIPER_LOGGING_ENABLED) console.error(...args);
}

// NFTStorefront contract for marketplace events (V1 - used by NFL All Day)
const STOREFRONT_CONTRACT = "A.4eb8a10cb9f87357.NFTStorefront";

// ============================================================
// FLOOR PRICE CACHE - Stores known floor prices for editions
// ============================================================

const floorPriceCache = new Map(); // editionId -> { floor, updatedAt }
const FLOOR_CACHE_TTL = 5 * 60 * 1000; // 5 minutes before refreshing
const MAX_FLOOR_CACHE_SIZE = 300; // Max cached editions (reduced to save memory)

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

    // Cleanup if cache gets too large - remove oldest entries
    if (floorPriceCache.size > MAX_FLOOR_CACHE_SIZE) {
      const entries = Array.from(floorPriceCache.entries())
        .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
      // Remove oldest 20% when over limit
      const toRemove = Math.floor(floorPriceCache.size * 0.2);
      for (let i = 0; i < toRemove; i++) {
        floorPriceCache.delete(entries[i][0]);
      }
    }
  }
}

// ============================================================
// LIVE SNIPER - Watch for listings below floor
// ============================================================

const sniperListings = []; // Array of ALL listings (in-memory cache)
const seenListingNfts = new Map(); // Track seen nftIds -> timestamp to prevent duplicates
const soldNfts = new Map(); // Track sold NFTs -> timestamp
const unlistedNfts = new Map(); // Track delisted NFTs -> timestamp
const MAX_SNIPER_LISTINGS = 300; // Reduced to save memory
const MAX_SEEN_NFTS = 500; // Max tracked seen NFTs (reduced)
const MAX_SOLD_NFTS = 500; // Max tracked sold NFTs (reduced)

// Database table for persistence (3-day retention)
async function ensureSniperListingsTable() {
  try {
    // First, create table if it doesn't exist
    await pgQuery(`
      CREATE TABLE IF NOT EXISTS sniper_listings (
        nft_id TEXT PRIMARY KEY,
        listing_data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    // Then, add columns if they don't exist (for existing tables)
    const alterStatements = [
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS listing_id TEXT`,
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS edition_id TEXT`,
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS listed_at TIMESTAMPTZ`,
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS is_sold BOOLEAN NOT NULL DEFAULT FALSE`,
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS is_unlisted BOOLEAN NOT NULL DEFAULT FALSE`,
      `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS buyer_address TEXT`
    ];

    for (const alterStmt of alterStatements) {
      try {
        await pgQuery(alterStmt);
      } catch (err) {
        // Ignore "column already exists" errors
        if (!err.message.includes('already exists') && !err.message.includes('duplicate')) {
          sniperError(`[Sniper] Error adding column:`, err.message);
        }
      }
    }

    // Create indexes if they don't exist
    const indexStatements = [
      `CREATE INDEX IF NOT EXISTS idx_sniper_listings_listed_at ON sniper_listings (listed_at DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_sniper_listings_updated_at ON sniper_listings (updated_at DESC)`,
      `CREATE INDEX IF NOT EXISTS idx_sniper_listings_status ON sniper_listings (is_sold, is_unlisted)`
    ];

    for (const indexStmt of indexStatements) {
      try {
        await pgQuery(indexStmt);
      } catch (err) {
        // Ignore index errors (they might already exist)
      }
    }

    // Update existing rows that don't have listed_at set (use updated_at as fallback)
    await pgQuery(`
      UPDATE sniper_listings 
      SET listed_at = updated_at 
      WHERE listed_at IS NULL
    `).catch(() => { }); // Ignore errors

    sniperLog("[Sniper] Database table and columns verified");
  } catch (err) {
    sniperError("[Sniper] Error ensuring sniper_listings table:", err.message);
  }
}

// Persist listing to database
async function persistSniperListing(listing) {
  try {
    const listedAt = listing.listedAt ? new Date(listing.listedAt) : new Date();
    await pgQuery(
      `INSERT INTO sniper_listings (
        nft_id, listing_id, edition_id, listing_data, listed_at, 
        updated_at, is_sold, is_unlisted, buyer_address
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, $8)
      ON CONFLICT (nft_id) 
      DO UPDATE SET 
        listing_id = COALESCE($2, sniper_listings.listing_id),
        listing_data = $4, 
        updated_at = NOW(),
        is_sold = $6,
        is_unlisted = $7,
        buyer_address = $8`,
      [
        listing.nftId,
        listing.listingId || null,
        listing.editionId || null,
        JSON.stringify(listing),
        listedAt,
        listing.isSold || false,
        listing.isUnlisted || false,
        listing.buyerAddr || null
      ]
    );
  } catch (err) {
    // Don't spam logs for persistence errors - it's not critical for functionality
    if (err.message && !err.message.includes('duplicate') && !err.message.includes('already exists')) {
      sniperError("[Sniper] Error persisting listing:", err.message);
    }
  }
}

async function addSniperListing(listing) {
  // Dedupe by nftId - same NFT can't be listed twice
  if (seenListingNfts.has(listing.nftId)) {
    // Update existing listing status
    const existingIndex = sniperListings.findIndex(l => l.nftId === listing.nftId);
    if (existingIndex >= 0) {
      // Update existing listing with latest data
      sniperListings[existingIndex] = { ...sniperListings[existingIndex], ...listing };
      await persistSniperListing(sniperListings[existingIndex]);
    }
    return;
  }

  seenListingNfts.set(listing.nftId, Date.now());

  // Cleanup seenListingNfts if it gets too large
  if (seenListingNfts.size > MAX_SEEN_NFTS) {
    const entries = Array.from(seenListingNfts.entries())
      .sort((a, b) => a[1] - b[1]); // Sort by timestamp
    const toRemove = Math.floor(seenListingNfts.size * 0.2); // Remove oldest 20%
    for (let i = 0; i < toRemove; i++) {
      seenListingNfts.delete(entries[i][0]);
    }
  }

  // NOTE: Don't override isSold/isUnlisted here - trust what's passed in the listing object
  // The calling code (processListingEvent) handles clearing sold/unlisted status for relistings

  // Add to front of array
  sniperListings.unshift(listing);

  // Keep only last N listings in memory
  if (sniperListings.length > MAX_SNIPER_LISTINGS) {
    const removed = sniperListings.pop();
    if (removed) seenListingNfts.delete(removed.nftId);
  }

  // Persist to database (fire and forget)
  persistSniperListing(listing).catch(() => { }); // Ignore errors

  // Log deals (below floor) - only if sniper logging enabled
  if (SNIPER_LOGGING_ENABLED && listing.dealPercent > 0 && !listing.isSold && !listing.isUnlisted) {
    console.log(`[SNIPER] 🎯 DEAL: ${listing.playerName} #${listing.serialNumber || '?'} - $${listing.listingPrice} (floor $${listing.floor}) - ${listing.dealPercent.toFixed(1)}% off!`);
  }
}

async function markListingAsSold(nftId, buyerAddr = null) {
  soldNfts.set(nftId, Date.now());

  // Cleanup soldNfts if it gets too large
  if (soldNfts.size > MAX_SOLD_NFTS) {
    const entries = Array.from(soldNfts.entries())
      .sort((a, b) => a[1] - b[1]); // Sort by timestamp
    const toRemove = Math.floor(soldNfts.size * 0.2); // Remove oldest 20%
    for (let i = 0; i < toRemove; i++) {
      soldNfts.delete(entries[i][0]);
    }
  }

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

  // Mark existing listings as sold in memory
  for (const listing of sniperListings) {
    if (listing.nftId === nftId) {
      listing.isSold = true;
      listing.isUnlisted = false; // Can't be both sold and unlisted
      if (buyerAddr) {
        listing.buyerAddr = buyerAddr;
        listing.buyerName = buyerName;
      }
      // Update in database
      persistSniperListing(listing).catch(() => { });
    }
  }

  // Also update database directly
  try {
    await pgQuery(
      `UPDATE sniper_listings 
       SET is_sold = TRUE, is_unlisted = FALSE, buyer_address = $1, updated_at = NOW()
       WHERE nft_id = $2`,
      [buyerAddr, nftId]
    );
  } catch (e) {
    // Ignore update errors
  }
}

// Reset all listings to unsold (useful after fixing verification logic)
async function resetAllListingsToUnsold() {
  try {
    console.log('[Sniper] Resetting all listings to unsold...');

    // Update database
    const result = await pgQuery(
      `UPDATE sniper_listings 
       SET is_sold = FALSE, buyer_address = NULL, updated_at = NOW()
       WHERE is_sold = TRUE`
    );

    sniperLog(`[Sniper] ✅ Reset ${result.rowCount || 0} listings to unsold in database`);

    // Clear in-memory soldNfts
    soldNfts.clear();
    console.log('[Sniper] ✅ Cleared in-memory soldNfts map');

    // Reset isSold flag on all in-memory listings
    let resetCount = 0;
    for (const listing of sniperListings) {
      if (listing.isSold) {
        listing.isSold = false;
        listing.buyerAddr = null;
        listing.buyerName = null;
        resetCount++;
      }
    }

    sniperLog(`[Sniper] ✅ Reset ${resetCount} listings to unsold in memory`);

    return {
      databaseReset: result.rowCount || 0,
      memoryReset: resetCount
    };
  } catch (err) {
    console.error('[Sniper] Error resetting listings to unsold:', err.message);
    throw err;
  }
}

async function markListingAsUnlisted(nftId, listingId = null) {
  unlistedNfts.set(nftId, Date.now());

  // Cleanup unlistedNfts if it gets too large
  if (unlistedNfts.size > MAX_SOLD_NFTS) {
    const entries = Array.from(unlistedNfts.entries())
      .sort((a, b) => a[1] - b[1]);
    const toRemove = Math.floor(unlistedNfts.size * 0.2);
    for (let i = 0; i < toRemove; i++) {
      unlistedNfts.delete(entries[i][0]);
    }
  }

  // Mark existing listings as unlisted in memory
  // If listingId provided, only mark that specific listing; otherwise mark all for this nftId
  for (const listing of sniperListings) {
    if (listing.nftId === nftId) {
      if (!listingId || listing.listingId === listingId) {
        listing.isUnlisted = true;
        listing.isSold = false; // Can't be both sold and unlisted
        // Update in database
        persistSniperListing(listing).catch(() => { });
      }
    }
  }

  // Also update database directly
  try {
    await pgQuery(
      `UPDATE sniper_listings 
       SET is_unlisted = TRUE, is_sold = FALSE, updated_at = NOW()
       WHERE nft_id = $1`,
      [nftId]
    );
  } catch (e) {
    // Ignore update errors
  }
}

// Process a new listing event from the Light Node
async function processListingEvent(event) {
  try {
    const { nftId, listingId, listingPrice, sellerAddr, timestamp, editionId: eventEditionId } = event;

    if (!nftId || !listingPrice) return;

    // Get edition info from our database
    let editionId = eventEditionId;
    let momentData = null;

    if (!editionId) {
      try {
        const result = await pgQuery(
          `SELECT edition_id, serial_number, first_name, last_name, team_name, tier, set_name, series_name, jersey_number
           FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`,
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
           FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`,
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

    // IMPORTANT: A new ListingAvailable event means this NFT is being (re)listed
    // Clear any old sold/unlisted status - this is a FRESH listing
    if (soldNfts.has(nftId)) {
      sniperLog(`[Sniper] 🔄 NFT ${nftId} was previously sold, now relisted - clearing sold status`);
      soldNfts.delete(nftId);
    }
    if (unlistedNfts.has(nftId)) {
      sniperLog(`[Sniper] 🔄 NFT ${nftId} was previously unlisted, now relisted - clearing unlisted status`);
      unlistedNfts.delete(nftId);
    }

    const listing = {
      nftId,
      listingId,
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
      buyerAddr: null,  // New listing has no buyer
      buyerName: null,
      isLowSerial: momentData?.serial_number && momentData.serial_number <= 100,
      isSold: false,     // New listing is NOT sold
      isUnlisted: false, // New listing is NOT unlisted
      listedAt: timestamp || new Date().toISOString(),
      // Direct moment link for faster buying (one click to buy page)
      listingUrl: `https://nflallday.com/moments/${nftId}`
    };

    addSniperListing(listing);

    // Update our floor cache with new floor (this listing might be the new floor)
    if (listingPrice < (previousFloor || Infinity)) {
      updateFloorCache(editionId, listingPrice);
    }

  } catch (err) {
    sniperError("[Sniper] Error processing listing event:", err.message);
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

  sniperLog("[Sniper] 🔴 Starting LIVE listing watcher (using Flow REST API)...");

  const checkForNewListings = async () => {
    try {
      // Get latest block height (try FindLab API first, fallback to Flow REST API)
      let currentBlock = await getLatestBlockHeightFindLab();
      if (currentBlock && currentBlock > 0) {
        // Successfully got block height from FindLab
      } else {
        // Fallback to Flow REST API
        try {
          const heightRes = await fetch(`${FLOW_REST_API}/v1/blocks?height=sealed`, {
            signal: AbortSignal.timeout(5000) // 5 second timeout
          });
          if (!heightRes.ok) {
            sniperError(`[Sniper] Failed to get block height from Flow REST API: ${heightRes.status} ${heightRes.statusText}`);
            return;
          }
          const heightData = await heightRes.json();
          const block = Array.isArray(heightData) ? heightData[0] : heightData;
          currentBlock = parseInt(block?.header?.height || 0);
          if (currentBlock > 0) {
            // Successfully got from Flow REST API fallback
          }
        } catch (err) {
          sniperError(`[Sniper] Error fetching block height from Flow REST API:`, err.message);
          return;
        }
      }

      if (!currentBlock || currentBlock <= 0) {
        sniperError(`[Sniper] Invalid block height after all attempts: ${currentBlock}`);
        return;
      }

      if (lastCheckedBlock === 0) {
        // Start from 100 blocks ago to catch any recent listings
        lastCheckedBlock = Math.max(0, currentBlock - 100);
        sniperLog(`[Sniper] 🔴 Starting watcher from block ${lastCheckedBlock} (current: ${currentBlock})`);
      }

      if (currentBlock <= lastCheckedBlock) {
        // No new blocks, but log occasionally to show it's alive
        if (Date.now() % 30000 < 3000) { // Every ~30 seconds
          sniperLog(`[Sniper] ⏳ Waiting for new blocks... (checked: ${lastCheckedBlock}, latest: ${currentBlock})`);
        }
        return;
      }

      // Query for ListingAvailable and ListingCompleted events in new blocks
      const startHeight = lastCheckedBlock + 1;
      const endHeight = Math.min(currentBlock, startHeight + 50); // Max 50 blocks at a time

      // Fetch all three event types in parallel with timeouts
      const fetchOptions = { signal: AbortSignal.timeout(10000) }; // 10 second timeout per request

      let listingRes, completedRes, removedRes;

      // Query NFTStorefront events (V1 - NFL All Day marketplace)
      sniperLog(`[Sniper] 🔍 Querying ${STOREFRONT_CONTRACT} events for blocks ${startHeight}-${endHeight}`);
      try {
        listingRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingAvailable&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);
      } catch (e) {
        sniperError(`[Sniper] Error fetching ListingAvailable events:`, e.message);
        listingRes = { ok: false };
      }

      try {
        completedRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingCompleted&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);
      } catch (e) {
        sniperError(`[Sniper] Error fetching ListingCompleted events:`, e.message);
        completedRes = { ok: false };
      }

      try {
        removedRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingRemoved&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);
      } catch (e) {
        sniperError(`[Sniper] Error fetching ListingRemoved events:`, e.message);
        removedRes = { ok: false };
      }

      let newListingCount = 0;
      let alldayCount = 0;
      let soldCount = 0;
      let unlistedCount = 0;

      // Process new listings
      if (listingRes.ok) {
        const eventData = await listingRes.json();
        sniperLog(`[Sniper] 📦 ListingAvailable response: ${eventData?.length || 0} blocks with events`);

        for (const block of eventData) {
          if (!block.events) continue;
          sniperLog(`[Sniper] 📦 Block ${block.block_height}: ${block.events.length} ListingAvailable events`);

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
              sniperLog(`[Sniper] ✨ Found AllDay listing: NFT ${nftId}, price $${listingPrice}, seller ${sellerAddr}`);

              // Get listingId if available
              const listingId = getField('listingResourceID')?.toString() || null;

              // Process this listing
              await processListingEvent({
                nftId,
                listingId,
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
              const listingResourceId = getField('listingResourceID')?.toString();

              // CRITICAL: Check the 'purchased' field to distinguish SOLD vs DELISTED
              // purchased = true  → SOLD (someone bought it)
              // purchased = false → DELISTED (seller cancelled the listing)
              // If 'purchased' field is missing or undefined, treat as DELISTED (safer)
              const purchasedField = getField('purchased');

              // Be very strict - only treat as sold if purchased is explicitly true
              let wasPurchased = false;
              if (purchasedField === true) {
                wasPurchased = true;
              } else if (typeof purchasedField === 'string' && purchasedField.toLowerCase() === 'true') {
                wasPurchased = true;
              } else if (typeof purchasedField === 'object' && purchasedField !== null) {
                // Cadence Bool might come as { value: true } or similar
                const boolValue = purchasedField.value ?? purchasedField;
                wasPurchased = boolValue === true || boolValue === 'true';
              }

              sniperLog(`[Sniper] 📋 ListingCompleted for NFT ${nftId} (listingId: ${listingResourceId}): purchased = ${JSON.stringify(purchasedField)} → wasPurchased = ${wasPurchased}`);

              // STRICT: Require listingResourceId to be present - this is the unique identifier for matching
              // Without it, we can't reliably match to our listings and risk false positives
              if (!listingResourceId) {
                sniperLog(`[Sniper] ⚠️  ListingCompleted without listingResourceId - skipping (nftId: ${nftId || 'none'})`);
                continue;
              }

              if (!nftId && !listingResourceId) continue;

              // Find the listing by listingResourceID first (most reliable), then by nftId
              let targetNftId = nftId;
              if (listingResourceId && !targetNftId) {
                const listing = sniperListings.find(l => l.listingId === listingResourceId);
                if (listing) {
                  targetNftId = listing.nftId;
                } else {
                  try {
                    const dbResult = await pgQuery(
                      `SELECT listing_data FROM sniper_listings WHERE listing_id = $1 LIMIT 1`,
                      [listingResourceId]
                    );
                    if (dbResult.rows.length > 0) {
                      const dbListing = typeof dbResult.rows[0].listing_data === 'string'
                        ? JSON.parse(dbResult.rows[0].listing_data)
                        : dbResult.rows[0].listing_data;
                      if (dbListing.nftId) {
                        targetNftId = dbListing.nftId;
                      }
                    }
                  } catch (e) {
                  }
                }
              }

              if (!targetNftId) continue;

              // CRITICAL: We MUST find a matching listing before marking as sold
              // Only mark as sold if:
              // 1. We have the listing in our system (memory or database)
              // 2. The listingResourceId matches exactly (most reliable)
              // 3. The sale event happened AFTER the listing was created

              let existingListing = null;

              // First, try to find by listingResourceId (most reliable)
              if (listingResourceId) {
                existingListing = sniperListings.find(l => l.listingId === listingResourceId);

                // If not in memory, check database
                if (!existingListing) {
                  try {
                    const dbResult = await pgQuery(
                      `SELECT nft_id, listing_id, listed_at, is_sold 
                       FROM sniper_listings 
                       WHERE listing_id = $1 
                       AND is_sold = FALSE 
                       LIMIT 1`,
                      [listingResourceId]
                    );
                    if (dbResult.rows.length > 0) {
                      const dbRow = dbResult.rows[0];
                      existingListing = {
                        nftId: dbRow.nft_id,
                        listingId: dbRow.listing_id,
                        listedAt: dbRow.listed_at,
                        isSold: dbRow.is_sold
                      };
                    }
                  } catch (e) {
                    // Ignore DB errors
                  }
                }
              }

              // If we don't have a listingResourceId match, we CANNOT reliably mark as sold
              // (an NFT could have been sold and relisted, so matching by nftId alone is unsafe)
              if (!existingListing) {
                sniperLog(`[Sniper] ⚠️  ListingCompleted event for ${targetNftId} (listingId: ${listingResourceId || 'none'}) but no matching listing found in our system - skipping`);
                continue;
              }

              // If already marked as sold, skip
              if (existingListing.isSold) {
                continue;
              }

              // Verify the sale happened AFTER the listing was created
              if (existingListing.listedAt) {
                const listedAtDate = new Date(existingListing.listedAt);
                const eventTimestamp = block.block_timestamp || event.timestamp || block.timestamp;
                const eventDate = eventTimestamp ? new Date(eventTimestamp) : null;

                if (eventDate && !isNaN(eventDate.getTime()) && !isNaN(listedAtDate.getTime())) {
                  // Add a small buffer (2 minutes) to account for timing variations
                  const bufferMs = 2 * 60 * 1000;
                  if (eventDate.getTime() < (listedAtDate.getTime() - bufferMs)) {
                    sniperLog(`[Sniper] ⏭️  Skipping old ListingCompleted for listing ${listingResourceId} - event was ${Math.round((listedAtDate.getTime() - eventDate.getTime()) / 1000 / 60)} minutes before listing`);
                    continue;
                  }
                } else {
                  // Can't verify timing - be very cautious, but if we have exact listingId match, it's probably valid
                  sniperLog(`[Sniper] ⚠️  Cannot verify timing for listing ${listingResourceId}, but proceeding with exact listingId match`);
                }
              }

              // Extract buyer address - best method is from AllDay.Deposit event in same tx
              // The Deposit event has 'to' field = buyer address, 'id' field = NFT ID
              let buyerAddr = null;

              // First try to get buyer from ListingCompleted event fields
              buyerAddr = getField('purchaser')?.toString()?.toLowerCase() ||
                getField('buyer')?.toString()?.toLowerCase() ||
                getField('recipient')?.toString()?.toLowerCase() ||
                null;

              // If not found, query AllDay.Deposit event for this block to find buyer
              // The Deposit event fires when an NFT is transferred TO the buyer
              if (!buyerAddr && block.block_height && targetNftId) {
                try {
                  const depositRes = await fetch(
                    `${FLOW_REST_API}/v1/events?type=A.e4cf4bdc1751c65d.AllDay.Deposit&start_height=${block.block_height}&end_height=${block.block_height}`,
                    { signal: AbortSignal.timeout(5000) }
                  );

                  if (depositRes.ok) {
                    const depositData = await depositRes.json();
                    sniperLog(`[Sniper] 🔍 Checking ${depositData?.length || 0} blocks for Deposit events for NFT ${targetNftId}`);

                    // Find the Deposit event for our NFT ID
                    for (const depositBlock of depositData) {
                      if (!depositBlock.events) continue;

                      for (const depositEvent of depositBlock.events) {
                        try {
                          let depositPayload = depositEvent.payload;
                          if (typeof depositPayload === 'string') {
                            depositPayload = JSON.parse(Buffer.from(depositPayload, 'base64').toString());
                          }

                          if (!depositPayload?.value?.fields) continue;

                          const depositFields = depositPayload.value.fields;

                          // Extract NFT ID - can be nested in different ways
                          const idField = depositFields.find(f => f.name === 'id');
                          let depositNftId = null;
                          if (idField) {
                            // Try to extract the numeric ID from nested structures
                            const extractId = (obj) => {
                              if (!obj) return null;
                              if (typeof obj === 'string' || typeof obj === 'number') return String(obj);
                              if (typeof obj === 'object') {
                                if (obj.value !== undefined) return extractId(obj.value);
                              }
                              return null;
                            };
                            depositNftId = extractId(idField);
                          }

                          // Extract 'to' address - Address type can be deeply nested in Cadence
                          const toField = depositFields.find(f => f.name === 'to');
                          let depositTo = null;
                          if (toField) {
                            // Debug: log the structure to understand it
                            // sniperLog(`[Sniper] DEBUG toField:`, JSON.stringify(toField, null, 2));

                            // Try to extract address from various nested structures
                            // Cadence Address can be: { value: "0x..." } or { value: { value: "0x..." } }
                            const extractAddress = (obj) => {
                              if (!obj) return null;
                              if (typeof obj === 'string' && obj.startsWith('0x')) return obj;
                              if (typeof obj === 'object') {
                                // Try common patterns
                                if (typeof obj.value === 'string' && obj.value.startsWith('0x')) return obj.value;
                                if (obj.value && typeof obj.value.value === 'string') return obj.value.value;
                                if (obj.value && obj.value.value && typeof obj.value.value.value === 'string') return obj.value.value.value;
                                // Recurse into value if it's an object
                                if (obj.value && typeof obj.value === 'object') return extractAddress(obj.value);
                              }
                              return null;
                            };

                            depositTo = extractAddress(toField);

                            // Log what we found for debugging
                            if (depositTo) {
                              sniperLog(`[Sniper] 📍 Extracted 'to' address: ${depositTo}`);
                            } else {
                              sniperLog(`[Sniper] ⚠️ Could not extract 'to' address from:`, JSON.stringify(toField).substring(0, 200));
                            }
                          }

                          // Normalize both IDs to strings for comparison
                          const targetIdStr = targetNftId?.toString();
                          const depositIdStr = depositNftId?.toString();

                          // Match by NFT ID - this is the buyer receiving the NFT
                          if (depositIdStr && targetIdStr && depositIdStr === targetIdStr && depositTo) {
                            // Ensure depositTo is a valid address string
                            if (typeof depositTo === 'string' && depositTo.startsWith('0x')) {
                              buyerAddr = depositTo.toLowerCase();
                              sniperLog(`[Sniper] 🎯 Found buyer ${buyerAddr} from AllDay.Deposit event for NFT ${targetNftId}`);
                              break;
                            } else {
                              sniperLog(`[Sniper] ⚠️ depositTo is not a valid address: ${typeof depositTo} - ${JSON.stringify(depositTo).substring(0, 100)}`);
                            }
                          }
                        } catch (e) {
                          sniperWarn(`[Sniper] Error parsing Deposit event:`, e.message);
                        }
                      }
                      if (buyerAddr) break;
                    }

                    if (!buyerAddr) {
                      sniperLog(`[Sniper] ⚠️ No matching Deposit event found for NFT ${targetNftId} in block ${block.block_height}`);
                    }
                  } else {
                    sniperWarn(`[Sniper] Deposit events fetch failed: ${depositRes.status}`);
                  }
                } catch (e) {
                  sniperWarn(`[Sniper] Could not fetch Deposit events for buyer lookup:`, e.message);
                }
              }

              // Fallback: try to get from transaction - query tx result for all events
              if (!buyerAddr) {
                let txId = event.transaction_id || event.transactionId ||
                  block.transaction_id || block.transactions?.[0]?.id;

                if (txId) {
                  try {
                    // Query transaction result which includes ALL events from that transaction
                    // This is more reliable than querying by block since we get the exact tx
                    const txResultRes = await fetch(`${FLOW_REST_API}/v1/transaction_results/${txId}`,
                      { signal: AbortSignal.timeout(5000) });

                    if (txResultRes.ok) {
                      const txResult = await txResultRes.json();

                      // Look for AllDay.Deposit event in this transaction's events
                      if (txResult.events && txResult.events.length > 0) {
                        for (const txEvent of txResult.events) {
                          if (txEvent.type && txEvent.type.includes('AllDay') && txEvent.type.includes('Deposit')) {
                            try {
                              let eventPayload = txEvent.payload;
                              if (typeof eventPayload === 'string') {
                                eventPayload = JSON.parse(Buffer.from(eventPayload, 'base64').toString());
                              }

                              if (eventPayload?.value?.fields) {
                                const fields = eventPayload.value.fields;
                                const idField = fields.find(f => f.name === 'id');
                                const toField = fields.find(f => f.name === 'to');

                                const eventNftId = idField?.value?.value?.toString() || idField?.value?.toString();
                                const eventTo = toField?.value?.value?.toString() || toField?.value?.toString();

                                if (eventNftId?.toString() === targetNftId?.toString() && eventTo) {
                                  buyerAddr = eventTo.toLowerCase();
                                  sniperLog(`[Sniper] 🎯 Found buyer ${buyerAddr} from tx_result Deposit event for NFT ${targetNftId}`);
                                  break;
                                }
                              }
                            } catch (e) {
                              // Skip malformed events
                            }
                          }
                        }
                      }

                      // If still no buyer, try authorizer as last resort
                      if (!buyerAddr) {
                        const txRes = await fetch(`${FLOW_REST_API}/v1/transactions/${txId}`,
                          { signal: AbortSignal.timeout(5000) });
                        if (txRes.ok) {
                          const txData = await txRes.json();
                          if (txData?.payload?.authorizers && txData.payload.authorizers.length > 0) {
                            buyerAddr = txData.payload.authorizers[0]?.toLowerCase();
                            sniperLog(`[Sniper] 📝 Using tx authorizer as buyer: ${buyerAddr}`);
                          } else if (txData?.payload?.payer) {
                            buyerAddr = txData.payload.payer?.toLowerCase();
                            sniperLog(`[Sniper] 📝 Using tx payer as buyer: ${buyerAddr}`);
                          }
                        }
                      }
                    }
                  } catch (e) {
                    sniperWarn(`[Sniper] Error fetching transaction ${txId}:`, e.message);
                  }
                } else {
                  sniperLog(`[Sniper] ⚠️ No transaction ID available for buyer lookup`);
                }
              }

              // We have exact listingResourceId match - this is very reliable
              // Buyer address is preferred but not required for exact listingResourceId matches
              // (listingResourceId is unique per listing, so if it matches, it's definitely sold)

              // Use the 'purchased' field to determine if this was a sale or delisting
              if (wasPurchased) {
                // This was a SALE - someone bought the listing
                if (buyerAddr && buyerAddr.trim() !== '' && buyerAddr.match(/^0x[a-f0-9]{4,64}$/i)) {
                  // We have a valid buyer address
                  await markListingAsSold(targetNftId, buyerAddr);
                  sniperLog(`[Sniper] ✅ Marked ${targetNftId} (listing ${listingResourceId}) as SOLD to ${buyerAddr}`);
                } else if (listingResourceId && existingListing && existingListing.listingId === listingResourceId) {
                  // Exact listingResourceId match but no buyer - still mark as sold (listingResourceId is unique)
                  await markListingAsSold(targetNftId, null);
                  sniperLog(`[Sniper] ✅ Marked ${targetNftId} (listing ${listingResourceId}) as SOLD (buyer unknown, but exact listingId match)`);
                } else {
                  // No buyer and no exact listingResourceId match - skip to avoid false positive
                  sniperLog(`[Sniper] ⚠️  ListingCompleted (purchased=true) for ${targetNftId} but no buyer and no exact listingId match - skipping`);
                  continue;
                }
                soldCount++;
              } else {
                // This was a DELISTING - seller cancelled the listing (purchased = false)
                await markListingAsUnlisted(targetNftId, listingResourceId);
                sniperLog(`[Sniper] ❌ Marked ${targetNftId} (listing ${listingResourceId}) as DELISTED (purchased=false)`);
                unlistedCount++;
              }

            } catch (e) {
              // Skip malformed events
            }
          }
        }
      }

      // Process removed listings (delisted/cancelled)
      if (removedRes.ok) {
        const eventData = await removedRes.json();

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
              const listingResourceId = getField('listingResourceID')?.toString();

              let targetNftId = nftId;
              if (listingResourceId && !targetNftId) {
                const listing = sniperListings.find(l => l.listingId === listingResourceId);
                if (listing) {
                  targetNftId = listing.nftId;
                } else {
                  try {
                    const dbResult = await pgQuery(
                      `SELECT listing_data FROM sniper_listings WHERE listing_id = $1 LIMIT 1`,
                      [listingResourceId]
                    );
                    if (dbResult.rows.length > 0) {
                      const dbListing = typeof dbResult.rows[0].listing_data === 'string'
                        ? JSON.parse(dbResult.rows[0].listing_data)
                        : dbResult.rows[0].listing_data;
                      if (dbListing.nftId) {
                        targetNftId = dbListing.nftId;
                      }
                    }
                  } catch (e) {
                  }
                }
              }

              if (!targetNftId) continue;

              await markListingAsUnlisted(targetNftId);
              sniperLog(`[Sniper] ✅ Marked ${targetNftId} as delisted`);
              unlistedCount++;

            } catch (e) {
              // Skip malformed events
            }
          }
        }
      }

      if (!listingRes.ok && !completedRes.ok && !removedRes.ok) {
        sniperLog(`[Sniper] ⚠️ All event fetches failed - listingRes: ${listingRes.status || 'error'}, completedRes: ${completedRes.status || 'error'}, removedRes: ${removedRes.status || 'error'}`);
        lastCheckedBlock = endHeight;
        return;
      }

      if (alldayCount > 0 || soldCount > 0 || unlistedCount > 0) {
        sniperLog(`[Sniper] Block ${startHeight}-${endHeight}: ${alldayCount} new listings, ${soldCount} sold, ${unlistedCount} delisted (total in memory: ${sniperListings.length})`);
      }

      // Log every 10 blocks even if no events (to show it's working)
      if ((startHeight % 10 === 0) || (alldayCount > 0 || soldCount > 0 || unlistedCount > 0)) {
        sniperLog(`[Sniper] Checked blocks ${startHeight}-${endHeight}, current block: ${currentBlock}, listings in memory: ${sniperListings.length}`);
      }

      lastCheckedBlock = endHeight;

    } catch (err) {
      sniperError("[Sniper] Error checking for listings:", err.message);
      // Don't let errors stop the watcher - log and continue
      // Add exponential backoff on repeated errors
    }
  };

  // Check every 3 seconds for real-time sniping (slightly slower to reduce load)
  // Use a more robust interval with error handling
  let consecutiveErrors = 0;
  const checkWithRetry = async () => {
    try {
      await checkForNewListings();
      consecutiveErrors = 0; // Reset on success
    } catch (err) {
      consecutiveErrors++;
      if (consecutiveErrors > 5) {
        sniperError(`[Sniper] Too many consecutive errors (${consecutiveErrors}), backing off...`);
        // Exponential backoff: wait longer after multiple errors
        await new Promise(resolve => setTimeout(resolve, Math.min(60000, 1000 * Math.pow(2, consecutiveErrors - 5))));
      }
    }
  };

  setInterval(checkWithRetry, 3000); // 3 second intervals
  checkWithRetry(); // Run immediately
}

async function verifyListingStatusFast(nftId, listingId, listedAt, sellerAddr) {
  try {
    if (!nftId || !sellerAddr || !listedAt) return null;

    const listedAtDate = new Date(listedAt);
    if (isNaN(listedAtDate.getTime())) {
      console.warn(`[Verify] Invalid listedAt date for ${nftId}: ${listedAt}`);
      return null;
    }

    // Try FindLab API first for block height (faster and more reliable)
    let latestHeight = await getLatestBlockHeightFindLab();
    if (!latestHeight || latestHeight <= 0) {
      // Fallback to Flow REST API
      try {
        const latestHeightRes = await fetch(`${FLOW_REST_API}/v1/blocks?height=sealed`, {
          signal: AbortSignal.timeout(5000)
        });
        if (!latestHeightRes.ok) {
          console.warn(`[Verify] Failed to get block height: ${latestHeightRes.status}`);
          return null;
        }
        const latestHeightData = await latestHeightRes.json();
        latestHeight = parseInt(latestHeightData[0]?.header?.height || 0);
        if (!latestHeight || latestHeight <= 0) {
          console.warn(`[Verify] Invalid block height returned: ${latestHeight}`);
          return null;
        }
      } catch (err) {
        console.warn(`[Verify] Error fetching block height:`, err.message);
        return null;
      }
    }

    // Estimate the block height when the listing was created
    // Flow produces ~2 blocks per second, so calculate approximate block height at listing time
    const now = Date.now();
    const secondsSinceListing = Math.max(0, (now - listedAtDate.getTime()) / 1000);
    const blocksSinceListing = Math.floor(secondsSinceListing * 2); // ~2 blocks/second
    const estimatedListedAtBlock = Math.max(0, latestHeight - blocksSinceListing);

    // Only check events that happened AFTER the listing was created
    // Use a small buffer (50 blocks = ~25 seconds) to account for timing variations
    const startHeight = Math.max(0, estimatedListedAtBlock - 50);

    const fetchOptions = { signal: AbortSignal.timeout(8000) };

    const getField = (fields, name) => {
      const f = fields.find(x => x.name === name);
      if (!f) return null;
      if (f.value?.value?.value) return f.value.value.value;
      if (f.value?.value) return f.value.value;
      return f.value;
    };

    // Query for ListingCompleted events only after the listing was created (use V2 contract)
    const completedRes = await fetch(
      `${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingCompleted&start_height=${startHeight}&end_height=${latestHeight}`,
      fetchOptions
    );

    if (completedRes.ok) {
      const completedData = await completedRes.json();
      for (const block of completedData) {
        if (!block.events) continue;
        const blockHeight = parseInt(block.block_height || block.height || 0);

        // CRITICAL: Only consider events that happened AFTER the listing was created
        if (blockHeight < estimatedListedAtBlock) {
          continue; // This event happened before the listing, ignore it
        }

        for (const event of block.events) {
          try {
            let payload = event.payload;
            if (typeof payload === 'string') {
              payload = JSON.parse(Buffer.from(payload, 'base64').toString());
            }

            if (!payload?.value?.fields) continue;

            const fields = payload.value.fields;
            const eventListingId = getField(fields, 'listingResourceID')?.toString();
            const eventNftId = getField(fields, 'nftID')?.toString();

            // STRICT MATCHING: 
            // 1. If we have listingId, require exact listingId match (most reliable)
            // 2. If no listingId in our data, we CANNOT safely match by nftId alone
            //    (NFT could have been sold and relisted multiple times)
            let matches = false;
            if (listingId && eventListingId) {
              // We have both listingIds - they must match exactly
              matches = (eventListingId === listingId) && (eventNftId === nftId);
            }
            // REMOVED: else if (eventNftId === nftId) - this was causing false positives!
            // If we don't have listingId in our data, we cannot safely verify which listing was sold

            if (matches && blockHeight >= estimatedListedAtBlock) {
              let buyerAddr = null;
              const txId = event.transaction_id || block.transactions?.[0]?.id;
              if (txId) {
                try {
                  const txRes = await fetch(`${FLOW_REST_API}/v1/transactions/${txId}`, {
                    signal: AbortSignal.timeout(5000)
                  });
                  if (txRes.ok) {
                    const txData = await txRes.json();
                    if (txData?.payload?.authorizers && txData.payload.authorizers.length > 0) {
                      buyerAddr = txData.payload.authorizers[0]?.toLowerCase();
                    } else if (txData?.payload?.payer) {
                      buyerAddr = txData.payload.payer?.toLowerCase();
                    }
                  }
                } catch (e) {
                }
              }

              // If we have exact listingResourceId match, it's definitely sold (even without buyer)
              // listingResourceId is unique per listing, so this is very reliable
              if (listingId && eventListingId === listingId) {
                if (buyerAddr && buyerAddr.match(/^0x[a-f0-9]{4,64}$/i)) {
                  console.log(`[Verify] ✅ Found ListingCompleted for listing ${listingId} (nft: ${nftId}) - buyer: ${buyerAddr}`);
                  return { isSold: true, isUnlisted: false, buyerAddr };
                } else {
                  // Exact listingResourceId match but no buyer - still mark as sold (listingResourceId is unique)
                  console.log(`[Verify] ✅ Found ListingCompleted for listing ${listingId} (nft: ${nftId}) - exact listingId match, marking as sold (buyer unknown)`);
                  return { isSold: true, isUnlisted: false, buyerAddr: null };
                }
              } else if (eventNftId === nftId && !listingId) {
                // Only nftId match and we don't have listingId in our data - require buyer address
                if (buyerAddr && buyerAddr.match(/^0x[a-f0-9]{4,64}$/i)) {
                  console.log(`[Verify] ✅ Found ListingCompleted for nft ${nftId} (no listingId in our data) - buyer: ${buyerAddr}`);
                  return { isSold: true, isUnlisted: false, buyerAddr };
                } else {
                  console.log(`[Verify] ⚠️  Found ListingCompleted for nft ${nftId} but no listingId in our data and no buyer (txId: ${txId || 'none'}) - skipping to avoid false positive`);
                }
              } else {
                console.log(`[Verify] ⚠️  Match conditions not met: listingId=${listingId}, eventListingId=${eventListingId}, nftId=${nftId}, eventNftId=${eventNftId}`);
              }
            }
          } catch (e) {
          }
        }
      }
    }

    // Query for ListingRemoved events only after the listing was created
    const removedRes = await fetch(
      `${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingRemoved&start_height=${startHeight}&end_height=${latestHeight}`,
      fetchOptions
    );

    if (removedRes.ok) {
      const removedData = await removedRes.json();
      for (const block of removedData) {
        if (!block.events) continue;
        const blockHeight = parseInt(block.block_height || block.height || 0);

        // Only consider events that happened AFTER the listing was created
        if (blockHeight < estimatedListedAtBlock) {
          continue;
        }

        for (const event of block.events) {
          try {
            let payload = event.payload;
            if (typeof payload === 'string') {
              payload = JSON.parse(Buffer.from(payload, 'base64').toString());
            }

            if (!payload?.value?.fields) continue;

            const fields = payload.value.fields;
            const eventListingId = getField(fields, 'listingResourceID')?.toString();
            const eventNftId = getField(fields, 'nftID')?.toString();

            // STRICT MATCHING: Only match if we have exact listingId match
            let matches = false;
            if (listingId && eventListingId) {
              matches = (eventListingId === listingId) && (eventNftId === nftId);
            }
            // REMOVED: else if (eventNftId === nftId) - this causes false positives for delisted items too!

            if (matches && blockHeight >= estimatedListedAtBlock) {
              return { isSold: false, isUnlisted: true, buyerAddr: null };
            }
          } catch (e) {
          }
        }
      }
    }

    return null;
  } catch (err) {
    return null;
  }
}

async function verifyListingStatus(nftId, listingId, listedAt) {
  try {
    if (!nftId) {
      return null;
    }

    const listing = sniperListings.find(l => l.nftId === nftId);
    const sellerAddr = listing?.sellerAddr;

    if (!sellerAddr) {
      return null;
    }

    return await verifyListingStatusFast(nftId, listingId, listedAt, sellerAddr);
  } catch (err) {
    return null;
  }
}

// Verify and update status of existing listings
async function verifyExistingListingsStatus() {
  try {
    // Check both memory and database listings
    const listingsToVerify = [];

    // Get from memory (active listings only)
    for (const listing of sniperListings) {
      if (!listing.isSold && !listing.isUnlisted && listing.nftId) {
        listingsToVerify.push({
          nftId: listing.nftId,
          listingId: listing.listingId,
          listedAt: listing.listedAt,
          source: 'memory'
        });
      }
    }

    // Also get from database (recent listings that might not be in memory)
    // Check last 3 days to catch older sold listings
    const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
    const dbResult = await pgQuery(
      `SELECT nft_id, listing_id, listed_at
       FROM sniper_listings 
       WHERE is_sold = FALSE 
         AND is_unlisted = FALSE
         AND listed_at >= $1
       ORDER BY listed_at DESC
       LIMIT 200`,
      [threeDaysAgo]
    );

    const memoryNftIds = new Set(listingsToVerify.map(l => l.nftId));
    for (const row of dbResult.rows) {
      if (!memoryNftIds.has(row.nft_id)) {
        listingsToVerify.push({
          nftId: row.nft_id,
          listingId: row.listing_id,
          listedAt: row.listed_at,
          source: 'database'
        });
      }
    }

    if (listingsToVerify.length === 0) {
      sniperLog(`[Sniper] No active listings to verify`);
      return;
    }

    sniperLog(`[Sniper] Verifying status of ${listingsToVerify.length} active listings...`);
    let updated = 0;
    let checked = 0;
    let errors = 0;

    const batchSize = 10;
    for (let i = 0; i < listingsToVerify.length; i += batchSize) {
      const batch = listingsToVerify.slice(i, i + batchSize);

      await Promise.all(batch.map(async (item) => {
        try {
          checked++;

          const listing = sniperListings.find(l => l.nftId === item.nftId);
          const sellerAddr = listing?.sellerAddr;

          if (!sellerAddr) {
            return;
          }

          const status = await verifyListingStatusFast(
            item.nftId,
            item.listingId,
            item.listedAt,
            sellerAddr
          );

          if (!status) {
            return;
          }

          if (status.isSold) {
            // Mark as sold - buyer address is optional if we have exact listingResourceId match
            await markListingAsSold(item.nftId, status.buyerAddr || null);
            if (status.buyerAddr) {
              console.log(`[Sniper Verify] ✅ Marked ${item.nftId} (listing ${item.listingId || 'unknown'}) as sold to ${status.buyerAddr}`);
            } else {
              console.log(`[Sniper Verify] ✅ Marked ${item.nftId} (listing ${item.listingId || 'unknown'}) as sold (buyer unknown, but exact listingId match)`);
            }
            updated++;
          } else if (status.isUnlisted) {
            await markListingAsUnlisted(item.nftId);
            updated++;
          }
        } catch (e) {
          errors++;
          sniperError(`[Sniper] Error checking ${item.nftId}:`, e.message);
        }
      }));

      if (i + batchSize < listingsToVerify.length) {
        await new Promise(resolve => setTimeout(resolve, 500));
      }
    }

    if (updated > 0) {
      sniperLog(`[Sniper] ✅ Updated ${updated} listings (${errors} errors)`);
    }
  } catch (err) {
    sniperError("[Sniper] Error verifying listing status:", err.message);
    sniperError("[Sniper] Stack:", err.stack);
  }
}

// Load recent listings from database on startup (last 3 days)
async function loadRecentListingsFromDB() {
  try {
    await ensureSniperListingsTable();

    const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);

    const result = await pgQuery(
      `SELECT listing_data, is_sold, is_unlisted, buyer_address, listed_at, listing_id
       FROM sniper_listings 
       WHERE listed_at >= $1
       ORDER BY listed_at DESC 
       LIMIT 500`,
      [threeDaysAgo]
    );

    if (result.rows.length === 0) {
      sniperLog("[Sniper] No recent listings found in database, will populate from watcher");
      return;
    }

    sniperLog(`[Sniper] Loading ${result.rows.length} recent listings from database...`);

    let loaded = 0;
    let skipped = 0;

    for (const row of result.rows) {
      try {
        const listing = typeof row.listing_data === 'string'
          ? JSON.parse(row.listing_data)
          : row.listing_data;

        if (!listing.nftId) continue;

        // Update status from database
        listing.isSold = row.is_sold || false;
        listing.isUnlisted = row.is_unlisted || false;
        if (row.buyer_address) {
          listing.buyerAddr = row.buyer_address;
        }
        if (row.listing_id && !listing.listingId) {
          listing.listingId = row.listing_id;
        }

        // Add to tracking maps
        if (listing.isSold) {
          soldNfts.set(listing.nftId, Date.now());
        }
        if (listing.isUnlisted) {
          unlistedNfts.set(listing.nftId, Date.now());
        }
        seenListingNfts.set(listing.nftId, Date.now());

        // Add to memory (avoid duplicates)
        if (!sniperListings.find(l => l.nftId === listing.nftId)) {
          sniperListings.push(listing);
          loaded++;
        } else {
          skipped++;
        }
      } catch (e) {
        sniperError("[Sniper] Error parsing listing from DB:", e.message);
      }
    }

    // Sort by listedAt (most recent first)
    sniperListings.sort((a, b) => {
      const aTime = new Date(a.listedAt || 0).getTime();
      const bTime = new Date(b.listedAt || 0).getTime();
      return bTime - aTime;
    });

    // Keep only the most recent 500 in memory
    if (sniperListings.length > MAX_SNIPER_LISTINGS) {
      sniperListings.splice(MAX_SNIPER_LISTINGS);
    }

    sniperLog(`[Sniper] Loaded ${loaded} listings from database (${skipped} duplicates skipped)`);

    // After loading, verify status of listings that aren't marked as sold/unlisted
    // Do this in background so it doesn't block startup - wait 30 seconds
    // This will catch any listings that were sold/delisted before the watcher was running
    setTimeout(() => {
      sniperLog("[Sniper] 🔍 Running initial verification on loaded listings...");
      verifyExistingListingsStatus().catch(err => {
        sniperError("[Sniper] Error in background status verification:", err.message);
      });
    }, 30000); // Wait 30 seconds after startup to let everything settle
  } catch (err) {
    sniperError("[Sniper] Error loading recent listings from database:", err.message);
    // Don't throw - continue with empty array, watcher will populate it
  }
}

// Periodic cleanup to prevent memory leaks
function cleanupSniperMemory() {
  try {
    // Clean up old floor cache entries (older than 1 hour)
    const oneHourAgo = Date.now() - (60 * 60 * 1000);
    for (const [editionId, cache] of floorPriceCache.entries()) {
      if (cache.updatedAt < oneHourAgo) {
        floorPriceCache.delete(editionId);
      }
    }

    // If still over limit, remove oldest
    if (floorPriceCache.size > MAX_FLOOR_CACHE_SIZE) {
      const entries = Array.from(floorPriceCache.entries())
        .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
      const toRemove = floorPriceCache.size - MAX_FLOOR_CACHE_SIZE;
      for (let i = 0; i < toRemove; i++) {
        floorPriceCache.delete(entries[i][0]);
      }
    }

    // Clean up old seen NFTs (older than 24 hours)
    const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
    for (const [nftId, timestamp] of seenListingNfts.entries()) {
      if (timestamp < oneDayAgo) {
        seenListingNfts.delete(nftId);
      }
    }

    // If still over limit, remove oldest
    if (seenListingNfts.size > MAX_SEEN_NFTS) {
      const entries = Array.from(seenListingNfts.entries())
        .sort((a, b) => a[1] - b[1]);
      const toRemove = seenListingNfts.size - MAX_SEEN_NFTS;
      for (let i = 0; i < toRemove; i++) {
        seenListingNfts.delete(entries[i][0]);
      }
    }

    // Clean up old sold NFTs (older than 7 days)
    const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
    for (const [nftId, timestamp] of soldNfts.entries()) {
      if (timestamp < sevenDaysAgo) {
        soldNfts.delete(nftId);
      }
    }

    // If still over limit, remove oldest
    if (soldNfts.size > MAX_SOLD_NFTS) {
      const entries = Array.from(soldNfts.entries())
        .sort((a, b) => a[1] - b[1]);
      const toRemove = soldNfts.size - MAX_SOLD_NFTS;
      for (let i = 0; i < toRemove; i++) {
        soldNfts.delete(entries[i][0]);
      }
    }

    // Log memory usage
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);
    const heapTotalMB = Math.round(memUsage.heapTotal / 1024 / 1024);

    sniperLog(`[Cleanup] Memory: ${heapUsedMB}/${heapTotalMB}MB | Floor: ${floorPriceCache.size} | Seen: ${seenListingNfts.size} | Sold: ${soldNfts.size} | Listings: ${sniperListings.length}`);

    // Force garbage collection if available (run node with --expose-gc)
    if (global.gc) {
      global.gc();
      console.log("[Cleanup] Forced garbage collection");
    }
  } catch (err) {
    console.error("[Cleanup] Error during cleanup:", err.message);
  }
}

// Global cleanup for all caches (runs less frequently)
function globalMemoryCleanup() {
  try {
    const memUsage = process.memoryUsage();
    const heapUsedMB = Math.round(memUsage.heapUsed / 1024 / 1024);

    console.log(`[Memory] Heap used: ${heapUsedMB}MB`);

    // If memory is getting high (>200MB), be more aggressive with cleanup
    if (heapUsedMB > 200) {
      console.log("[Memory] High memory usage detected, running aggressive cleanup...");

      // Clear expired graphql cache entries
      const now = Date.now();
      for (const [key, entry] of graphqlCache.entries()) {
        if (now >= entry.expiresAt) {
          graphqlCache.delete(key);
        }
      }

      // Trim sniper listings more aggressively
      if (sniperListings.length > 200) {
        sniperListings.length = 200;
      }

      // Clear old caches
      if (typeof rarityLeaderboardCache !== 'undefined') {
        rarityLeaderboardCache = null;
      }
      if (typeof setTotalsCache !== 'undefined') {
        setTotalsCache = null;
      }

      console.log(`[Memory] After cleanup - GraphQL cache: ${graphqlCache.size}, Listings: ${sniperListings.length}`);
    }
  } catch (err) {
    console.error("[Memory] Error during global cleanup:", err.message);
  }
}

// Periodic cleanup to remove old listings from database (>3 days)
async function cleanupOldListings() {
  try {
    const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
    const result = await pgQuery(
      `DELETE FROM sniper_listings WHERE listed_at < $1`,
      [threeDaysAgo]
    );
    if (result.rowCount > 0) {
      sniperLog(`[Sniper] Cleaned up ${result.rowCount} old listings (>3 days)`);
    }
  } catch (err) {
    sniperError("[Sniper] Error cleaning up old listings:", err.message);
  }
}

// Start periodic cleanup every 5 minutes (memory) and every hour (database)
setInterval(cleanupSniperMemory, 5 * 60 * 1000); // Every 5 minutes
setInterval(cleanupOldListings, 60 * 60 * 1000); // Every hour
setInterval(globalMemoryCleanup, 2 * 60 * 1000); // Every 2 minutes for memory check

// Verify listing status periodically (every 5 minutes)
setInterval(() => {
  verifyExistingListingsStatus().catch(err => {
    sniperError("[Sniper] Error in periodic status verification:", err.message);
  });
}, 5 * 60 * 1000); // Every 5 minutes

// Also run verification once immediately after startup (after 30 seconds to let things settle)
setTimeout(() => {
  sniperLog("[Sniper] Running initial status verification on all active listings...");
  verifyExistingListingsStatus().catch(err => {
    sniperError("[Sniper] Error in initial status verification:", err.message);
  });
}, 30000); // After 30 seconds

// Initialize sniper on server start
async function initializeSniper() {
  try {
    sniperLog("[Sniper] 🚀 Initializing sniper system...");

    // First, ensure database table is set up correctly
    await ensureSniperListingsTable();

    // Then, load recent listings from database
    await loadRecentListingsFromDB();

    sniperLog(`[Sniper] ✅ Loaded ${sniperListings.length} listings from database`);

    // Then start the watcher
    setTimeout(() => {
      sniperLog("[Sniper] 🔄 Starting blockchain watcher...");
      watchForListings();
    }, 2000);

    // Run cleanup once on startup
    setTimeout(cleanupOldListings, 30000); // After 30 seconds

    sniperLog("[Sniper] ✅ Sniper system initialized successfully");
  } catch (err) {
    sniperError("[Sniper] ❌ Error initializing sniper:", err.message);
    sniperError("[Sniper] Stack:", err.stack);
    // Still try to start the watcher
    setTimeout(() => {
      sniperLog("[Sniper] 🔄 Attempting to start watcher despite errors...");
      watchForListings();
    }, 5000);
  }
}

// API endpoint to get sniper listings with filtering
// Debug endpoint to find a listing by player name and serial
app.get("/api/sniper/find-listing", async (req, res) => {
  try {
    const { player, serial } = req.query;

    if (!player && !serial) {
      return res.status(400).json({
        ok: false,
        error: "Please provide player name (e.g., ?player=Tony Dorsett&serial=44)"
      });
    }

    // Search in database
    let query = `
      SELECT sl.nft_id, sl.listing_id, sl.listed_at, sl.is_sold, sl.is_unlisted, 
             sl.buyer_address, sl.listing_data, sl.updated_at,
             m.serial_number, m.first_name, m.last_name, m.team_name, m.tier
      FROM sniper_listings sl
      JOIN nft_core_metadata_v2 m ON m.nft_id = sl.nft_id
      WHERE 1=1
    `;
    const params = [];
    let paramCount = 1;

    if (player) {
      const playerLower = player.toLowerCase();
      query += ` AND (LOWER(m.first_name || ' ' || m.last_name) LIKE $${paramCount} OR LOWER(m.last_name) LIKE $${paramCount})`;
      params.push(`%${playerLower}%`);
      paramCount++;
    }

    if (serial) {
      query += ` AND m.serial_number = $${paramCount}`;
      params.push(parseInt(serial));
      paramCount++;
    }

    query += ` ORDER BY sl.listed_at DESC LIMIT 10`;

    const result = await pgQuery(query, params);

    if (result.rows.length === 0) {
      return res.json({
        ok: true,
        found: false,
        message: "No listings found matching criteria"
      });
    }

    // For each found listing, get verification details
    const listings = [];
    for (const row of result.rows) {
      const listingData = typeof row.listing_data === 'string'
        ? JSON.parse(row.listing_data)
        : row.listing_data;

      const listing = {
        nftId: row.nft_id,
        listingId: row.listing_id,
        serialNumber: row.serial_number,
        playerName: `${row.first_name} ${row.last_name}`,
        team: row.team_name,
        tier: row.tier,
        listedAt: row.listed_at,
        isSold: row.is_sold,
        isUnlisted: row.is_unlisted,
        buyerAddress: row.buyer_address,
        updatedAt: row.updated_at,
        sellerAddr: listingData?.sellerAddr || listingData?.seller_addr,
        listingPrice: listingData?.listingPrice
      };

      // If it's marked as sold, let's verify why
      if (row.is_sold && row.listing_id) {
        try {
          const status = await verifyListingStatusFast(
            row.nft_id,
            row.listing_id,
            row.listed_at,
            listing.sellerAddr
          );
          listing.verificationResult = status;
        } catch (e) {
          listing.verificationError = e.message;
        }
      }

      listings.push(listing);
    }

    return res.json({
      ok: true,
      found: true,
      count: listings.length,
      listings: listings
    });
  } catch (err) {
    console.error("[API] Error finding listing:", err.message);
    return res.status(500).json({
      ok: false,
      error: "Failed to find listing: " + (err.message || String(err))
    });
  }
});

// Manual verification endpoint for a specific listing
app.get("/api/sniper/verify-listing", async (req, res) => {
  try {
    const { nftId, listingId } = req.query;

    if (!nftId && !listingId) {
      return res.status(400).json({
        ok: false,
        error: "Please provide either nftId or listingId"
      });
    }

    // Find the listing
    let listing = null;
    if (listingId) {
      listing = sniperListings.find(l => l.listingId === listingId);
      if (!listing) {
        const dbResult = await pgQuery(
          `SELECT nft_id, listing_id, listed_at, seller_addr, listing_data 
           FROM sniper_listings 
           WHERE listing_id = $1 LIMIT 1`,
          [listingId]
        );
        if (dbResult.rows.length > 0) {
          const row = dbResult.rows[0];
          listing = {
            nftId: row.nft_id,
            listingId: row.listing_id,
            listedAt: row.listed_at,
            sellerAddr: row.seller_addr
          };
        }
      }
    } else if (nftId) {
      listing = sniperListings.find(l => l.nftId === nftId);
      if (!listing) {
        const dbResult = await pgQuery(
          `SELECT nft_id, listing_id, listed_at, seller_addr 
           FROM sniper_listings 
           WHERE nft_id = $1 
           ORDER BY listed_at DESC 
           LIMIT 1`,
          [nftId]
        );
        if (dbResult.rows.length > 0) {
          const row = dbResult.rows[0];
          listing = {
            nftId: row.nft_id,
            listingId: row.listing_id,
            listedAt: row.listed_at,
            sellerAddr: row.seller_addr
          };
        }
      }
    }

    if (!listing) {
      return res.status(404).json({
        ok: false,
        error: "Listing not found in our system"
      });
    }

    // Verify status
    const status = await verifyListingStatusFast(
      listing.nftId,
      listing.listingId,
      listing.listedAt,
      listing.sellerAddr
    );

    if (status && status.isSold) {
      // Mark as sold if verified
      await markListingAsSold(listing.nftId, status.buyerAddr || null);
    } else if (status && status.isUnlisted) {
      await markListingAsUnlisted(listing.nftId);
    }

    return res.json({
      ok: true,
      listing: {
        nftId: listing.nftId,
        listingId: listing.listingId,
        listedAt: listing.listedAt,
        sellerAddr: listing.sellerAddr
      },
      verification: status || { isSold: false, isUnlisted: false },
      message: status
        ? (status.isSold ? "Listing verified as SOLD" : status.isUnlisted ? "Listing verified as DELISTED" : "Listing is still ACTIVE")
        : "Could not verify listing status"
    });
  } catch (err) {
    console.error("[API] Error verifying listing:", err.message);
    return res.status(500).json({
      ok: false,
      error: "Failed to verify listing: " + (err.message || String(err))
    });
  }
});

// Reset all listings to unsold (useful after fixing verification logic)
app.post("/api/sniper/reset-all-unsold", async (req, res) => {
  try {
    const result = await resetAllListingsToUnsold();
    return res.json({
      ok: true,
      message: "All listings reset to unsold",
      databaseReset: result.databaseReset,
      memoryReset: result.memoryReset
    });
  } catch (err) {
    console.error("[API] Error resetting listings to unsold:", err.message);
    return res.status(500).json({
      ok: false,
      error: "Failed to reset listings: " + (err.message || String(err))
    });
  }
});

app.get("/api/sniper-deals", async (req, res) => {
  try {
    // Get filter params
    const { team, player, tier, minDiscount, maxPrice, maxSerial, dealsOnly } = req.query;

    const statusFilter = req.query.status || 'active';

    let filtered = sniperListings;

    if (statusFilter === 'active') {
      filtered = sniperListings.filter(l => !l.isSold && !l.isUnlisted);
    } else if (statusFilter === 'sold') {
      filtered = sniperListings.filter(l => l.isSold);
    } else if (statusFilter === 'unlisted') {
      filtered = sniperListings.filter(l => l.isUnlisted);
    } else if (statusFilter === 'sold-unlisted') {
      filtered = sniperListings.filter(l => l.isSold || l.isUnlisted);
    }
    // 'all' shows everything (no filter)

    // If we don't have many listings in memory, also check database (for fresh page loads)
    // Only query DB if we have very few listings (less than 10) to avoid slow queries on every request
    if (filtered.length < 10 && statusFilter === 'active') {
      try {
        await ensureSniperListingsTable();
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);

        let dbQuery = `SELECT listing_data, is_sold, is_unlisted, buyer_address
           FROM sniper_listings 
           WHERE listed_at >= $1`;
        const dbParams = [threeDaysAgo];

        // Apply status filter to database query if not showing all
        if (statusFilter === 'active') {
          dbQuery += ` AND is_sold = FALSE AND is_unlisted = FALSE`;
        } else if (statusFilter === 'sold') {
          dbQuery += ` AND is_sold = TRUE`;
        } else if (statusFilter === 'unlisted') {
          dbQuery += ` AND is_unlisted = TRUE AND is_sold = FALSE`;
        } else if (statusFilter === 'sold-unlisted') {
          dbQuery += ` AND (is_sold = TRUE OR is_unlisted = TRUE)`;
        }
        // 'all' - no additional WHERE clause

        dbQuery += ` ORDER BY listed_at DESC LIMIT 200`;

        const dbResult = await pgQuery(dbQuery, dbParams);

        // Merge DB results with memory (avoid duplicates)
        const memoryNftIds = new Set(filtered.map(l => l.nftId));
        for (const row of dbResult.rows) {
          try {
            const listing = typeof row.listing_data === 'string'
              ? JSON.parse(row.listing_data)
              : row.listing_data;

            if (!listing.nftId || memoryNftIds.has(listing.nftId)) continue;

            // Update status from database
            listing.isSold = row.is_sold || false;
            listing.isUnlisted = row.is_unlisted || false;
            if (row.buyer_address) {
              listing.buyerAddr = row.buyer_address;
            }

            // Apply status filter when adding from database
            if (statusFilter === 'active' && (!listing.isSold && !listing.isUnlisted)) {
              filtered.push(listing);
              memoryNftIds.add(listing.nftId);
            } else if (statusFilter === 'sold' && listing.isSold) {
              filtered.push(listing);
              memoryNftIds.add(listing.nftId);
            } else if (statusFilter === 'unlisted' && listing.isUnlisted && !listing.isSold) {
              filtered.push(listing);
              memoryNftIds.add(listing.nftId);
            } else if (statusFilter === 'sold-unlisted' && (listing.isSold || listing.isUnlisted)) {
              filtered.push(listing);
              memoryNftIds.add(listing.nftId);
            } else if (statusFilter === 'all') {
              filtered.push(listing);
              memoryNftIds.add(listing.nftId);
            }
          } catch (e) {
            // Skip malformed listings
          }
        }

        // Sort by listedAt (most recent first)
        filtered.sort((a, b) => {
          const aTime = new Date(a.listedAt || 0).getTime();
          const bTime = new Date(b.listedAt || 0).getTime();
          return bTime - aTime;
        });
      } catch (dbErr) {
        // If DB query fails, just use memory listings
        sniperError("[Sniper] Error fetching from database:", dbErr.message);
      }
    }

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
          `SELECT nft_id, series_name, jersey_number FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
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
        sniperError("[Sniper] Error fetching series names by nftId:", e.message);
      }
    }

    // Batch fetch series names by editionId (fallback for listings without nftId)
    if (allEditionIds.length > 0) {
      try {
        const metaResult = await pgQuery(
          `SELECT edition_id, series_name FROM nft_core_metadata_v2 WHERE edition_id = ANY($1::text[]) AND series_name IS NOT NULL`,
          [allEditionIds]
        );
        metaResult.rows.forEach(row => {
          if (row.series_name) {
            seriesMapByEditionId.set(row.edition_id, row.series_name);
          }
        });
      } catch (e) {
        sniperError("[Sniper] Error fetching series names by editionId:", e.message);
      }
    }

    // Batch fetch ASP and top sale for all listings
    const topSaleMap = new Map();
    if (allEditionIds.length > 0) {
      try {
        const priceResult = await pgQuery(
          `SELECT edition_id, avg_sale_usd, top_sale_usd FROM edition_price_scrape WHERE edition_id = ANY($1::text[])`,
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
          const topSaleValue = row.top_sale_usd;
          if (topSaleValue != null) {
            const numValue = Number(topSaleValue);
            if (!isNaN(numValue) && numValue > 0) {
              topSaleMap.set(row.edition_id, numValue);
            }
          }
        });
      } catch (e) {
        // Silently fail - price data is optional
      }
    }

    // Helper function to detect parallel variant from set name and mint size
    function detectParallelVariant(setName, maxMint) {
      if (!setName) return 'standard';
      const setLower = setName.toLowerCase();
      if (!setLower.includes('parallel')) return 'standard';

      // Detect by mint size
      if (maxMint === 25 || maxMint === '25') return 'sapphire';
      if (maxMint === 50 || maxMint === '50') return 'emerald';
      if (maxMint === 299 || maxMint === '299' || !maxMint) return 'ruby';
      return 'parallel'; // Unknown parallel variant
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
        // Add top sale price
        if (topSaleMap.has(enriched.editionId)) {
          enriched.topSale = topSaleMap.get(enriched.editionId);
        }
      }

      // Detect parallel variant from set name
      enriched.parallelVariant = detectParallelVariant(enriched.setName, enriched.maxMint);

      // Special serial detection
      const serial = enriched.serialNumber;
      const jersey = enriched.jerseyNumber;

      // Is #1 serial
      enriched.isNumberOne = serial === 1;

      // Is jersey match (serial equals player's jersey number)
      enriched.isJerseyMatch = jersey && serial && serial === jersey;

      // Low serial classifications
      enriched.isTop10 = serial && serial <= 10;
      enriched.isTop100 = serial && serial <= 100;

      // Calculate floor delta (% savings vs floor)
      const listPrice = enriched.listingPrice;
      const floor = enriched.floor || enriched.floorPrice;
      if (listPrice && floor && floor > 0) {
        enriched.floorDelta = ((floor - listPrice) / floor) * 100; // positive = savings
      }

      // Calculate ASP delta (% savings vs average sale price)
      if (listPrice && enriched.avgSale && enriched.avgSale > 0) {
        enriched.aspDelta = ((enriched.avgSale - listPrice) / enriched.avgSale) * 100; // positive = savings
      }

      return enriched;
    });

    // Debug: Log enrichment stats
    //if (enrichedListings.length > 0) {
    //  const withSeries = enrichedListings.filter(l => l.seriesName).length;
    //  const withASP = enrichedListings.filter(l => l.avgSale != null && l.avgSale > 0).length;
    //  const withEditionId = enrichedListings.filter(l => l.editionId).length;
    //  console.log(`[Sniper API] Enriched ${enrichedListings.length} listings: ${withSeries} with series, ${withASP} with ASP (${withEditionId} have editionId)`);
    //}

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
    sniperError("[Sniper] Error getting listings:", err);
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
        `SELECT DISTINCT edition_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
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
           WHERE nft_id = ANY($1::text[])`,
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
      sniperLog(`[Sniper] Scraping real-time prices for ${editionList.length} editions...`);

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

      sniperLog(`[Sniper] Got real-time prices for ${Object.keys(scrapedData).length} editions`);
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
                  team_name, position, tier, set_name
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
        // Direct moment link for faster buying (one click to buy page)
        listingUrl: listing.nftId ? `https://nflallday.com/moments/${listing.nftId}` : null
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

// ============================================================
// NFL ALL DAY GRAPHQL API (Playbook, Challenges, Offers)
// ============================================================

const NFLAD_GRAPHQL_URL = process.env.NFLAD_GRAPHQL_URL || "https://nflallday.com/consumer/graphql";

// Response cache for GraphQL requests
const graphqlCache = new Map(); // key -> { data, timestamp, expiresAt }
const CACHE_TTL = 2 * 60 * 1000; // Cache for 2 minutes
const MAX_GRAPHQL_CACHE_SIZE = 50; // Limit cache size to prevent memory issues

// Live challenges cache (lightweight, unauthenticated searchChallenges)
const CHALLENGES_LIVE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes
let challengesLiveCache = null;
let challengesLiveCacheTime = 0;

// Live kickoffs cache (searchKickoffSlates, unauthenticated)
const KICKOFFS_LIVE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes
let kickoffsLiveCache = null;
let kickoffsLiveCacheTime = 0;

// Rate limiting for GraphQL requests
let graphqlRequestCount = 0;
let graphqlRequestWindowStart = Date.now();
const GRAPHQL_RATE_LIMIT_WINDOW = 60000; // 1 minute
const GRAPHQL_MAX_REQUESTS_PER_WINDOW = 5; // Max 5 requests per minute (increased from 1)

// Helper function to create cache key from query and variables
function createCacheKey(query, variables) {
  return JSON.stringify({ query, variables });
}

// Extract query name from GraphQL query string (e.g., "SearchKickoffSlates" from "query searchKickoffSlates")
// Also handles mutations (e.g., "AcceptOffer" from "mutation AcceptOffer")
function extractQueryName(query) {
  const queryMatch = query.match(/query\s+(\w+)/);
  const mutationMatch = query.match(/mutation\s+(\w+)/);
  return queryMatch ? queryMatch[1] : (mutationMatch ? mutationMatch[1] : null);
}

async function nfladGraphQLQuery(query, variables = {}, userToken = null, retries = 3, useCache = true, req = null) {
  const cacheKey = createCacheKey(query, variables);
  const operationName = extractQueryName(query);

  // Check cache first
  if (useCache) {
    const cached = graphqlCache.get(cacheKey);
    if (cached && Date.now() < cached.expiresAt) {
      console.log("[GraphQL Cache] Returning cached data for query:", operationName || "unknown");
      return cached.data;
    }

    // Clean up expired cache entries
    if (cached && Date.now() >= cached.expiresAt) {
      graphqlCache.delete(cacheKey);
    }
  }

  // Rate limiting: track requests per minute
  const now = Date.now();
  if (now - graphqlRequestWindowStart > GRAPHQL_RATE_LIMIT_WINDOW) {
    // Reset window
    graphqlRequestCount = 0;
    graphqlRequestWindowStart = now;
  }

  // Only enforce rate limit if we're at the limit (don't block first requests)
  if (graphqlRequestCount >= GRAPHQL_MAX_REQUESTS_PER_WINDOW) {
    const waitTime = GRAPHQL_RATE_LIMIT_WINDOW - (now - graphqlRequestWindowStart);
    if (waitTime > 0) {
      console.warn(`[GraphQL] Rate limit reached (${graphqlRequestCount}/${GRAPHQL_MAX_REQUESTS_PER_WINDOW}). Waiting ${Math.ceil(waitTime / 1000)}s before next request...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
      graphqlRequestCount = 0;
      graphqlRequestWindowStart = Date.now();
    }
  }

  graphqlRequestCount++;

  // Add a small delay before making request (helps avoid bot detection)
  await new Promise(resolve => setTimeout(resolve, 300)); // 300ms delay between requests

  try {
    const headers = {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Origin": "https://nflallday.com",
      "Referer": "https://nflallday.com/playbook",
      "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
      "sec-ch-ua-mobile": "?0",
      "sec-ch-ua-platform": '"Windows"',
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "same-origin"
    };

    // Add authentication token if provided
    if (userToken) {
      headers["X-Id-Token"] = userToken;
    }

    // Add cookies if available from request (for browser session)
    // Note: This is a public API, but some endpoints may require cookies for bot detection
    // Forward cookies from the client if available
    let cookieString = '';
    if (req && req.headers && req.headers.cookie) {
      cookieString = req.headers.cookie;
    }

    // Also add nfl_session cookies if we have them stored (from token input)
    // The user might have pasted the session cookie, so try to use it
    if (userToken && userToken.length > 1000) {
      // This looks like a session cookie, add it to cookies instead of X-Id-Token
      if (cookieString) {
        cookieString += `; nfl_session.0=${userToken}`;
      } else {
        cookieString = `nfl_session.0=${userToken}`;
      }
      // Don't use session cookie as X-Id-Token
      delete headers["X-Id-Token"];
    }

    if (cookieString) {
      headers["Cookie"] = cookieString;
      console.log(`[GraphQL] Forwarding cookies from browser request (${cookieString.length} chars)`);
    }

    // Extract query name and add as URL parameter (like browser does)
    const queryName = extractQueryName(query);
    const url = queryName ? `${NFLAD_GRAPHQL_URL}?${queryName}` : NFLAD_GRAPHQL_URL;

    // Log for debugging
    console.log(`[GraphQL] Making request #${graphqlRequestCount} to: ${url}`);
    console.log(`[GraphQL] Query name: ${queryName || 'unknown'}`);
    console.log(`[GraphQL] Query: ${query.substring(0, 150)}...`);
    console.log(`[GraphQL] Variables:`, JSON.stringify(variables).substring(0, 200));
    console.log(`[GraphQL] Has token: ${!!userToken} (${userToken ? userToken.length : 0} chars)`);
    console.log(`[GraphQL] Has cookies: ${!!cookieString} (${cookieString ? cookieString.length : 0} chars)`);

    // Build request body with operationName (required by NFL All Day API)
    const operationName = extractQueryName(query);
    const requestBody = {
      query,
      variables
    };

    // Add operationName if we could extract it
    if (operationName) {
      requestBody.operationName = operationName;
    }

    const response = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(requestBody)
    });

    const responseText = await response.text();
    let data;

    try {
      data = JSON.parse(responseText);
    } catch (parseErr) {
      console.error("[GraphQL] Response was not JSON:", responseText.substring(0, 500));
      throw new Error(`Invalid JSON response: ${response.status} ${response.statusText}`);
    }

    if (!response.ok) {
      // Log full response for debugging
      console.error(`[GraphQL] HTTP ${response.status} ${response.statusText}`);
      console.error(`[GraphQL] Response body:`, responseText.substring(0, 500));

      // Handle rate limiting and retry
      if (response.status === 403 || response.status === 429) {
        if (retries > 0) {
          const backoffDelay = (4 - retries) * 2000; // 2s, 4s, 6s
          console.warn(`[GraphQL] Rate limited (${response.status}). Retrying in ${backoffDelay}ms... (${retries} retries left)`);
          await new Promise(resolve => setTimeout(resolve, backoffDelay));
          return nfladGraphQLQuery(query, variables, userToken, retries - 1, useCache, req);
        }
      }

      throw new Error(`GraphQL request failed: ${response.status} ${response.statusText}. Response: ${responseText.substring(0, 200)}`);
    }

    if (data.errors) {
      console.error("GraphQL errors:", data.errors);
      throw new Error(data.errors[0]?.message || "GraphQL query failed");
    }

    // Cache the successful response
    if (useCache && data.data) {
      // Cleanup if cache is too large
      if (graphqlCache.size >= MAX_GRAPHQL_CACHE_SIZE) {
        const entries = Array.from(graphqlCache.entries())
          .sort((a, b) => a[1].expiresAt - b[1].expiresAt);
        // Remove oldest 20%
        const toRemove = Math.ceil(graphqlCache.size * 0.2);
        for (let i = 0; i < toRemove; i++) {
          graphqlCache.delete(entries[i][0]);
        }
      }

      graphqlCache.set(cacheKey, {
        data: data.data,
        timestamp: Date.now(),
        expiresAt: Date.now() + CACHE_TTL
      });
      console.log(`[GraphQL] Cached response (cache size: ${graphqlCache.size})`);
    }

    return data.data;
  } catch (err) {
    // Retry on network errors
    if (retries > 0 && (err.message.includes('fetch') || err.message.includes('network'))) {
      const backoffDelay = (4 - retries) * 1000; // 1s, 2s, 3s
      console.warn(`[GraphQL] Network error. Retrying in ${backoffDelay}ms... (${retries} retries left)`);
      await new Promise(resolve => setTimeout(resolve, backoffDelay));
      return nfladGraphQLQuery(query, variables, userToken, retries - 1, useCache, req);
    }
    console.error("NFL All Day GraphQL error:", err.message);
    throw err;
  }
}

// Get user's authentication token from request headers
function getUserToken(req) {
  return req.headers["x-id-token"] || null;
}

// Helper to check if response is cached
function isResponseCached(query, variables) {
  const cacheKey = createCacheKey(query, variables);
  const cached = graphqlCache.get(cacheKey);
  return cached && Date.now() < cached.expiresAt;
}

// ============================================================
// PLAYBOOK/KICKOFF API ENDPOINTS
// ============================================================

// Cached data files (populated by admin script: node scripts/refresh-kickoffs.js)
const CACHED_KICKOFFS_FILE = path.join(__dirname, 'data', 'kickoffs.json');
const CACHED_CHALLENGES_FILE = path.join(__dirname, 'data', 'challenges.json');
// Allow a root-level challenges.json fallback if data/ file is missing
const ALT_CACHED_CHALLENGES_FILE = path.join(__dirname, 'challenges.json');
const RARITY_LEADERBOARD_SNAPSHOT_FILE = path.join(__dirname, 'data', 'rarity-leaderboard.json');

// Fetch live challenges via public consumer GraphQL endpoint (no auth)
async function fetchLiveChallenges() {
  const now = Date.now();
  if (challengesLiveCache && (now - challengesLiveCacheTime) < CHALLENGES_LIVE_CACHE_TTL) {
    return challengesLiveCache;
  }

  const body = {
    operationName: "SearchChallenges",
    variables: {
      input: {
        after: "",
        first: 50,
        filters: {}
      }
    },
    query: `query SearchChallenges($input: SearchChallengesInput!) {
      searchChallenges(input: $input) {
        edges {
          node {
            id
            slug
            title
            subtitle
            description
            category
            status
            startsAt
            endsAt
            completedAt
            rewardsDescription
            submissions { totalCount }
            requirements {
              description
              count
              filters {
                byTiers
                byPlayerPositions
                bySetIDs
                byTeamIDs
                byPlayerIDs
              }
            }
          }
          cursor
        }
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }`
  };

  const res = await fetch("https://nflallday.com/consumer/graphql?searchChallenges", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json"
    },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    throw new Error(`searchChallenges HTTP ${res.status}`);
  }

  const json = await res.json();
  const edges = json?.data?.searchChallenges?.edges || [];
  const challenges = edges.map(e => e?.node).filter(Boolean);

  const payload = {
    ok: true,
    lastUpdated: new Date().toISOString(),
    totalChallenges: challenges.length,
    challenges
  };

  challengesLiveCache = payload;
  challengesLiveCacheTime = now;
  return payload;
}

// Fetch live kickoffs via public consumer GraphQL endpoint (no auth)
async function fetchLiveKickoffs() {
  const now = Date.now();
  if (kickoffsLiveCache && (now - kickoffsLiveCacheTime) < KICKOFFS_LIVE_CACHE_TTL) {
    return kickoffsLiveCache;
  }

  const body = {
    operationName: "SearchKickoffSlates",
    variables: {
      input: {
        after: "",
        first: 100,
        filters: {},
        sortBy: "START_DATE_DESC"
      }
    },
    query: `query SearchKickoffSlates($input: SearchKickoffSlatesInput!) {
      searchKickoffSlates(input: $input) {
        edges {
          node {
            id
            name
            startDate
            endDate
            status
            kickoffs {
              id
              name
              slateID
              difficulty
              status
              submissionDeadline
              gamesStartAt
              completedAt
              numParticipants
              slots {
                id
                slotOrder
                stats {
                  id
                  stat
                  valueNeeded
                  valueType
                  groupV2
                }
                requirements {
                  playerPositions
                  tiers
                  badgeSlugs
                  setIDs
                  teamIDs
                }
              }
            }
          }
          cursor
        }
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }`
  };

  const res = await fetch("https://nflallday.com/consumer/graphql?searchKickoffSlates", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json"
    },
    body: JSON.stringify(body)
  });

  if (!res.ok) {
    throw new Error(`searchKickoffSlates HTTP ${res.status}`);
  }

  const json = await res.json();
  const slates = (json?.data?.searchKickoffSlates?.edges || []).map(e => e?.node).filter(Boolean);

  // Flatten kickoffs with slate metadata
  const allKickoffs = [];
  for (const slate of slates) {
    if (slate?.kickoffs?.length) {
      for (const k of slate.kickoffs) {
        allKickoffs.push({
          ...k,
          slateName: slate.name,
          slateStatus: slate.status,
          slateStartDate: slate.startDate,
          slateEndDate: slate.endDate
        });
      }
    }
  }

  const payload = {
    ok: true,
    lastUpdated: new Date().toISOString(),
    totalSlates: slates.length,
    slates,
    totalKickoffs: allKickoffs.length,
    kickoffs: allKickoffs
  };

  kickoffsLiveCache = payload;
  kickoffsLiveCacheTime = now;
  return payload;
}

// Get cached kickoffs data
app.get("/api/kickoffs/cached", (req, res) => {
  try {
    if (fs.existsSync(CACHED_KICKOFFS_FILE)) {
      const raw = JSON.parse(fs.readFileSync(CACHED_KICKOFFS_FILE, 'utf8'));
      // Normalize: accept browser dump (data.searchKickoffSlates.edges) or server shape
      let slates = raw.slates;
      if (!slates && raw.data?.searchKickoffSlates?.edges) {
        slates = raw.data.searchKickoffSlates.edges.map(e => e?.node).filter(Boolean);
      }

      // Flatten kickoffs even if file only has slates
      let kickoffs = raw.kickoffs;
      if ((!kickoffs || !kickoffs.length) && Array.isArray(slates)) {
        kickoffs = [];
        for (const slate of slates) {
          if (slate?.kickoffs?.length) {
            for (const k of slate.kickoffs) {
              kickoffs.push({
                ...k,
                slateName: slate.name,
                slateStatus: slate.status,
                slateStartDate: slate.startDate,
                slateEndDate: slate.endDate
              });
            }
          }
        }
      }
      const payload = {
        ok: true,
        lastUpdated: raw.lastUpdated || new Date().toISOString(),
        totalSlates: raw.totalSlates || (slates ? slates.length : undefined),
        totalKickoffs: kickoffs ? kickoffs.length : raw.totalKickoffs,
        slates,
        kickoffs
      };
      res.json(payload);
    } else {
      res.json({ ok: false, error: "No cached data available. Run: node scripts/refresh-kickoffs.js" });
    }
  } catch (err) {
    console.error("Error reading cached kickoffs:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get cached challenges data
app.get("/api/challenges/cached", (req, res) => {
  try {
    const challengesPath = fs.existsSync(CACHED_CHALLENGES_FILE)
      ? CACHED_CHALLENGES_FILE
      : (fs.existsSync(ALT_CACHED_CHALLENGES_FILE) ? ALT_CACHED_CHALLENGES_FILE : null);

    if (challengesPath) {
      const raw = JSON.parse(fs.readFileSync(challengesPath, 'utf8'));
      // Normalize: if browser dump (raw.data.searchChallenges.edges) convert to challenges array
      let challenges = raw.challenges;
      let lastUpdated = raw.lastUpdated;
      if (!challenges && raw.data?.searchChallenges?.edges) {
        challenges = raw.data.searchChallenges.edges.map(e => e?.node).filter(Boolean);
        lastUpdated = lastUpdated || new Date().toISOString();
      }
      // Map fields to frontend expectations
      if (Array.isArray(challenges)) {
        challenges = challenges.map(c => ({
          id: c.id,
          title: c.title || c.name || "Challenge",
          name: c.name,
          description: c.description,
          category: c.category,
          status: c.status || "ACTIVE",
          startsAt: c.startsAt || c.startDate,
          endsAt: c.endsAt || c.endDate,
          slug: c.slug || c.id,
          subtitle: c.subtitle,
          rewardsDescription: c.rewardsDescription || c.description,
          submissions: c.submissions,
          requirements: c.requirements
        }));
      }
      const payload = {
        ok: true,
        lastUpdated: lastUpdated || new Date().toISOString(),
        totalChallenges: raw.totalChallenges || (challenges ? challenges.length : undefined),
        challenges
      };
      res.json(payload);
    } else {
      res.json({ ok: false, error: "No cached data available. Run: node scripts/refresh-kickoffs.js" });
    }
  } catch (err) {
    console.error("Error reading cached challenges:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Live challenges endpoint; falls back to cached file on failure
app.get("/api/challenges/live", async (_req, res) => {
  try {
    const data = await fetchLiveChallenges();
    return res.json(data);
  } catch (err) {
    console.error("Error fetching live challenges:", err);
    const challengesPath = fs.existsSync(CACHED_CHALLENGES_FILE)
      ? CACHED_CHALLENGES_FILE
      : (fs.existsSync(ALT_CACHED_CHALLENGES_FILE) ? ALT_CACHED_CHALLENGES_FILE : null);
    if (challengesPath) {
      try {
        const raw = JSON.parse(fs.readFileSync(challengesPath, "utf8"));
        let challenges = raw.challenges;
        let lastUpdated = raw.lastUpdated;
        if (!challenges && raw.data?.searchChallenges?.edges) {
          challenges = raw.data.searchChallenges.edges.map(e => e?.node).filter(Boolean);
          lastUpdated = lastUpdated || new Date().toISOString();
        }
        if (Array.isArray(challenges)) {
          challenges = challenges.map(c => ({
            id: c.id,
            title: c.title || c.name || "Challenge",
            name: c.name,
            description: c.description,
            category: c.category,
            status: c.status || "ACTIVE",
            startsAt: c.startsAt || c.startDate,
            endsAt: c.endsAt || c.endDate,
            slug: c.slug || c.id,
            subtitle: c.subtitle,
            rewardsDescription: c.rewardsDescription || c.description,
            submissions: c.submissions,
            requirements: c.requirements
          }));
        }
        const payload = {
          ok: true,
          lastUpdated: lastUpdated || new Date().toISOString(),
          totalChallenges: raw.totalChallenges || (challenges ? challenges.length : undefined),
          challenges,
          fallback: true,
          error: err.message
        };
        return res.json(payload);
      } catch (e) {
        console.error("Error reading cached challenges after live failure:", e);
      }
    }
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Live challenges endpoint; falls back to cached file on failure
app.get("/api/challenges/live", async (_req, res) => {
  try {
    const data = await fetchLiveChallenges();
    return res.json(data);
  } catch (err) {
    console.error("Error fetching live challenges:", err);
    if (fs.existsSync(CACHED_CHALLENGES_FILE)) {
      try {
        const data = JSON.parse(fs.readFileSync(CACHED_CHALLENGES_FILE, "utf8"));
        return res.json({ ok: true, ...data, fallback: true, error: err.message });
      } catch (e) {
        console.error("Error reading cached challenges after live failure:", e);
      }
    }
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Live kickoffs endpoint; falls back to cached file on failure
app.get("/api/kickoffs/live", async (_req, res) => {
  try {
    const data = await fetchLiveKickoffs();
    return res.json(data);
  } catch (err) {
    console.error("Error fetching live kickoffs:", err);
    if (fs.existsSync(CACHED_KICKOFFS_FILE)) {
      try {
        const raw = JSON.parse(fs.readFileSync(CACHED_KICKOFFS_FILE, "utf8"));
        let slates = raw.slates;
        if (!slates && raw.data?.searchKickoffSlates?.edges) {
          slates = raw.data.searchKickoffSlates.edges.map(e => e?.node).filter(Boolean);
        }
        let kickoffs = raw.kickoffs;
        if ((!kickoffs || !kickoffs.length) && Array.isArray(slates)) {
          kickoffs = [];
          for (const slate of slates) {
            if (slate?.kickoffs?.length) {
              for (const k of slate.kickoffs) {
                kickoffs.push({
                  ...k,
                  slateName: slate.name,
                  slateStatus: slate.status,
                  slateStartDate: slate.startDate,
                  slateEndDate: slate.endDate
                });
              }
            }
          }
        }
        const payload = {
          ok: true,
          lastUpdated: raw.lastUpdated || new Date().toISOString(),
          totalSlates: raw.totalSlates || (slates ? slates.length : undefined),
          totalKickoffs: kickoffs ? kickoffs.length : raw.totalKickoffs,
          slates,
          kickoffs,
          fallback: true,
          error: err.message
        };
        return res.json(payload);
      } catch (e) {
        console.error("Error reading cached kickoffs after live failure:", e);
      }
    }
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Scrape kickoff slates from HTML (fallback when GraphQL is blocked)
async function scrapeKickoffSlatesFromHTML() {
  try {
    // Try multiple possible URLs for kickoff page
    const urls = [
      'https://nflallday.com/kickoffs',
      'https://www.nflallday.com/kickoffs',
      'https://nflallday.com/kickoffs/',
      'https://www.nflallday.com/kickoffs/'
    ];

    let response = null;
    let lastError = null;

    for (const url of urls) {
      try {
        response = await fetch(url, {
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://nflallday.com/",
            "Accept-Encoding": "gzip, deflate, br"
          },
          redirect: 'follow'
        });

        if (response.ok) {
          console.log(`[Scrape] Successfully fetched ${url}`);
          break;
        } else {
          lastError = new Error(`HTTP ${response.status}: ${response.statusText} for ${url}`);
        }
      } catch (err) {
        lastError = err;
        continue;
      }
    }

    if (!response || !response.ok) {
      throw lastError || new Error(`Failed to fetch kickoffs page. All URLs returned errors.`);
    }

    const html = await response.text();

    // Try to find embedded JSON data in script tags (Next.js pattern)
    const nextDataMatch = html.match(/<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
    if (nextDataMatch) {
      try {
        const nextData = JSON.parse(nextDataMatch[1]);
        console.log('[Scrape] Found Next.js embedded data');

        // Navigate through Next.js data structure to find kickoff data
        // The structure is typically in props.pageProps or props.initialState
        const pageProps = nextData.props?.pageProps;
        const initialState = nextData.props?.initialState;

        // Try to find GraphQL query result data
        // Look for searchKickoffSlates in various locations
        let slatesData = null;

        // Check common locations for GraphQL data in Next.js apps
        if (pageProps?.data?.searchKickoffSlates) {
          slatesData = pageProps.data.searchKickoffSlates;
        } else if (pageProps?.searchKickoffSlates) {
          slatesData = pageProps.searchKickoffSlates;
        } else if (pageProps?.dehydratedState?.queries) {
          // React Query / TanStack Query dehydrated state
          for (const query of pageProps.dehydratedState.queries) {
            if (query.queryKey?.[0] === 'SearchKickoffSlates' && query.state?.data) {
              slatesData = query.state.data.searchKickoffSlates || query.state.data;
              console.log('[Scrape] Found data in dehydratedState queries');
              break;
            }
          }
        } else if (initialState?.apollo?.ROOT_QUERY) {
          // Apollo Client cache structure
          const apolloCache = initialState.apollo.ROOT_QUERY;
          const slateKeys = Object.keys(apolloCache).filter(k => k.includes('searchKickoffSlates'));
          if (slateKeys.length > 0) {
            // Extract from Apollo cache (more complex parsing needed)
            console.log('[Scrape] Found Apollo cache, but needs custom extraction');
          }
        }

        // Also try to find data in window.__APOLLO_STATE__ or similar
        const apolloStateMatch = html.match(/window\.__APOLLO_STATE__\s*=\s*({.+?});/s);
        if (apolloStateMatch && !slatesData) {
          try {
            const apolloState = JSON.parse(apolloStateMatch[1]);
            console.log('[Scrape] Found window.__APOLLO_STATE__');
            // Apollo state is keyed by query hash, need to search for searchKickoffSlates
            const allKeys = Object.keys(apolloState);
            const slateQueryKeys = allKeys.filter(k => k.includes('searchKickoffSlates') || k.includes('SearchKickoffSlates'));
            if (slateQueryKeys.length > 0) {
              // Try to extract edges/nodes from Apollo cache
              for (const key of slateQueryKeys) {
                const queryData = apolloState[key];
                if (queryData?.edges) {
                  slatesData = queryData;
                  break;
                }
              }
            }
          } catch (e) {
            console.log('[Scrape] Could not parse Apollo state:', e.message);
          }
        }

        if (slatesData && slatesData.edges) {
          console.log(`[Scrape] Successfully extracted ${slatesData.edges.length} slates from HTML`);
          return {
            ok: true,
            ...slatesData,
            scraped: true
          };
        } else {
          console.log('[Scrape] Found Next.js data but no kickoff slates structure');
          // Log structure for debugging
          console.log('[Scrape] Available keys:', Object.keys(nextData.props || {}));
        }
      } catch (e) {
        console.log('[Scrape] Could not parse Next.js data:', e.message);
      }
    } else {
      console.log('[Scrape] No __NEXT_DATA__ script tag found');
    }

    // Try regex pattern matching for slate data in HTML (less reliable but fallback)
    // Look for JSON-like structures in script tags
    const scriptMatches = html.match(/<script[^>]*>([\s\S]*?)<\/script>/gi);
    if (scriptMatches) {
      for (const script of scriptMatches) {
        // Look for patterns like "searchKickoffSlates" or "edges"
        if (script.includes('searchKickoffSlates') || script.includes('kickoff')) {
          // Try to extract JSON
          const jsonMatch = script.match(/\{[\s\S]*"searchKickoffSlates"[\s\S]*\}/);
          if (jsonMatch) {
            try {
              const data = JSON.parse(jsonMatch[0]);
              if (data.searchKickoffSlates) {
                console.log('[Scrape] Found slates in script tag');
                return {
                  ok: true,
                  ...data.searchKickoffSlates,
                  scraped: true
                };
              }
            } catch (e) {
              // Not valid JSON, continue
            }
          }
        }
      }
    }

    return { ok: false, error: 'Could not extract kickoff slate data from HTML. The page structure may have changed.' };
  } catch (err) {
    console.error('[Scrape] Error scraping kickoff page:', err.message);
    throw err;
  }
}

// Get all kickoff slates
app.get("/api/kickoff/slates", async (req, res) => {
  try {
    const { after = "", first = 50, statuses, useClientProxy = "false", useBrowserHeaders = "false", useScraping = "false" } = req.query;

    // If client proxy is requested, return instructions for browser-side fetch
    if (useClientProxy === "true") {
      return res.json({
        ok: true,
        useClientProxy: true,
        graphqlUrl: NFLAD_GRAPHQL_URL,
        query: `
          query searchKickoffSlates($input: SearchKickoffSlatesInput!) {
            searchKickoffSlates(input: $input) {
              edges {
                node {
                  id
                  name
                  startDate
                  endDate
                  status
                  kickoffs {
                    id
                    name
                    slateID
                    difficulty
                    status
                    submissionDeadline
                    gamesStartAt
                    completedAt
                    numParticipants
                  }
                }
                cursor
              }
              pageInfo {
                endCursor
                hasNextPage
              }
              totalCount
            }
          }
        `,
        variables: {
          input: {
            after,
            first: parseInt(first, 10),
            ...(statuses && { filters: { byStatuses: statuses.split(",") } })
          }
        }
      });
    }

    const query = `
      query searchKickoffSlates($input: SearchKickoffSlatesInput!) {
        searchKickoffSlates(input: $input) {
          edges {
            node {
              id
              name
              startDate
              endDate
              status
              kickoffs {
                id
                name
                slateID
                difficulty
                status
                submissionDeadline
                gamesStartAt
                completedAt
                numParticipants
              }
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        after,
        first: parseInt(first, 10),
        ...(statuses && { filters: { byStatuses: statuses.split(",") } })
      }
    };

    try {
      // Try GraphQL SearchKickoffSlates first
      const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true, req);
      if (data && data.searchKickoffSlates) {
        return res.json({ ok: true, ...data.searchKickoffSlates, cached: isResponseCached(query, variables) });
      } else {
        throw new Error('GraphQL returned invalid data structure');
      }
    } catch (graphqlErr) {
      console.error("[API] GraphQL error for kickoff slates:", graphqlErr.message);

      // FALLBACK: Try SearchKickoffs directly (newer endpoint that NFLAD is pushing)
      try {
        console.log("[API] Trying SearchKickoffs fallback...");
        const kickoffsQuery = `
          query searchKickoffs($input: SearchKickoffsInput!) {
            searchKickoffs(input: $input) {
              edges {
                node {
                  id
                  name
                  slateID
                  difficulty
                  status
                  submissionDeadline
                  gamesStartAt
                  completedAt
                  numParticipants
                }
                cursor
              }
              pageInfo {
                endCursor
                hasNextPage
              }
              totalCount
            }
          }
        `;

        const kickoffsVars = {
          input: {
            first: parseInt(first, 10),
            filters: {
              byStatuses: ["STARTED", "NOT_STARTED", "GAMES_IN_PROGRESS"]
            }
          }
        };

        const kickoffsData = await nfladGraphQLQuery(kickoffsQuery, kickoffsVars, getUserToken(req), 2, true, req);

        if (kickoffsData && kickoffsData.searchKickoffs && kickoffsData.searchKickoffs.edges) {
          console.log(`[API] SearchKickoffs fallback success! Got ${kickoffsData.searchKickoffs.edges.length} kickoffs`);

          // Transform to match expected format (wrap in a fake slate)
          const transformedData = {
            edges: [{
              node: {
                id: "current-week",
                name: "Current Week Kickoffs",
                startDate: new Date().toISOString(),
                endDate: null,
                status: "ACTIVE",
                kickoffs: kickoffsData.searchKickoffs.edges.map(e => e.node)
              }
            }],
            totalCount: kickoffsData.searchKickoffs.totalCount
          };

          return res.json({
            ok: true,
            ...transformedData,
            source: "SearchKickoffs",
            cached: false
          });
        }
      } catch (fallbackErr) {
        console.error("[API] SearchKickoffs fallback also failed:", fallbackErr.message);
      }

      // Check if we have cached data we can return even on error
      const cacheKey = createCacheKey(query, variables);
      const cached = graphqlCache.get(cacheKey);

      if (cached && Date.now() < cached.expiresAt) {
        console.log("[API] Returning cached data despite error");
        return res.json({
          ok: true,
          ...cached.data.searchKickoffSlates,
          cached: true,
          error: "Using cached data due to API error",
          errorDetails: graphqlErr.message
        });
      }

      // Determine error status code based on error message
      let statusCode = 503;
      let errorMsg = "Unable to connect to NFL All Day API";

      if (graphqlErr.message.includes("403") || graphqlErr.message.includes("Forbidden")) {
        statusCode = 403;
        errorMsg = "Access denied by NFL All Day API. This may be due to rate limiting, authentication requirements, or bot detection.";
      } else if (graphqlErr.message.includes("429") || graphqlErr.message.includes("rate limit")) {
        statusCode = 429;
        errorMsg = "Rate limited by NFL All Day API. Please try again later.";
      }

      // If GraphQL fails with 403, automatically try HTML scraping as fallback
      if (statusCode === 403) {
        console.log("[API] GraphQL blocked (403), automatically attempting HTML scraping fallback...");
        try {
          const scrapedData = await scrapeKickoffSlatesFromHTML();
          if (scrapedData.ok) {
            console.log("[API] HTML scraping succeeded! Returning scraped data.");
            return res.json(scrapedData);
          } else {
            console.log("[API] HTML scraping found no data:", scrapedData.error);
          }
        } catch (scrapeErr) {
          console.error("[API] HTML scraping failed:", scrapeErr.message);
        }
      } else if (useScraping === "true") {
        // Also try scraping if explicitly requested
        console.log("[API] HTML scraping explicitly requested...");
        try {
          const scrapedData = await scrapeKickoffSlatesFromHTML();
          if (scrapedData.ok) {
            return res.json(scrapedData);
          }
        } catch (scrapeErr) {
          console.error("[API] HTML scraping failed:", scrapeErr.message);
        }
      }

      // Return suggestion to use scraping or client proxy on 403
      res.status(statusCode).json({
        ok: false,
        error: errorMsg,
        details: graphqlErr.message,
        useClientProxy: true,
        useScraping: true,
        hint: statusCode === 403 ? "The API is blocking requests. Try adding ?useScraping=true to use HTML scraping, or access nflallday.com in your browser first to establish a session." : "Set NFLAD_GRAPHQL_URL environment variable or check server logs for details"
      });
    }
  } catch (err) {
    console.error("[API] Error fetching kickoff slates:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get all active kickoffs directly (newer NFLAD endpoint)
app.get("/api/kickoffs", async (req, res) => {
  try {
    const { first = 20, statuses } = req.query;

    // Default to active statuses
    const statusFilter = statuses
      ? statuses.split(",")
      : ["STARTED", "NOT_STARTED", "GAMES_IN_PROGRESS"];

    const query = `
      query searchKickoffs($input: SearchKickoffsInput!) {
        searchKickoffs(input: $input) {
          edges {
            node {
              id
              name
              slateID
              difficulty
              status
              submissionDeadline
              gamesStartAt
              completedAt
              numParticipants
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        first: parseInt(first, 10),
        filters: {
          byStatuses: statusFilter
        }
      }
    };

    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true, req);

    if (data && data.searchKickoffs) {
      const kickoffs = data.searchKickoffs.edges.map(e => e.node);
      console.log(`[API] SearchKickoffs returned ${kickoffs.length} kickoffs`);

      return res.json({
        ok: true,
        kickoffs,
        totalCount: data.searchKickoffs.totalCount,
        cached: isResponseCached(query, variables)
      });
    }

    throw new Error('No data returned from SearchKickoffs');
  } catch (err) {
    console.error("[API] Error fetching kickoffs:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get specific kickoff details
app.get("/api/kickoff/:kickoffId", async (req, res) => {
  try {
    const { kickoffId } = req.params;

    const query = `
      query searchKickoffs($input: SearchKickoffsInput!) {
        searchKickoffs(input: $input) {
          edges {
            node {
              id
              name
              slateID
              difficulty
              slots {
                id
                slotOrder
                stats {
                  id
                  stat
                  valueNeeded
                  valueType
                  groupV2
                }
                requirements {
                  editionFlowIDs
                  playerIDs
                  playTypes
                  setIDs
                  teamIDs
                  tiers
                  series
                  seriesFlowIDs
                  playerPositions
                  badgeSlugs
                  combinedBadgeSlugs
                }
              }
              submissionDeadline
              status
              gamesStartAt
              completedAt
              createdAt
              updatedAt
              numParticipants
            }
            cursor
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        filters: {
          byIDs: [kickoffId]
        }
      }
    };

    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true);
    const kickoff = data.searchKickoffs.edges[0]?.node;

    if (!kickoff) {
      return res.status(404).json({ ok: false, error: "Kickoff not found" });
    }

    res.json({ ok: true, kickoff });
  } catch (err) {
    console.error("[API] Error fetching kickoff:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get eligible players for a kickoff
app.get("/api/kickoff/:kickoffId/eligible-players", async (req, res) => {
  try {
    const { kickoffId } = req.params;

    const query = `
      query GetKickoffEligiblePlayers($kickoffID: ID!) {
        getKickoffEligiblePlayers(kickoffID: $kickoffID) {
          id
          fullName
          position
          teamID
          teamName
        }
      }
    `;

    const variables = { kickoffID: kickoffId };
    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true);

    res.json({ ok: true, players: data.getKickoffEligiblePlayers || [] });
  } catch (err) {
    console.error("[API] Error fetching eligible players:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get kickoff submissions (user's picks) - requires auth
app.get("/api/kickoff/:kickoffId/submissions", async (req, res) => {
  try {
    const { kickoffId } = req.params;
    const { after = "", first = 50 } = req.query;
    const userToken = getUserToken(req);

    if (!userToken) {
      return res.status(401).json({ ok: false, error: "Authentication required" });
    }

    const query = `
      query searchKickoffSubmissions($input: SearchKickoffSubmissionsInput!) {
        searchKickoffSubmissions(input: $input) {
          edges {
            node {
              id
              kickoffID
              slotID
              momentNFT {
                id
                serialNumber
                flowID
                edition {
                  id
                  flowID
                  tier
                  play {
                    player {
                      fullName
                      position
                    }
                  }
                }
              }
              createdAt
              updatedAt
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        after,
        first: parseInt(first, 10),
        filters: {
          byKickoffID: kickoffId
        }
      }
    };

    const data = await nfladGraphQLQuery(query, variables, userToken, 3, true, req);
    res.json({ ok: true, ...data.searchKickoffSubmissions });
  } catch (err) {
    console.error("[API] Error fetching submissions:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get kickoff games (live game scores)
app.get("/api/kickoff/:kickoffId/games", async (req, res) => {
  try {
    const { kickoffId } = req.params;
    const { after = "", first = 50 } = req.query;

    const query = `
      query searchKickoffGames($input: SearchKickoffGamesInput!) {
        searchKickoffGames(input: $input) {
          edges {
            node {
              fixtureID
              status
              homeTeamID
              awayTeamID
              homeTeamScore
              awayTeamScore
              clock
              quarter
              scheduledAt
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        after,
        first: parseInt(first, 10),
        filters: {
          byKickoffID: kickoffId
        }
      }
    };

    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true);
    res.json({ ok: true, ...data.searchKickoffGames });
  } catch (err) {
    console.error("[API] Error fetching kickoff games:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get player's lowest listing prices for a kickoff
app.get("/api/kickoff/:kickoffId/player-prices", async (req, res) => {
  try {
    const { kickoffId } = req.params;
    const { playerIDs } = req.query; // Comma-separated list

    if (!playerIDs) {
      return res.status(400).json({ ok: false, error: "playerIDs query parameter required" });
    }

    const query = `
      query GetPlayersLowestListingPrice($playerIDs: [ID!]!) {
        getPlayersLowestListingPrice(playerIDs: $playerIDs) {
          playerID
          lowestListingPrice
        }
      }
    `;

    const variables = {
      playerIDs: playerIDs.split(",").map(id => id.trim())
    };

    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true);
    res.json({ ok: true, prices: data.getPlayersLowestListingPrice || [] });
  } catch (err) {
    console.error("[API] Error fetching player prices:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get player's weekly stats for kickoff slot analysis
app.get("/api/player/:playerId/weekly-stats", async (req, res) => {
  try {
    const { playerId } = req.params;
    const { stats, weeks = 5, orderBy = "DESC" } = req.query;

    // Parse stat categories - can be comma-separated or single
    const statCategories = stats
      ? stats.split(",").map(s => s.trim().toUpperCase())
      : ["TOUCHDOWNS", "RUSHING_YARDS", "RECEPTIONS_YARDS", "PASSES_SUCCEEDED_YARDS"];

    // Make parallel requests for each stat category
    const statPromises = statCategories.map(async (statCategory) => {
      const query = `
        query GetPlayersWeeklyStats($input: GetPlayersWeeklyStatsInput!) {
          getPlayersWeeklyStats(input: $input) {
            statsByWeek {
              stat
              value
              round
              opponentTeamID
              isHomeGame
              gameStartAt
              season
            }
          }
        }
      `;

      const variables = {
        input: {
          playerID: playerId,
          statCategories: statCategory,
          numberOfRounds: parseInt(weeks, 10),
          orderBy: orderBy.toUpperCase()
        }
      };

      try {
        const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 2, true, req);
        return {
          stat: statCategory,
          weeks: data?.getPlayersWeeklyStats?.statsByWeek || []
        };
      } catch (err) {
        console.error(`[API] Error fetching ${statCategory} for player ${playerId}:`, err.message);
        return { stat: statCategory, weeks: [], error: err.message };
      }
    });

    const results = await Promise.all(statPromises);

    // Combine results into a structured response
    const statsByCategory = {};
    results.forEach(result => {
      statsByCategory[result.stat] = result.weeks;
    });

    res.json({
      ok: true,
      playerId,
      stats: statsByCategory,
      categories: statCategories
    });
  } catch (err) {
    console.error("[API] Error fetching player weekly stats:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get weekly stats for multiple players (batch)
app.post("/api/players/weekly-stats", async (req, res) => {
  try {
    const { playerIDs, stats, weeks = 5 } = req.body;

    if (!playerIDs || !Array.isArray(playerIDs) || playerIDs.length === 0) {
      return res.status(400).json({ ok: false, error: "playerIDs array required in request body" });
    }

    const statCategories = stats || ["TOUCHDOWNS"];

    // Limit to prevent abuse
    const limitedPlayerIDs = playerIDs.slice(0, 20);

    const playerPromises = limitedPlayerIDs.map(async (playerId) => {
      const query = `
        query GetPlayersWeeklyStats($input: GetPlayersWeeklyStatsInput!) {
          getPlayersWeeklyStats(input: $input) {
            statsByWeek {
              stat
              value
              round
              opponentTeamID
              isHomeGame
              gameStartAt
              season
            }
          }
        }
      `;

      const variables = {
        input: {
          playerID: playerId,
          statCategories: statCategories[0], // Primary stat
          numberOfRounds: parseInt(weeks, 10),
          orderBy: "DESC"
        }
      };

      try {
        const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 2, true, req);
        return {
          playerId,
          stats: data?.getPlayersWeeklyStats?.statsByWeek || []
        };
      } catch (err) {
        return { playerId, stats: [], error: err.message };
      }
    });

    const results = await Promise.all(playerPromises);

    res.json({
      ok: true,
      players: results,
      requestedStats: statCategories
    });
  } catch (err) {
    console.error("[API] Error fetching batch player weekly stats:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ============================================================
// CHALLENGES API ENDPOINTS
// ============================================================

app.get("/api/challenges", async (req, res) => {
  try {
    const { first = 20, statuses } = req.query;

    // Use searchChallenges query (per dev recommendation)
    const query = `
      query searchChallenges($input: SearchChallengesInput!) {
        searchChallenges(input: $input) {
          edges {
            node {
              id
              name
              description
              status
              startDate
              endDate
              reward {
                id
                name
                description
                type
              }
              requirements {
                id
                description
                type
                count
                tiers
                setIDs
                playerIDs
                teamIDs
              }
              submissions {
                totalCount
              }
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const statusFilter = statuses
      ? statuses.split(",")
      : ["ACTIVE", "UPCOMING", "COMPLETED"];

    const variables = {
      input: {
        first: parseInt(first, 10),
        filters: {
          byStatuses: statusFilter
        }
      }
    };

    const data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true, req);

    if (data && data.searchChallenges) {
      const challenges = data.searchChallenges.edges.map(e => ({
        ...e.node,
        progress: 0, // User-specific progress would need auth
        requirements: (e.node.requirements || []).map(r => ({
          ...r,
          met: false // User-specific would need auth
        }))
      }));

      console.log(`[API] searchChallenges returned ${challenges.length} challenges`);

      return res.json({
        ok: true,
        challenges,
        totalCount: data.searchChallenges.totalCount,
        cached: isResponseCached(query, variables)
      });
    }

    throw new Error('No data returned from searchChallenges');
  } catch (err) {
    console.error("[API] Error fetching challenges:", err.message);

    // Check for auth errors
    if (err.message.includes('403') || err.message.includes('401') || err.message.includes('Unauthorized')) {
      return res.status(401).json({
        ok: false,
        error: "Authentication may be required",
        requiresAuth: true,
        details: err.message
      });
    }

    res.status(500).json({ ok: false, error: err.message });
  }
});

// ============================================================
// TRADE-IN LEADERBOARDS API
// ============================================================

// Get all published trade-in leaderboards
app.get("/api/tradeins", async (req, res) => {
  try {
    const query = `
      query GetPublishedLeaderboards {
        getPublishedLeaderboards {
          id
          slug
          name
          description
          startDate
          endDate
          status
          totalEntries
          reward {
            id
            name
            description
          }
        }
      }
    `;

    const data = await nfladGraphQLQuery(query, {}, getUserToken(req), 3, true, req);

    if (data && data.getPublishedLeaderboards) {
      const leaderboards = data.getPublishedLeaderboards;
      console.log(`[API] getPublishedLeaderboards returned ${leaderboards.length} trade-in leaderboards`);

      return res.json({
        ok: true,
        leaderboards,
        cached: isResponseCached(query, {})
      });
    }

    throw new Error('No data returned from getPublishedLeaderboards');
  } catch (err) {
    console.error("[API] Error fetching trade-in leaderboards:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Get specific trade-in leaderboard entries
app.get("/api/tradeins/:idOrSlug", async (req, res) => {
  try {
    const { idOrSlug } = req.params;
    const { first = 50, after } = req.query;

    const query = `
      query GetLeaderboard($input: GetLeaderboardInput!) {
        getLeaderboard(input: $input) {
          id
          slug
          name
          description
          startDate
          endDate
          status
          totalEntries
          reward {
            id
            name
            description
          }
          entries(first: $first, after: $after) {
            edges {
              node {
                id
                rank
                score
                user {
                  id
                  displayName
                  username
                }
                submittedAt
                moments {
                  id
                  flowID
                  serialNumber
                  player {
                    id
                    fullName
                  }
                  tier
                  set {
                    id
                    name
                  }
                }
              }
              cursor
            }
            pageInfo {
              endCursor
              hasNextPage
            }
            totalCount
          }
        }
      }
    `;

    // Try by slug first, then by ID
    const variables = {
      input: {
        slug: idOrSlug
      },
      first: parseInt(first, 10),
      after: after || null
    };

    let data;
    try {
      data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true, req);
    } catch (slugErr) {
      // Try by ID if slug fails
      variables.input = { id: idOrSlug };
      data = await nfladGraphQLQuery(query, variables, getUserToken(req), 3, true, req);
    }

    if (data && data.getLeaderboard) {
      const leaderboard = data.getLeaderboard;
      console.log(`[API] getLeaderboard returned leaderboard: ${leaderboard.name}`);

      return res.json({
        ok: true,
        leaderboard,
        entries: leaderboard.entries?.edges?.map(e => e.node) || [],
        totalEntries: leaderboard.entries?.totalCount || 0,
        cached: isResponseCached(query, variables)
      });
    }

    throw new Error('No data returned from getLeaderboard');
  } catch (err) {
    console.error("[API] Error fetching trade-in leaderboard:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ============================================================
// OFFERS API ENDPOINTS
// ============================================================

app.get("/api/offers/received", async (req, res) => {
  try {
    const userToken = getUserToken(req);
    // Note: We allow requests without token - GraphQL API may work with cookies
    // If authentication is required, the GraphQL API will return an error

    const query = `
      query GetReceivedOffers($input: SearchOffersInput!) {
        searchOffers(input: $input) {
          edges {
            node {
              id
              status
              price
              createdAt
              expiresAt
              type
              from {
                id
                displayName
                username
              }
              to {
                id
                displayName
                username
              }
              moments {
                id
                flowID
                player {
                  id
                  fullName
                }
                serialNumber
                tier
                set {
                  id
                  name
                }
              }
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        filters: {
          toMe: true,
          statuses: ["PENDING", "ACCEPTED", "REJECTED", "EXPIRED", "CANCELLED"]
        },
        first: 50
      }
    };

    try {
      const data = await nfladGraphQLQuery(query, variables, userToken, 3, true, req);

      // Transform GraphQL response to match frontend expectations
      const offers = (data.searchOffers?.edges || []).map(edge => {
        const offer = edge.node;
        return {
          id: offer.id,
          status: (offer.status || 'PENDING').toLowerCase(),
          price: offer.price || 0,
          createdAt: offer.createdAt,
          expiresAt: offer.expiresAt,
          type: offer.type || 'Standard',
          counterparty: offer.from?.displayName || offer.from?.username || 'Unknown',
          moments: (offer.moments || []).map(moment => ({
            id: moment.id,
            flowID: moment.flowID,
            playerName: moment.player?.fullName || 'Unknown Player',
            serialNumber: moment.serialNumber,
            tier: moment.tier,
            setName: moment.set?.name
          }))
        };
      });

      res.json({
        ok: true,
        offers,
        totalCount: data.searchOffers?.totalCount || 0,
        cached: isResponseCached(query, variables)
      });
    } catch (graphqlErr) {
      console.error("[API] GraphQL error for received offers:", graphqlErr.message);

      // Check if this is an authentication error
      if (graphqlErr.message.includes("Authentication") ||
        graphqlErr.message.includes("Unauthorized") ||
        graphqlErr.message.includes("401") ||
        graphqlErr.message.includes("403")) {
        return res.status(401).json({
          ok: false,
          error: "Authentication required",
          requiresAuth: true,
          message: "Please log in with your NFL All Day account to view received offers.",
          details: graphqlErr.message
        });
      }

      // If the query fails, it might be because the schema is different
      if (graphqlErr.message.includes("Cannot query field") || graphqlErr.message.includes("Unknown type")) {
        return res.status(503).json({
          ok: false,
          error: "Offers API not available",
          message: "The offers feature is not yet fully implemented. The GraphQL schema may need to be updated.",
          hint: "Check server logs for GraphQL schema details"
        });
      }

      res.status(503).json({
        ok: false,
        error: "Unable to fetch received offers",
        details: graphqlErr.message
      });
    }
  } catch (err) {
    console.error("[API] Error fetching received offers:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/offers/sent", async (req, res) => {
  try {
    const userToken = getUserToken(req);
    // Note: We allow requests without token - GraphQL API may work with cookies
    // If authentication is required, the GraphQL API will return an error

    const query = `
      query GetSentOffers($input: SearchOffersInput!) {
        searchOffers(input: $input) {
          edges {
            node {
              id
              status
              price
              createdAt
              expiresAt
              type
              from {
                id
                displayName
                username
              }
              to {
                id
                displayName
                username
              }
              moments {
                id
                flowID
                player {
                  id
                  fullName
                }
                serialNumber
                tier
                set {
                  id
                  name
                }
              }
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
          totalCount
        }
      }
    `;

    const variables = {
      input: {
        filters: {
          fromMe: true,
          statuses: ["PENDING", "ACCEPTED", "REJECTED", "EXPIRED", "CANCELLED"]
        },
        first: 50
      }
    };

    try {
      const data = await nfladGraphQLQuery(query, variables, userToken, 3, true, req);

      // Transform GraphQL response to match frontend expectations
      const offers = (data.searchOffers?.edges || []).map(edge => {
        const offer = edge.node;
        return {
          id: offer.id,
          status: (offer.status || 'PENDING').toLowerCase(),
          price: offer.price || 0,
          createdAt: offer.createdAt,
          expiresAt: offer.expiresAt,
          type: offer.type || 'Standard',
          counterparty: offer.to?.displayName || offer.to?.username || 'Unknown',
          moments: (offer.moments || []).map(moment => ({
            id: moment.id,
            flowID: moment.flowID,
            playerName: moment.player?.fullName || 'Unknown Player',
            serialNumber: moment.serialNumber,
            tier: moment.tier,
            setName: moment.set?.name
          }))
        };
      });

      res.json({
        ok: true,
        offers,
        totalCount: data.searchOffers?.totalCount || 0,
        cached: isResponseCached(query, variables)
      });
    } catch (graphqlErr) {
      console.error("[API] GraphQL error for sent offers:", graphqlErr.message);

      // Check if this is an authentication error
      if (graphqlErr.message.includes("Authentication") ||
        graphqlErr.message.includes("Unauthorized") ||
        graphqlErr.message.includes("401") ||
        graphqlErr.message.includes("403")) {
        return res.status(401).json({
          ok: false,
          error: "Authentication required",
          requiresAuth: true,
          message: "Please log in with your NFL All Day account to view sent offers.",
          details: graphqlErr.message
        });
      }

      // If the query fails, it might be because the schema is different
      if (graphqlErr.message.includes("Cannot query field") || graphqlErr.message.includes("Unknown type")) {
        return res.status(503).json({
          ok: false,
          error: "Offers API not available",
          message: "The offers feature is not yet fully implemented. The GraphQL schema may need to be updated.",
          hint: "Check server logs for GraphQL schema details"
        });
      }

      res.status(503).json({
        ok: false,
        error: "Unable to fetch sent offers",
        details: graphqlErr.message
      });
    }
  } catch (err) {
    console.error("[API] Error fetching sent offers:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Accept an offer (received offers only)
app.post("/api/offers/:offerId/accept", async (req, res) => {
  try {
    const userToken = getUserToken(req);
    // Note: Mutations typically require authentication, but we'll let GraphQL API handle it

    const { offerId } = req.params;

    const mutation = `
      mutation AcceptOffer($offerId: ID!) {
        acceptOffer(offerId: $offerId) {
          id
          status
        }
      }
    `;

    const variables = { offerId };

    try {
      const data = await nfladGraphQLQuery(mutation, variables, userToken, 3, false, req);

      if (data.acceptOffer) {
        res.json({ ok: true, offer: data.acceptOffer });
      } else {
        res.status(400).json({ ok: false, error: "Failed to accept offer" });
      }
    } catch (graphqlErr) {
      console.error("[API] GraphQL error accepting offer:", graphqlErr.message);
      res.status(503).json({
        ok: false,
        error: "Unable to accept offer",
        details: graphqlErr.message
      });
    }
  } catch (err) {
    console.error("[API] Error accepting offer:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Reject an offer (received offers only)
app.post("/api/offers/:offerId/reject", async (req, res) => {
  try {
    const userToken = getUserToken(req);
    // Note: Mutations typically require authentication, but we'll let GraphQL API handle it

    const { offerId } = req.params;

    const mutation = `
      mutation RejectOffer($offerId: ID!) {
        rejectOffer(offerId: $offerId) {
          id
          status
        }
      }
    `;

    const variables = { offerId };

    try {
      const data = await nfladGraphQLQuery(mutation, variables, userToken, 3, false, req);

      if (data.rejectOffer) {
        res.json({ ok: true, offer: data.rejectOffer });
      } else {
        res.status(400).json({ ok: false, error: "Failed to reject offer" });
      }
    } catch (graphqlErr) {
      console.error("[API] GraphQL error rejecting offer:", graphqlErr.message);
      res.status(503).json({
        ok: false,
        error: "Unable to reject offer",
        details: graphqlErr.message
      });
    }
  } catch (err) {
    console.error("[API] Error rejecting offer:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Cancel an offer (sent offers only)
app.post("/api/offers/:offerId/cancel", async (req, res) => {
  try {
    const userToken = getUserToken(req);
    // Note: Mutations typically require authentication, but we'll let GraphQL API handle it

    const { offerId } = req.params;

    const mutation = `
      mutation CancelOffer($offerId: ID!) {
        cancelOffer(offerId: $offerId) {
          id
          status
        }
      }
    `;

    const variables = { offerId };

    try {
      const data = await nfladGraphQLQuery(mutation, variables, userToken, 3, false, req);

      if (data.cancelOffer) {
        res.json({ ok: true, offer: data.cancelOffer });
      } else {
        res.status(400).json({ ok: false, error: "Failed to cancel offer" });
      }
    } catch (graphqlErr) {
      console.error("[API] GraphQL error canceling offer:", graphqlErr.message);
      res.status(503).json({
        ok: false,
        error: "Unable to cancel offer",
        details: graphqlErr.message
      });
    }
  } catch (err) {
    console.error("[API] Error canceling offer:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ============================================================
// NEW ANALYTICS ENDPOINTS
// ============================================================

// Get filter options for dropdowns (teams, sets, series)
app.get("/api/filter-options", async (req, res) => {
  try {
    const type = (req.query.type || "").toString().toLowerCase();

    let query = "";
    if (type === "teams") {
      query = `SELECT DISTINCT team_name as option FROM nft_core_metadata_v2 WHERE team_name IS NOT NULL AND team_name != '' ORDER BY team_name`;
    } else if (type === "sets") {
      query = `SELECT DISTINCT set_name as option FROM nft_core_metadata_v2 WHERE set_name IS NOT NULL AND set_name != '' ORDER BY set_name`;
    } else if (type === "series") {
      query = `SELECT DISTINCT series_name as option FROM nft_core_metadata_v2 WHERE series_name IS NOT NULL AND series_name != '' ORDER BY series_name`;
    } else {
      return res.status(400).json({ ok: false, error: "Invalid type. Use: teams, sets, or series" });
    }

    const result = await pgQuery(query);
    return res.json({
      ok: true,
      options: result.rows.map(r => r.option)
    });
  } catch (err) {
    console.error("Error in /api/filter-options:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Advanced NFT search with multiple filters
app.get("/api/search-nfts-advanced", async (req, res) => {
  try {
    const player = (req.query.player || "").toString().trim();
    const team = (req.query.team || "").toString().trim();
    const tier = (req.query.tier || "").toString().trim().toUpperCase();
    const set = (req.query.set || "").toString().trim();
    const series = (req.query.series || "").toString().trim();
    const serial = parseInt(req.query.serial) || null;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);

    const conditions = [];
    const params = [];
    let paramIndex = 1;

    if (player) {
      conditions.push(`CONCAT(COALESCE(m.first_name,''), ' ', COALESCE(m.last_name,'')) ILIKE $${paramIndex}`);
      params.push(`%${player}%`);
      paramIndex++;
    }

    if (team) {
      conditions.push(`m.team_name = $${paramIndex}`);
      params.push(team);
      paramIndex++;
    }

    if (tier) {
      conditions.push(`UPPER(m.tier) = $${paramIndex}`);
      params.push(tier);
      paramIndex++;
    }

    if (set) {
      conditions.push(`m.set_name = $${paramIndex}`);
      params.push(set);
      paramIndex++;
    }

    if (series) {
      conditions.push(`m.series_name = $${paramIndex}`);
      params.push(series);
      paramIndex++;
    }

    if (serial) {
      conditions.push(`m.serial_number = $${paramIndex}`);
      params.push(serial);
      paramIndex++;
    }

    if (conditions.length === 0) {
      return res.status(400).json({ ok: false, error: "At least one filter is required" });
    }

    const whereClause = conditions.join(' AND ');
    params.push(limit);

    const result = await pgQuery(
      `SELECT 
        m.nft_id, m.edition_id, m.first_name, m.last_name, m.team_name,
        m.set_name, m.series_name, m.tier, m.serial_number, m.max_mint_size,
        m.jersey_number,
        h.wallet_address, p.display_name as owner_name
      FROM nft_core_metadata_v2 m
      LEFT JOIN wallet_holdings h ON h.nft_id = m.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      WHERE ${whereClause}
      ORDER BY 
        CASE m.tier 
          WHEN 'ULTIMATE' THEN 1 
          WHEN 'LEGENDARY' THEN 2 
          WHEN 'RARE' THEN 3 
          WHEN 'UNCOMMON' THEN 4 
          ELSE 5 
        END,
        m.serial_number ASC
      LIMIT $${paramIndex}`,
      params
    );

    return res.json({
      ok: true,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/search-nfts-advanced:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Search NFTs by player name, team, set, series, etc.
app.get("/api/search-nfts", async (req, res) => {
  try {
    const q = (req.query.q || "").toString().trim();
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);

    if (!q || q.length < 2) {
      return res.status(400).json({ ok: false, error: "Search query too short (min 2 chars)" });
    }

    const pattern = `%${q}%`;

    // Search by player name, team, set name, or series
    const result = await pgQuery(
      `SELECT 
        m.nft_id, m.edition_id, m.first_name, m.last_name, m.team_name,
        m.set_name, m.series_name, m.tier, m.serial_number, m.max_mint_size,
        m.jersey_number,
        h.wallet_address, p.display_name as owner_name
      FROM nft_core_metadata_v2 m
      LEFT JOIN wallet_holdings h ON h.nft_id = m.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      WHERE 
        CONCAT(COALESCE(m.first_name,''), ' ', COALESCE(m.last_name,'')) ILIKE $1
        OR m.team_name ILIKE $1
        OR m.set_name ILIKE $1
        OR m.series_name ILIKE $1
        OR m.nft_id::text = $2
        OR m.edition_id::text = $2
      ORDER BY 
        CASE m.tier 
          WHEN 'ULTIMATE' THEN 1 
          WHEN 'LEGENDARY' THEN 2 
          WHEN 'RARE' THEN 3 
          WHEN 'UNCOMMON' THEN 4 
          ELSE 5 
        END,
        m.serial_number ASC
      LIMIT $3`,
      [pattern, q, limit]
    );

    return res.json({
      ok: true,
      count: result.rowCount,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/search-nfts:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Set Completion - track set completion progress
// Cache for set totals (refreshed every 10 minutes)
let setTotalsCache = null;
let setTotalsCacheTime = 0;
const SET_TOTALS_CACHE_TTL = 10 * 60 * 1000; // 10 minutes

async function ensureSetsCatalogTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS sets_catalog (
      set_name TEXT PRIMARY KEY,
      total_editions INTEGER DEFAULT 0,
      common INTEGER DEFAULT 0,
      uncommon INTEGER DEFAULT 0,
      rare INTEGER DEFAULT 0,
      legendary INTEGER DEFAULT 0,
      ultimate INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function refreshSetsCatalog() {
  await ensureSetsCatalogTable();
  const rows = await pgQuery(`
    INSERT INTO sets_catalog (set_name, total_editions, common, uncommon, rare, legendary, ultimate, updated_at)
    SELECT 
      set_name,
      COUNT(DISTINCT edition_id) as total_editions,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'COMMON' THEN edition_id END) as common,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'UNCOMMON' THEN edition_id END) as uncommon,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'RARE' THEN edition_id END) as rare,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'LEGENDARY' THEN edition_id END) as legendary,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'ULTIMATE' THEN edition_id END) as ultimate,
      now()
    FROM nft_core_metadata_v2
    WHERE set_name IS NOT NULL
    GROUP BY set_name
    ON CONFLICT (set_name) DO UPDATE SET
      total_editions = EXCLUDED.total_editions,
      common        = EXCLUDED.common,
      uncommon      = EXCLUDED.uncommon,
      rare          = EXCLUDED.rare,
      legendary     = EXCLUDED.legendary,
      ultimate      = EXCLUDED.ultimate,
      updated_at    = now()
    RETURNING set_name;
  `);
  console.log(`[Set Completion] Refreshed sets_catalog (${rows.rowCount} rows)`);
}

async function getSetTotals() {
  const now = Date.now();
  if (setTotalsCache && (now - setTotalsCacheTime) < SET_TOTALS_CACHE_TTL) {
    return setTotalsCache;
  }

  // Fast path: set_totals_snapshot (prebuilt, small)
  try {
    const snap = await pgQuery(`SELECT set_name, total_editions FROM set_totals_snapshot`);
    if (snap.rowCount > 0) {
      setTotalsCache = new Map(snap.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
      setTotalsCacheTime = now;
      console.log(`[Set Completion] Loaded set totals from set_totals_snapshot (${snap.rowCount} rows)`);
      return setTotalsCache;
    }
  } catch (err) {
    console.warn("[Set Completion] set_totals_snapshot not available, falling back:", err.message);
  }

  await ensureSetsCatalogTable();
  // Try to read from catalog if it exists and is fresh
  const catalog = await pgQuery(
    `SELECT set_name, total_editions, updated_at FROM sets_catalog WHERE updated_at > now() - interval '15 minutes'`
  );
  if (catalog.rowCount > 0) {
    setTotalsCache = new Map(catalog.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
    setTotalsCacheTime = now;
    console.log(`[Set Completion] Loaded set totals from sets_catalog (${catalog.rowCount} rows)`);
    return setTotalsCache;
  }

  // Fallback: rebuild catalog, then return as map
  await refreshSetsCatalog();
  const fresh = await pgQuery(`SELECT set_name, total_editions FROM sets_catalog`);
  setTotalsCache = new Map(fresh.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
  setTotalsCacheTime = now;
  console.log(`[Set Completion] Cached ${setTotalsCache.size} set totals (rebuilt)`);
  return setTotalsCache;
}

// Public: list all sets with total editions
app.get("/api/set-completion/all", async (_req, res) => {
  try {
    const setTotals = await getSetTotals();

    const sets = Array.from(setTotals.entries())
      .map(([set_name, total]) => ({
        set_name,
        total,
        is_team_set: false // computed on wallet fetch; keeps this endpoint fast
      }))
      .sort((a, b) => a.set_name.localeCompare(b.set_name));
    return res.json({ ok: true, sets });
  } catch (err) {
    console.error("Error in /api/set-completion/all:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/set-completion", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) return res.status(400).json({ ok: false, error: "Missing ?wallet=" });

    const startTime = Date.now();
    let stepTime = Date.now();

    // Get cached set totals (fast - from memory)
    const setTotals = await getSetTotals();
    console.log(`[Set Completion] Step 1 - getSetTotals: ${Date.now() - stepTime}ms`);
    stepTime = Date.now();

    // Query only what the user owns (much faster - single pass)
    const result = await pgQuery(`
      SELECT 
        m.set_name,
        COUNT(DISTINCT m.edition_id) as owned_editions,
        COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'COMMON' THEN m.edition_id END) as common,
        COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'UNCOMMON' THEN m.edition_id END) as uncommon,
        COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'RARE' THEN m.edition_id END) as rare,
        COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'LEGENDARY' THEN m.edition_id END) as legendary,
        COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'ULTIMATE' THEN m.edition_id END) as ultimate
      FROM wallet_holdings h
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1 AND m.set_name IS NOT NULL
      GROUP BY m.set_name
      ORDER BY m.set_name`,
      [wallet]
    );
    console.log(`[Set Completion] Step 2 - owned editions: ${Date.now() - stepTime}ms`);
    stepTime = Date.now();

    // Cost to complete (sum lowest ask of missing editions) using set_editions_snapshot for speed
    const costRes = await pgQuery(
      `
      WITH owned AS (
        SELECT DISTINCT m.edition_id
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
      ),
      missing AS (
        SELECT s.set_name, s.edition_id, s.lowest_ask_usd
        FROM set_editions_snapshot s
        LEFT JOIN owned o ON s.edition_id = o.edition_id
        WHERE o.edition_id IS NULL
      )
      SELECT set_name, COALESCE(SUM(lowest_ask_usd), 0) AS cost_to_complete
      FROM missing
      GROUP BY set_name;
      `,
      [wallet]
    );
    const costMap = new Map(costRes.rows.map(r => [r.set_name, Number(r.cost_to_complete) || 0]));
    console.log(`[Set Completion] Step 3 - cost to complete: ${Date.now() - stepTime}ms`);
    stepTime = Date.now();

    // Team totals (for team-level tracking)
    const teamTotalsRes = await pgQuery(`
      SELECT team_name, COUNT(DISTINCT edition_id) AS total_editions
      FROM nft_core_metadata_v2
      WHERE team_name IS NOT NULL AND team_name <> ''
      GROUP BY team_name
    `);
    const teamTotals = new Map(teamTotalsRes.rows.map(r => [(r.team_name || "").toLowerCase(), parseInt(r.total_editions)]));
    console.log(`[Set Completion] Step 4 - team totals: ${Date.now() - stepTime}ms`);
    stepTime = Date.now();

    // Merge with cached totals
    const sets = result.rows.map(r => {
      const total = setTotals.get(r.set_name) || 0;
      const owned = parseInt(r.owned_editions);
      const cost = costMap.get(r.set_name) || 0;
      return {
        set_name: r.set_name,
        total,
        owned,
        completion: total > 0 ? Math.round(owned * 1000 / total) / 10 : 0,
        cost_to_complete: cost,
        // classify team set if set name contains a known team name
        is_team_set: Array.from(teamTotals.keys()).some(t => (r.set_name || "").toLowerCase().includes(t)),
        by_tier: {
          Common: parseInt(r.common) || 0,
          Uncommon: parseInt(r.uncommon) || 0,
          Rare: parseInt(r.rare) || 0,
          Legendary: parseInt(r.legendary) || 0,
          Ultimate: parseInt(r.ultimate) || 0
        }
      };
    }).sort((a, b) => b.completion - a.completion || a.set_name.localeCompare(b.set_name));

    // Team progress
    const teamOwnedRes = await pgQuery(
      `
      SELECT m.team_name, COUNT(DISTINCT m.edition_id) AS owned_editions
      FROM wallet_holdings h
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1 AND m.team_name IS NOT NULL AND m.team_name <> ''
      GROUP BY m.team_name;
      `,
      [wallet]
    );
    const teamProgress = teamOwnedRes.rows.map(r => {
      const key = (r.team_name || "").toLowerCase();
      const total = teamTotals.get(key) || 0;
      const owned = parseInt(r.owned_editions);
      return {
        team_name: r.team_name,
        total,
        owned,
        completion: total > 0 ? Math.round(owned * 1000 / total) / 10 : 0
      };
    }).sort((a, b) => b.completion - a.completion || a.team_name.localeCompare(b.team_name));

    const summary = {
      total_sets: sets.length,
      completed_sets: sets.filter(s => s.completion === 100).length,
      in_progress: sets.filter(s => s.completion > 0 && s.completion < 100).length,
      avg_completion: sets.length > 0 ? sets.reduce((a, b) => a + b.completion, 0) / sets.length : 0
    };

    const elapsed = Date.now() - startTime;
    console.log(`[Set Completion] Query for ${wallet.substring(0, 10)}... completed in ${elapsed}ms (${sets.length} sets)`);

    return res.json({ ok: true, summary, sets, team_progress: teamProgress });
  } catch (err) {
    console.error("Error in /api/set-completion:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Serial Finder - find NFTs with specific serial numbers
app.get("/api/serial-finder", async (req, res) => {
  try {
    const serial = parseInt(req.query.serial);
    const tierParam = (req.query.tier || "").trim();
    const player = (req.query.player || "").trim();
    const team = (req.query.team || "").trim();
    const seriesParam = (req.query.series || "").trim();
    const setParam = (req.query.set || "").trim();
    const positionParam = (req.query.position || "").trim();

    // Parse comma-separated values for multi-select filters
    const tiers = tierParam ? tierParam.split(',').map(t => t.trim().toUpperCase()).filter(Boolean) : [];
    const seriesList = seriesParam ? seriesParam.split(',').map(s => s.trim()).filter(Boolean) : [];
    const sets = setParam ? setParam.split(',').map(s => s.trim()).filter(Boolean) : [];
    const positions = positionParam ? positionParam.split(',').map(p => p.trim().toUpperCase()).filter(Boolean) : [];

    if (!serial || serial < 1) return res.status(400).json({ ok: false, error: "Missing ?serial=" });

    let query = `
      SELECT 
        h.wallet_address, h.is_locked,
        m.nft_id, m.first_name, m.last_name, m.team_name, m.tier, m.set_name,
        m.serial_number, m.jersey_number, m.series_name, m.position,
        p.display_name
      FROM wallet_holdings h
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      WHERE m.serial_number = $1
    `;
    const params = [serial];
    let paramIndex = 2;

    // Tier filter (multi-select with IN clause)
    if (tiers.length > 0) {
      const placeholders = tiers.map((_, i) => `$${paramIndex + i}`).join(', ');
      query += ` AND UPPER(m.tier) IN (${placeholders})`;
      params.push(...tiers);
      paramIndex += tiers.length;
    }

    if (player) {
      query += ` AND TRIM(COALESCE(m.first_name, '') || ' ' || COALESCE(m.last_name, '')) ILIKE $${paramIndex}`;
      params.push(player);
      paramIndex++;
    }

    if (team) {
      query += ` AND m.team_name ILIKE $${paramIndex}`;
      params.push(`%${team}%`);
      paramIndex++;
    }

    // Series filter (multi-select)
    if (seriesList.length > 0) {
      const conditions = seriesList.map((_, i) => `m.series_name ILIKE $${paramIndex + i}`).join(' OR ');
      query += ` AND (${conditions})`;
      seriesList.forEach(s => params.push(`%${s}%`));
      paramIndex += seriesList.length;
    }

    // Set filter (multi-select)
    if (sets.length > 0) {
      const conditions = sets.map((_, i) => `m.set_name ILIKE $${paramIndex + i}`).join(' OR ');
      query += ` AND (${conditions})`;
      sets.forEach(s => params.push(`%${s}%`));
      paramIndex += sets.length;
    }

    // Position filter (multi-select with IN clause)
    if (positions.length > 0) {
      const placeholders = positions.map((_, i) => `$${paramIndex + i}`).join(', ');
      query += ` AND UPPER(m.position) IN (${placeholders})`;
      params.push(...positions);
      paramIndex += positions.length;
    }

    query += ` ORDER BY m.tier DESC, m.last_name LIMIT 500`;

    const result = await pgQuery(query, params);

    const uniqueOwners = new Set(result.rows.map(r => r.wallet_address)).size;
    const jerseyMatches = result.rows.filter(r => r.serial_number == r.jersey_number).length;

    return res.json({
      ok: true,
      serial,
      count: result.rowCount,
      unique_owners: uniqueOwners,
      jersey_matches: jerseyMatches,
      rows: result.rows
    });
  } catch (err) {
    console.error("Error in /api/serial-finder:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Wallet Comparison - compare two wallets
app.get("/api/wallet-compare", async (req, res) => {
  try {
    const w1 = (req.query.wallet1 || "").toString().trim().toLowerCase();
    const w2 = (req.query.wallet2 || "").toString().trim().toLowerCase();

    if (!w1 || !w2) return res.status(400).json({ ok: false, error: "Missing wallet1 or wallet2" });

    const statsQuery = `
      SELECT 
        h.wallet_address,
        p.display_name,
        COUNT(*)::int as total,
        COUNT(*) FILTER (WHERE h.is_locked)::int as locked,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int as common,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int as uncommon,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int as rare,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int as legendary,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int as ultimate,
        COALESCE(SUM(eps.lowest_ask_usd), 0)::numeric as floor_value
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
      WHERE h.wallet_address = $1
      GROUP BY h.wallet_address, p.display_name
    `;

    const [r1, r2] = await Promise.all([
      pgQuery(statsQuery, [w1]),
      pgQuery(statsQuery, [w2])
    ]);

    const s1 = r1.rows[0] || { total: 0, locked: 0, common: 0, uncommon: 0, rare: 0, legendary: 0, ultimate: 0, floor_value: 0 };
    const s2 = r2.rows[0] || { total: 0, locked: 0, common: 0, uncommon: 0, rare: 0, legendary: 0, ultimate: 0, floor_value: 0 };

    // Shared editions
    const sharedResult = await pgQuery(
      `SELECT COUNT(DISTINCT m1.edition_id)::int as shared_editions,
              COUNT(DISTINCT CONCAT(m1.first_name, m1.last_name))::int as shared_players
       FROM wallet_holdings h1
       JOIN nft_core_metadata_v2 m1 ON m1.nft_id = h1.nft_id
       WHERE h1.wallet_address = $1
         AND m1.edition_id IN (
           SELECT m2.edition_id FROM wallet_holdings h2
           JOIN nft_core_metadata_v2 m2 ON m2.nft_id = h2.nft_id
           WHERE h2.wallet_address = $2
         )`,
      [w1, w2]
    );

    return res.json({
      ok: true,
      wallet1: { address: w1, display_name: s1.display_name, ...s1 },
      wallet2: { address: w2, display_name: s2.display_name, ...s2 },
      shared: sharedResult.rows[0] || { shared_editions: 0, shared_players: 0 }
    });
  } catch (err) {
    console.error("Error in /api/wallet-compare:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Rarity Score Leaderboard - cached for performance
let rarityLeaderboardCache = null;
let rarityLeaderboardCacheTime = 0;
const RARITY_LEADERBOARD_CACHE_TTL = 30 * 60 * 1000; // 30 minutes

async function computeRarityScore(walletAddress) {
  const result = await pgQuery(
    `SELECT 
      h.nft_id, m.serial_number, m.jersey_number, m.tier, m.max_mint_size,
      m.first_name, m.last_name, m.edition_id
    FROM wallet_holdings h
    JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
    WHERE h.wallet_address = $1`,
    [walletAddress]
  );

  const rows = result.rows;
  let score = 0;
  let serial1Count = 0, serial10Count = 0, jerseyMatchCount = 0;
  let ultimateCount = 0, legendaryCount = 0, rareCount = 0;

  for (const r of rows) {
    let pts = 0;
    const serial = parseInt(r.serial_number) || 9999;
    const tier = (r.tier || '').toUpperCase();

    // Serial scoring
    if (serial === 1) { serial1Count++; pts += 1000; }
    else if (serial <= 10) { serial10Count++; pts += 200; }
    else if (serial <= 100) pts += 20;

    // Jersey match
    if (r.jersey_number && serial == r.jersey_number) {
      jerseyMatchCount++; pts += 300;
    }

    // Tier scoring
    if (tier === 'ULTIMATE') { ultimateCount++; pts += 500; }
    else if (tier === 'LEGENDARY') { legendaryCount++; pts += 200; }
    else if (tier === 'RARE') { rareCount++; pts += 50; }

    score += pts;
  }

  const uniqueEditions = new Set(rows.map(r => r.edition_id)).size;
  score += uniqueEditions * 2 + rows.length;

  return {
    wallet: walletAddress,
    score: Math.round(score),
    moments: rows.length,
    serial1Count,
    ultimateCount,
    legendaryCount
  };
}

// Known contract/holding addresses to exclude from leaderboard
const EXCLUDED_LEADERBOARD_WALLETS = [
  '0xe4cf4bdc1751c65d', // NFL All Day contract
  '0xb6f2481eba4df97b', // Huge custodial/system wallet  
  '0x4eb8a10cb9f87357', // NFT Storefront contract
  '0xf919ee77447b7497', // Dapper wallet / marketplace
  '0x4eded0de73c5b00c', // Another system wallet
  '0x0b2a3299cc857e29', // Pack distribution
];

app.get("/api/rarity-leaderboard", async (req, res) => {
  try {
    const now = Date.now();
    const forceRefresh = req.query.refresh === 'true';

    // Return cached if available and not expired
    if (!forceRefresh && rarityLeaderboardCache && (now - rarityLeaderboardCacheTime) < RARITY_LEADERBOARD_CACHE_TTL) {
      return res.json({
        ok: true,
        leaderboard: rarityLeaderboardCache,
        cached: true,
        cache_age_minutes: Math.round((now - rarityLeaderboardCacheTime) / 60000)
      });
    }

    // Try on-disk snapshot to avoid recompute
    if (!forceRefresh && fs.existsSync(RARITY_LEADERBOARD_SNAPSHOT_FILE)) {
      try {
        const snapshot = JSON.parse(fs.readFileSync(RARITY_LEADERBOARD_SNAPSHOT_FILE, "utf8"));
        if (Array.isArray(snapshot)) {
          rarityLeaderboardCache = snapshot;
          rarityLeaderboardCacheTime = now;
          return res.json({
            ok: true,
            leaderboard: snapshot,
            cached: true,
            fromSnapshot: true,
            cache_age_minutes: null
          });
        }
      } catch (e) {
        console.warn("[Rarity Leaderboard] Failed to read snapshot:", e.message);
      }
    }

    console.log("[Rarity Leaderboard] Computing leaderboard (this may take a moment)...");
    const startTime = Date.now();

    // Get all wallets with significant holdings (at least 10 moments for leaderboard)
    // Exclude known contract/holding addresses
    const walletsResult = await pgQuery(`
      SELECT wallet_address, COUNT(*) as moment_count
      FROM wallet_holdings
      WHERE wallet_address NOT IN (${EXCLUDED_LEADERBOARD_WALLETS.map((_, i) => `$${i + 1}`).join(', ')})
      GROUP BY wallet_address
      HAVING COUNT(*) >= 10
      ORDER BY COUNT(*) DESC
      LIMIT 500
    `, EXCLUDED_LEADERBOARD_WALLETS);

    // Compute scores for top wallets (batch for efficiency)
    const leaderboard = [];

    // Process in parallel batches of 20
    const batchSize = 20;
    for (let i = 0; i < walletsResult.rows.length; i += batchSize) {
      const batch = walletsResult.rows.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map(w => computeRarityScore(w.wallet_address).catch(() => null))
      );
      leaderboard.push(...batchResults.filter(r => r !== null));
    }

    // Sort by score and take top 100
    leaderboard.sort((a, b) => b.score - a.score);
    const top100 = leaderboard.slice(0, 100);

    // Add rank and get display names from wallet_profiles
    const walletAddresses = top100.map(e => e.wallet);
    let displayNames = {};

    try {
      // Try wallet_profiles first (primary source for display names)
      const namesResult = await pgQuery(`
        SELECT wallet_address, display_name 
        FROM wallet_profiles 
        WHERE wallet_address = ANY($1) AND display_name IS NOT NULL AND display_name != ''
      `, [walletAddresses]);
      displayNames = Object.fromEntries(namesResult.rows.map(r => [r.wallet_address, r.display_name]));

      // Fall back to wallet_holdings for any missing names
      const missingWallets = walletAddresses.filter(w => !displayNames[w]);
      if (missingWallets.length > 0) {
        const holdingsNames = await pgQuery(`
          SELECT DISTINCT ON (wallet_address) wallet_address, display_name 
          FROM wallet_holdings 
          WHERE wallet_address = ANY($1) AND display_name IS NOT NULL AND display_name != ''
        `, [missingWallets]);
        for (const r of holdingsNames.rows) {
          if (!displayNames[r.wallet_address]) {
            displayNames[r.wallet_address] = r.display_name;
          }
        }
      }
    } catch (e) {
      console.log("[Rarity Leaderboard] Could not fetch display names:", e.message);
    }

    const rankedLeaderboard = top100.map((entry, idx) => ({
      rank: idx + 1,
      ...entry,
      displayName: displayNames[entry.wallet] || null
    }));

    // Cache the result
    rarityLeaderboardCache = rankedLeaderboard;
    rarityLeaderboardCacheTime = now;
    try {
      fs.mkdirSync(path.dirname(RARITY_LEADERBOARD_SNAPSHOT_FILE), { recursive: true });
      fs.writeFileSync(RARITY_LEADERBOARD_SNAPSHOT_FILE, JSON.stringify(rankedLeaderboard, null, 2), "utf8");
    } catch (e) {
      console.warn("[Rarity Leaderboard] Failed to write snapshot:", e.message);
    }

    const elapsed = Date.now() - startTime;
    console.log(`[Rarity Leaderboard] Computed ${rankedLeaderboard.length} entries in ${elapsed}ms`);

    return res.json({
      ok: true,
      leaderboard: rankedLeaderboard,
      cached: false,
      computed_in_ms: elapsed
    });
  } catch (err) {
    console.error("Error in /api/rarity-leaderboard:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Rarity Score Calculator
app.get("/api/rarity-score", async (req, res) => {
  try {
    const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
    if (!wallet) return res.status(400).json({ ok: false, error: "Missing ?wallet=" });

    const result = await pgQuery(
      `SELECT 
        h.nft_id, m.serial_number, m.jersey_number, m.tier, m.max_mint_size,
        m.first_name, m.last_name
      FROM wallet_holdings h
      JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.wallet_address = $1`,
      [wallet]
    );

    const rows = result.rows;
    let score = 0;
    const breakdown = {
      serial_1_count: 0, serial_1_points: 0,
      serial_10_count: 0, serial_10_points: 0,
      jersey_match_count: 0, jersey_match_points: 0,
      ultimate_count: 0, ultimate_points: 0,
      legendary_count: 0, legendary_points: 0,
      rare_count: 0, rare_points: 0,
      total_moments: rows.length,
      collection_points: rows.length,
      unique_editions: new Set(rows.map(r => r.edition_id)).size,
      edition_points: 0,
      low_serial_pct: 0
    };

    const topMoments = [];

    for (const r of rows) {
      let pts = 0;
      const serial = parseInt(r.serial_number) || 9999;
      const tier = (r.tier || '').toUpperCase();

      // Serial scoring
      if (serial === 1) { breakdown.serial_1_count++; pts += 1000; breakdown.serial_1_points += 1000; }
      else if (serial <= 10) { breakdown.serial_10_count++; pts += 200; breakdown.serial_10_points += 200; }
      else if (serial <= 100) pts += 20;

      // Jersey match
      if (r.jersey_number && serial == r.jersey_number) {
        breakdown.jersey_match_count++; pts += 300; breakdown.jersey_match_points += 300;
      }

      // Tier scoring
      if (tier === 'ULTIMATE') { breakdown.ultimate_count++; pts += 500; breakdown.ultimate_points += 500; }
      else if (tier === 'LEGENDARY') { breakdown.legendary_count++; pts += 200; breakdown.legendary_points += 200; }
      else if (tier === 'RARE') { breakdown.rare_count++; pts += 50; breakdown.rare_points += 50; }

      score += pts;
      if (pts >= 100) topMoments.push({ ...r, points: pts });
    }

    breakdown.edition_points = breakdown.unique_editions * 2;
    score += breakdown.edition_points + breakdown.collection_points;

    const lowSerialCount = rows.filter(r => (parseInt(r.serial_number) || 9999) <= 100).length;
    breakdown.low_serial_pct = rows.length > 0 ? Math.round(lowSerialCount / rows.length * 100) : 0;

    topMoments.sort((a, b) => b.points - a.points);

    // Find rank from cached leaderboard
    let rank = null;
    let totalWallets = null;
    if (rarityLeaderboardCache) {
      const entry = rarityLeaderboardCache.find(e => e.wallet === wallet);
      if (entry) {
        rank = entry.rank;
      }
      totalWallets = rarityLeaderboardCache.length;
    }

    return res.json({
      ok: true,
      score: Math.round(score),
      rank,
      total_wallets: totalWallets,
      breakdown,
      top_moments: topMoments.slice(0, 10)
    });
  } catch (err) {
    console.error("Error in /api/rarity-score:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ================== P2P TRADING API ==================

// Initialize trades table
async function initTradesTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        initiator_wallet TEXT NOT NULL,
        initiator_child_wallet TEXT,
        target_wallet TEXT NOT NULL,
        target_child_wallet TEXT,
        initiator_nft_ids JSONB NOT NULL DEFAULT '[]',
        target_nft_ids JSONB NOT NULL DEFAULT '[]',
        status TEXT DEFAULT 'pending',
        initiator_signed BOOLEAN DEFAULT FALSE,
        target_signed BOOLEAN DEFAULT FALSE,
        tx_id TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_trades_initiator ON trades(initiator_wallet)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_trades_target ON trades(target_wallet)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)`);

    console.log("✅ Trades table initialized");
  } catch (err) {
    console.error("Failed to initialize trades table:", err.message);
  }
}

// Initialize analytics tables
async function initAnalyticsTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS analytics_page_views (
        id SERIAL PRIMARY KEY,
        page_path TEXT NOT NULL,
        wallet_address TEXT,
        session_id TEXT,
        ip_hash TEXT,
        user_agent TEXT,
        referrer TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS analytics_sessions (
        id SERIAL PRIMARY KEY,
        session_id TEXT UNIQUE NOT NULL,
        wallet_address TEXT,
        started_at TIMESTAMPTZ DEFAULT NOW(),
        last_activity TIMESTAMPTZ DEFAULT NOW(),
        page_count INTEGER DEFAULT 1
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_page_views_created ON analytics_page_views(created_at)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_page_views_path ON analytics_page_views(page_path)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sessions_last_activity ON analytics_sessions(last_activity)`);

    console.log("✅ Analytics tables initialized");
  } catch (err) {
    console.error("Failed to initialize analytics tables:", err.message);
  }
}

// POST /api/trades - Create a new trade offer
app.post("/api/trades", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const initiatorWallet = user.default_wallet_address;
    if (!initiatorWallet) {
      return res.status(400).json({ ok: false, error: "No wallet connected" });
    }

    const {
      targetWallet,
      initiatorNftIds = [],
      targetNftIds = [],
      initiatorChildWallet,
      targetChildWallet
    } = req.body;

    if (!targetWallet) {
      return res.status(400).json({ ok: false, error: "Target wallet required" });
    }

    if (initiatorNftIds.length === 0 && targetNftIds.length === 0) {
      return res.status(400).json({ ok: false, error: "At least one NFT must be included" });
    }

    const { rows } = await pool.query(`
      INSERT INTO trades (
        initiator_wallet, initiator_child_wallet, 
        target_wallet, target_child_wallet,
        initiator_nft_ids, target_nft_ids,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, 'pending')
      RETURNING *
    `, [
      initiatorWallet.toLowerCase(),
      initiatorChildWallet?.toLowerCase() || null,
      targetWallet.toLowerCase(),
      targetChildWallet?.toLowerCase() || null,
      JSON.stringify(initiatorNftIds),
      JSON.stringify(targetNftIds)
    ]);

    console.log(`[Trade] Created trade #${rows[0].id} from ${initiatorWallet.substring(0, 8)}... to ${targetWallet.substring(0, 8)}...`);

    return res.json({ ok: true, trade: rows[0] });
  } catch (err) {
    console.error("POST /api/trades error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/trades - List trades for current user
app.get("/api/trades", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    if (!wallet) {
      return res.json({ ok: true, trades: [] });
    }

    const { rows } = await pool.query(`
      SELECT * FROM trades 
      WHERE initiator_wallet = $1 OR target_wallet = $1
      ORDER BY created_at DESC
      LIMIT 100
    `, [wallet]);

    return res.json({ ok: true, trades: rows });
  } catch (err) {
    console.error("GET /api/trades error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/trades/:id - Get single trade details
app.get("/api/trades/:id", async (req, res) => {
  try {
    const tradeId = parseInt(req.params.id);
    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    const { rows } = await pool.query(`SELECT * FROM trades WHERE id = $1`, [tradeId]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found" });
    }

    return res.json({ ok: true, trade: rows[0] });
  } catch (err) {
    console.error("GET /api/trades/:id error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/trades/:id/accept - Accept a trade offer
app.post("/api/trades/:id/accept", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const tradeId = parseInt(req.params.id);

    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    const { rows } = await pool.query(`
      UPDATE trades 
      SET status = 'accepted', updated_at = NOW()
      WHERE id = $1 AND target_wallet = $2 AND status = 'pending'
      RETURNING *
    `, [tradeId, wallet]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found or not authorized" });
    }

    console.log(`[Trade] Trade #${tradeId} accepted by ${wallet?.substring(0, 8)}...`);

    return res.json({ ok: true, trade: rows[0] });
  } catch (err) {
    console.error("POST /api/trades/:id/accept error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/trades/:id/reject - Reject a trade offer
app.post("/api/trades/:id/reject", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const tradeId = parseInt(req.params.id);

    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    const { rows } = await pool.query(`
      UPDATE trades 
      SET status = 'rejected', updated_at = NOW()
      WHERE id = $1 AND (target_wallet = $2 OR initiator_wallet = $2) AND status IN ('pending', 'accepted')
      RETURNING *
    `, [tradeId, wallet]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found or not authorized" });
    }

    console.log(`[Trade] Trade #${tradeId} rejected by ${wallet?.substring(0, 8)}...`);

    return res.json({ ok: true, trade: rows[0] });
  } catch (err) {
    console.error("POST /api/trades/:id/reject error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/trades/:id/sign - Record signature for multi-party swap
app.post("/api/trades/:id/sign", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const tradeId = parseInt(req.params.id);

    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    // Get current trade
    const { rows: trades } = await pool.query(`SELECT * FROM trades WHERE id = $1`, [tradeId]);

    if (trades.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found" });
    }

    const trade = trades[0];

    if (trade.status !== 'accepted') {
      return res.status(400).json({ ok: false, error: "Trade must be accepted before signing" });
    }

    // Determine which party is signing
    let updateField = null;
    if (wallet === trade.initiator_wallet) {
      updateField = 'initiator_signed';
    } else if (wallet === trade.target_wallet) {
      updateField = 'target_signed';
    } else {
      return res.status(403).json({ ok: false, error: "Not authorized to sign this trade" });
    }

    // Update signature status
    const { rows: updated } = await pool.query(`
      UPDATE trades 
      SET ${updateField} = TRUE, updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `, [tradeId]);

    const updatedTrade = updated[0];

    // Check if both parties have signed
    if (updatedTrade.initiator_signed && updatedTrade.target_signed) {
      // Both signed - trade is ready for execution
      console.log(`[Trade] Trade #${tradeId} fully signed - ready for execution`);

      await pool.query(`
        UPDATE trades SET status = 'ready', updated_at = NOW() WHERE id = $1
      `, [tradeId]);

      updatedTrade.status = 'ready';
    }

    console.log(`[Trade] Trade #${tradeId} signed by ${wallet?.substring(0, 8)}... (${updateField})`);

    return res.json({ ok: true, trade: updatedTrade });
  } catch (err) {
    console.error("POST /api/trades/:id/sign error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/trades/:id/complete - Mark trade as completed with tx_id
app.post("/api/trades/:id/complete", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const tradeId = parseInt(req.params.id);
    const { txId } = req.body;

    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    const { rows } = await pool.query(`
      UPDATE trades 
      SET status = 'completed', tx_id = $2, updated_at = NOW()
      WHERE id = $1 AND (initiator_wallet = $3 OR target_wallet = $3)
      RETURNING *
    `, [tradeId, txId || null, wallet]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found or not authorized" });
    }

    console.log(`[Trade] Trade #${tradeId} completed with tx_id: ${txId || 'none'}`);

    return res.json({ ok: true, trade: rows[0] });
  } catch (err) {
    console.error("POST /api/trades/:id/complete error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ================== ADMIN ANALYTICS API ==================

// POST /api/trades/:id/execute - Execute trade (transfer NFTs on-chain)
app.post("/api/trades/:id/execute", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const tradeId = parseInt(req.params.id);
    const { txIds } = req.body; // Array of transaction IDs from on-chain transfers

    if (!tradeId) {
      return res.status(400).json({ ok: false, error: "Invalid trade ID" });
    }

    // Get trade to verify status
    const { rows: trades } = await pool.query(`SELECT * FROM trades WHERE id = $1`, [tradeId]);

    if (trades.length === 0) {
      return res.status(404).json({ ok: false, error: "Trade not found" });
    }

    const trade = trades[0];

    // Verify user is party to this trade
    const isInitiator = trade.initiator_wallet === wallet;
    const isTarget = trade.target_wallet === wallet;

    if (!isInitiator && !isTarget) {
      return res.status(403).json({ ok: false, error: "Not authorized for this trade" });
    }

    // Trade must be in 'ready' or 'executing' status
    if (trade.status !== 'ready' && trade.status !== 'executing') {
      return res.status(400).json({ ok: false, error: `Trade must be ready to execute (current status: ${trade.status})` });
    }

    // Update the appropriate party's execution status
    const txIdStr = Array.isArray(txIds) ? txIds.join(',') : (txIds || null);

    let updateQuery;
    if (isInitiator) {
      updateQuery = `
        UPDATE trades 
        SET initiator_executed = TRUE, 
            initiator_tx_id = $2,
            status = CASE WHEN target_executed = TRUE THEN 'completed' ELSE 'executing' END,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
      `;
    } else {
      updateQuery = `
        UPDATE trades 
        SET target_executed = TRUE, 
            target_tx_id = $2,
            status = CASE WHEN initiator_executed = TRUE THEN 'completed' ELSE 'executing' END,
            updated_at = NOW()
        WHERE id = $1
        RETURNING *
      `;
    }

    const { rows: updated } = await pool.query(updateQuery, [tradeId, txIdStr]);
    const updatedTrade = updated[0];

    const partyLabel = isInitiator ? 'initiator' : 'target';
    console.log(`[Trade] Trade #${tradeId} ${partyLabel} executed by ${wallet?.substring(0, 8)}... (txIds: ${txIdStr || 'none'})`);

    // Get partner info for response
    const partnerWallet = isInitiator ? trade.target_wallet : trade.initiator_wallet;
    const partnerExecuted = isInitiator ? updatedTrade.target_executed : updatedTrade.initiator_executed;

    return res.json({
      ok: true,
      trade: updatedTrade,
      yourExecuted: true,
      partnerExecuted,
      completed: updatedTrade.status === 'completed',
      message: updatedTrade.status === 'completed'
        ? "🎉 Trade completed! Both parties have executed their transfers."
        : `Your transfers complete! Waiting for ${partnerWallet?.substring(0, 10)}... to execute their side.`
    });
  } catch (err) {
    console.error("POST /api/trades/:id/execute error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Initialize listings and bundles tables
async function initListingsTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS listings (
        id SERIAL PRIMARY KEY,
        seller_wallet TEXT NOT NULL,
        nft_id TEXT NOT NULL UNIQUE,
        price_usd DECIMAL(10, 2) NOT NULL,
        status TEXT DEFAULT 'active',
        buyer_wallet TEXT,
        tx_id TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_listings_seller ON listings(seller_wallet)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status)`);

    console.log("✅ Listings table initialized");
  } catch (err) {
    console.error("Failed to initialize listings table:", err.message);
  }
}

async function initBundlesTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bundles (
        id SERIAL PRIMARY KEY,
        seller_wallet TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        nft_ids JSONB NOT NULL DEFAULT '[]',
        price_usd DECIMAL(10, 2) NOT NULL,
        status TEXT DEFAULT 'active',
        buyer_wallet TEXT,
        tx_id TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundles_seller ON bundles(seller_wallet)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundles_status ON bundles(status)`);

    console.log("✅ Bundles table initialized");
  } catch (err) {
    console.error("Failed to initialize bundles table:", err.message);
  }
}

// ================== MARKETPLACE LISTINGS API ==================

// GET /api/listings - Get all active listings
app.get("/api/listings", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT l.*, m.first_name, m.last_name, m.team_name, m.tier, m.serial_number, m.max_mint_size,
             p.display_name as seller_name
      FROM listings l
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = l.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = l.seller_wallet
      WHERE l.status = 'active'
      ORDER BY l.created_at DESC
      LIMIT 200
    `);

    return res.json({ ok: true, listings: rows });
  } catch (err) {
    console.error("GET /api/listings error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/my-listings - Get current user's listings
app.get("/api/my-listings", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    if (!wallet) {
      return res.json({ ok: true, listings: [] });
    }

    const { rows } = await pool.query(`
      SELECT l.*, m.first_name, m.last_name, m.team_name, m.tier, m.serial_number, m.max_mint_size
      FROM listings l
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = l.nft_id
      WHERE l.seller_wallet = $1 AND l.status = 'active'
      ORDER BY l.created_at DESC
    `, [wallet]);

    return res.json({ ok: true, listings: rows });
  } catch (err) {
    console.error("GET /api/my-listings error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/listings - Create new listing(s)
app.post("/api/listings", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "No wallet connected" });
    }

    const { items } = req.body; // Array of { nft_id, price_usd }

    if (!items || items.length === 0) {
      return res.status(400).json({ ok: false, error: "No items to list" });
    }

    const created = [];
    for (const item of items) {
      if (!item.nft_id || !item.price_usd || item.price_usd <= 0) {
        continue;
      }

      try {
        const { rows } = await pool.query(`
          INSERT INTO listings (seller_wallet, nft_id, price_usd, status)
          VALUES ($1, $2, $3, 'active')
          ON CONFLICT (nft_id) DO UPDATE SET 
            price_usd = EXCLUDED.price_usd,
            status = 'active',
            updated_at = NOW()
          RETURNING *
        `, [wallet, item.nft_id, item.price_usd]);

        if (rows.length > 0) {
          created.push(rows[0]);
        }
      } catch (itemErr) {
        console.warn(`Failed to create listing for ${item.nft_id}:`, itemErr.message);
      }
    }

    console.log(`[Listings] Created ${created.length} listings for ${wallet.substring(0, 8)}...`);

    return res.json({ ok: true, listings: created });
  } catch (err) {
    console.error("POST /api/listings error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE /api/listings/:id - Cancel a listing
app.delete("/api/listings/:id", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const listingId = parseInt(req.params.id);

    const { rows } = await pool.query(`
      UPDATE listings SET status = 'cancelled', updated_at = NOW()
      WHERE id = $1 AND seller_wallet = $2 AND status = 'active'
      RETURNING *
    `, [listingId, wallet]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Listing not found or not authorized" });
    }

    return res.json({ ok: true, listing: rows[0] });
  } catch (err) {
    console.error("DELETE /api/listings/:id error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ================== BUNDLES API ==================

// POST /api/listings/:id/buy - Purchase a listing (marks as pending purchase)
app.post("/api/listings/:id/buy", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const buyerWallet = user.default_wallet_address?.toLowerCase();
    if (!buyerWallet) {
      return res.status(400).json({ ok: false, error: "No wallet connected" });
    }

    const listingId = parseInt(req.params.id);

    // Get the listing first
    const { rows: listings } = await pool.query(`
      SELECT * FROM listings WHERE id = $1 AND status = 'active'
    `, [listingId]);

    if (listings.length === 0) {
      return res.status(404).json({ ok: false, error: "Listing not found or no longer available" });
    }

    const listing = listings[0];

    if (listing.seller_wallet === buyerWallet) {
      return res.status(400).json({ ok: false, error: "Cannot buy your own listing" });
    }

    // Mark as sold with buyer info
    const { rows } = await pool.query(`
      UPDATE listings 
      SET status = 'sold', buyer_wallet = $2, updated_at = NOW()
      WHERE id = $1 AND status = 'active'
      RETURNING *
    `, [listingId, buyerWallet]);

    if (rows.length === 0) {
      return res.status(400).json({ ok: false, error: "Listing already sold" });
    }

    console.log(`[Listings] Listing #${listingId} purchased by ${buyerWallet.substring(0, 8)}...`);

    return res.json({
      ok: true,
      listing: rows[0],
      message: "Purchase recorded! Contact seller to complete the transfer."
    });
  } catch (err) {
    console.error("POST /api/listings/:id/buy error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/bundles/:id/buy - Purchase a bundle
app.post("/api/bundles/:id/buy", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const buyerWallet = user.default_wallet_address?.toLowerCase();
    if (!buyerWallet) {
      return res.status(400).json({ ok: false, error: "No wallet connected" });
    }

    const bundleId = parseInt(req.params.id);

    // Get the bundle
    const { rows: bundles } = await pool.query(`
      SELECT * FROM bundles WHERE id = $1 AND status = 'active'
    `, [bundleId]);

    if (bundles.length === 0) {
      return res.status(404).json({ ok: false, error: "Bundle not found or no longer available" });
    }

    const bundle = bundles[0];

    if (bundle.seller_wallet === buyerWallet) {
      return res.status(400).json({ ok: false, error: "Cannot buy your own bundle" });
    }

    // Mark as sold
    const { rows } = await pool.query(`
      UPDATE bundles 
      SET status = 'sold', buyer_wallet = $2, updated_at = NOW()
      WHERE id = $1 AND status = 'active'
      RETURNING *
    `, [bundleId, buyerWallet]);

    if (rows.length === 0) {
      return res.status(400).json({ ok: false, error: "Bundle already sold" });
    }

    console.log(`[Bundles] Bundle #${bundleId} purchased by ${buyerWallet.substring(0, 8)}...`);

    return res.json({
      ok: true,
      bundle: rows[0],
      message: "Purchase recorded! Contact seller to complete the transfer."
    });
  } catch (err) {
    console.error("POST /api/bundles/:id/buy error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/bundles - Get all active bundles
app.get("/api/bundles", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT b.*, p.display_name as seller_name
      FROM bundles b
      LEFT JOIN wallet_profiles p ON p.wallet_address = b.seller_wallet
      WHERE b.status = 'active'
      ORDER BY b.created_at DESC
      LIMIT 100
    `);

    return res.json({ ok: true, bundles: rows });
  } catch (err) {
    console.error("GET /api/bundles error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/my-bundles - Get current user's bundles
app.get("/api/my-bundles", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    if (!wallet) {
      return res.json({ ok: true, bundles: [] });
    }

    const { rows } = await pool.query(`
      SELECT * FROM bundles
      WHERE seller_wallet = $1 AND status = 'active'
      ORDER BY created_at DESC
    `, [wallet]);

    return res.json({ ok: true, bundles: rows });
  } catch (err) {
    console.error("GET /api/my-bundles error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/bundles - Create a new bundle
app.post("/api/bundles", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    if (!wallet) {
      return res.status(400).json({ ok: false, error: "No wallet connected" });
    }

    const { title, description, nft_ids, price_usd } = req.body;

    if (!title) {
      return res.status(400).json({ ok: false, error: "Bundle title required" });
    }

    if (!nft_ids || nft_ids.length === 0) {
      return res.status(400).json({ ok: false, error: "At least one NFT required" });
    }

    if (!price_usd || price_usd <= 0) {
      return res.status(400).json({ ok: false, error: "Valid price required" });
    }

    const { rows } = await pool.query(`
      INSERT INTO bundles (seller_wallet, title, description, nft_ids, price_usd, status)
      VALUES ($1, $2, $3, $4, $5, 'active')
      RETURNING *
    `, [wallet, title, description || '', JSON.stringify(nft_ids), price_usd]);

    console.log(`[Bundles] Created bundle "${title}" with ${nft_ids.length} NFTs for ${wallet.substring(0, 8)}...`);

    return res.json({ ok: true, bundle: rows[0] });
  } catch (err) {
    console.error("POST /api/bundles error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// DELETE /api/bundles/:id - Cancel a bundle
app.delete("/api/bundles/:id", async (req, res) => {
  try {
    const user = req.session?.user;
    if (!user) {
      return res.status(401).json({ ok: false, error: "Not logged in" });
    }

    const wallet = user.default_wallet_address?.toLowerCase();
    const bundleId = parseInt(req.params.id);

    const { rows } = await pool.query(`
      UPDATE bundles SET status = 'cancelled', updated_at = NOW()
      WHERE id = $1 AND seller_wallet = $2 AND status = 'active'
      RETURNING *
    `, [bundleId, wallet]);

    if (rows.length === 0) {
      return res.status(404).json({ ok: false, error: "Bundle not found or not authorized" });
    }

    return res.json({ ok: true, bundle: rows[0] });
  } catch (err) {
    console.error("DELETE /api/bundles/:id error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

function isAdmin(wallet) {
  if (!wallet) return false;
  return ADMIN_WALLETS.includes(wallet.toLowerCase());
}

function requireAdmin(req, res) {
  const user = req.session?.user;
  if (!user) {
    res.status(401).json({ ok: false, error: "Not logged in" });
    return null;
  }
  const wallet = user.default_wallet_address;
  if (!isAdmin(wallet)) {
    res.status(403).json({ ok: false, error: "Admin access required" });
    return null;
  }
  return user;
}

app.get("/api/admin/analytics/overview", async (req, res) => {
  const admin = requireAdmin(req, res);
  if (!admin) return;

  try {
    const pageViewsResult = await pool.query(`
      SELECT 
        COUNT(*) as total_views,
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as views_24h,
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as views_7d,
        COUNT(DISTINCT session_id) as unique_sessions
      FROM analytics_page_views
    `);

    return res.json({ ok: true, overview: pageViewsResult.rows[0] });
  } catch (err) {
    console.error("GET /api/admin/analytics/overview error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/admin/analytics/pages", async (req, res) => {
  const admin = requireAdmin(req, res);
  if (!admin) return;

  try {
    const { rows } = await pool.query(`
      SELECT page_path, COUNT(*) as views, COUNT(DISTINCT session_id) as unique_visitors
      FROM analytics_page_views
      WHERE created_at > NOW() - INTERVAL '30 days'
      GROUP BY page_path
      ORDER BY views DESC
      LIMIT 50
    `);

    return res.json({ ok: true, pages: rows });
  } catch (err) {
    console.error("GET /api/admin/analytics/pages error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

app.get("/api/admin/analytics/sessions", async (req, res) => {
  const admin = requireAdmin(req, res);
  if (!admin) return;

  try {
    const { rows } = await pool.query(`
      SELECT session_id, wallet_address, started_at, last_activity, page_count
      FROM analytics_sessions
      WHERE last_activity > NOW() - INTERVAL '7 days'
      ORDER BY last_activity DESC
      LIMIT 100
    `);

    return res.json({ ok: true, sessions: rows });
  } catch (err) {
    console.error("GET /api/admin/analytics/sessions error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ================== VISIT COUNTER ==================

// Ensure visit counter table exists
async function initVisitCounterTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS site_visits (
        id SERIAL PRIMARY KEY,
        total_count INTEGER DEFAULT 0,
        last_updated TIMESTAMPTZ DEFAULT NOW()
      );
      INSERT INTO site_visits (id, total_count) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

      CREATE TABLE IF NOT EXISTS unique_visits_log (
        visitor_hash TEXT PRIMARY KEY,
        first_seen TIMESTAMPTZ DEFAULT NOW()
      );
    `);
  } catch (err) {
    console.error("Error initializing visit counter table:", err.message);
  }
}

// POST /api/visit - record a visit and return count
app.post("/api/visit", async (req, res) => {
  try {
    // Basic IP/UA hashing for "uniqueness"
    const crypto = require('crypto');
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
    const ua = req.headers['user-agent'] || 'unknown';
    const hash = crypto.createHash('sha256').update(ip + ua).digest('hex');

    // Attempt to log the unique visit
    const { rowCount } = await pool.query(`
      INSERT INTO unique_visits_log (visitor_hash) 
      VALUES ($1) 
      ON CONFLICT (visitor_hash) DO NOTHING
    `, [hash]);

    // If it was a new unique visit, increment the total count
    if (rowCount > 0) {
      await pool.query(`
        UPDATE site_visits 
        SET total_count = total_count + 1, last_updated = NOW() 
        WHERE id = 1
      `);
    }

    // Always return current total count
    const { rows } = await pool.query(`SELECT total_count FROM site_visits WHERE id = 1`);
    const count = rows[0]?.total_count || 0;
    return res.json({ ok: true, count });
  } catch (err) {
    console.error("POST /api/visit error:", err.message);
    return res.json({ ok: false, count: 0 });
  }
});

// GET /api/visit-count - get current count without incrementing
app.get("/api/visit-count", async (req, res) => {
  try {
    const { rows } = await pool.query(`SELECT total_count FROM site_visits WHERE id = 1`);
    const count = rows[0]?.total_count || 0;
    return res.json({ ok: true, count });
  } catch (err) {
    console.error("GET /api/visit-count error:", err.message);
    return res.json({ ok: false, count: 0 });
  }
});

// ================== SERVER START ==================

app.listen(port, async () => {
  console.log(`NFL ALL DAY collection viewer running on http://localhost:${port}`);

  // Initialize database tables
  await initTradesTable();
  await initListingsTable();
  await initBundlesTable();
  await initAnalyticsTables();
  await initVisitCounterTable();

  // Set up insights refresh after server starts
  setTimeout(() => {
    setupInsightsRefresh();
  }, 2000); // Wait 2 seconds for server to be fully ready

  // Initialize sniper system (loads from DB and starts watcher)
  initializeSniper();
});
