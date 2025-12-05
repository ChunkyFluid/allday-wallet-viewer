// Flow Blockchain Service
// Direct integration with Flow blockchain using Cadence scripts
// Replaces Snowflake dependency for wallet holdings queries

import * as fcl from "@onflow/fcl";
import * as t from "@onflow/types";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configure FCL for Flow mainnet
fcl.config()
  .put("accessNode.api", process.env.FLOW_ACCESS_NODE || "https://rest-mainnet.onflow.org")
  .put("flow.network", "mainnet")
  .put("app.detail.title", "NFL All Day Wallet Viewer")
  .put("app.detail.icon", "https://allday.com/favicon.ico");

// NFL All Day contract address
const ALLDAY_CONTRACT = "0xe4cf4bdc1751c65d";

// Load Cadence scripts
function loadCadenceScript(filename) {
  const scriptPath = path.join(__dirname, "..", "cadence", "scripts", filename);
  try {
    return fs.readFileSync(scriptPath, "utf8");
  } catch (err) {
    console.error(`Failed to load Cadence script ${filename}:`, err);
    throw err;
  }
}

/**
 * Get all NFT IDs owned by a wallet address
 * @param {string} walletAddress - Flow wallet address (0x...)
 * @returns {Promise<number[]>} Array of NFT IDs
 */
export async function getWalletNFTIds(walletAddress) {
  try {
    // Validate address format
    if (!walletAddress || !walletAddress.startsWith("0x")) {
      throw new Error("Invalid wallet address format");
    }

    const cadence = loadCadenceScript("get_wallet_nft_ids.cdc");
    
    const result = await fcl.query({
      cadence: cadence,
      args: (arg, t) => [
        arg(walletAddress, t.Address)
      ]
    });

    // Result is an array of UInt64, convert to numbers
    return Array.isArray(result) ? result.map(id => Number(id)) : [];
  } catch (err) {
    console.error(`[Flow] Error getting wallet NFT IDs for ${walletAddress}:`, err);
    throw err;
  }
}

/**
 * Get metadata for a specific NFT
 * @param {string} walletAddress - Wallet address that owns the NFT
 * @param {number} nftId - NFT ID
 * @returns {Promise<Object|null>} NFT metadata or null if not found
 */
export async function getNFTMetadata(walletAddress, nftId) {
  try {
    const cadence = loadCadenceScript("get_nft_metadata.cdc");
    
    const result = await fcl.query({
      cadence: cadence,
      args: (arg, t) => [
        arg(walletAddress, t.Address),
        arg(nftId.toString(), t.UInt64)
      ]
    });

    return result || null;
  } catch (err) {
    console.error(`[Flow] Error getting NFT metadata for ${nftId}:`, err);
    return null;
  }
}

/**
 * Get full details for a specific NFT
 * @param {string} walletAddress - Wallet address that owns the NFT
 * @param {number} nftId - NFT ID
 * @returns {Promise<Object|null>} Full NFT details or null
 */
export async function getNFTFullDetails(walletAddress, nftId) {
  try {
    const cadence = loadCadenceScript("get_nft_full_details.cdc");
    
    const result = await fcl.query({
      cadence: cadence,
      args: (arg, t) => [
        arg(walletAddress, t.Address),
        arg(nftId.toString(), t.UInt64)
      ]
    });

    return result || null;
  } catch (err) {
    console.error(`[Flow] Error getting NFT full details for ${nftId}:`, err);
    return null;
  }
}

/**
 * Get all NFTs with metadata for a wallet (batch operation)
 * This is more efficient than querying each NFT individually
 * @param {string} walletAddress - Wallet address
 * @returns {Promise<Array>} Array of NFT objects with metadata
 */
export async function getWalletNFTsWithMetadata(walletAddress) {
  try {
    // First get all NFT IDs
    const nftIds = await getWalletNFTIds(walletAddress);
    
    if (nftIds.length === 0) {
      return [];
    }

    // For now, we'll get basic info and rely on database for full metadata
    // Full metadata queries can be slow, so we batch them or use database cache
    const nfts = nftIds.map(nftId => ({
      nft_id: nftId.toString(),
      wallet_address: walletAddress.toLowerCase()
    }));

    return nfts;
  } catch (err) {
    console.error(`[Flow] Error getting wallet NFTs with metadata:`, err);
    throw err;
  }
}

/**
 * Check if a wallet has the AllDay collection set up
 * @param {string} walletAddress - Wallet address
 * @returns {Promise<boolean>} True if collection exists
 */
export async function hasAllDayCollection(walletAddress) {
  try {
    const nftIds = await getWalletNFTIds(walletAddress);
    // If we can query it, the collection exists
    return true;
  } catch (err) {
    // If it fails with "Collection not found", return false
    if (err.message && err.message.includes("Collection not found")) {
      return false;
    }
    // Other errors, re-throw
    throw err;
  }
}

/**
 * Get wallet NFT count (faster than getting all IDs)
 * @param {string} walletAddress - Wallet address
 * @returns {Promise<number>} Number of NFTs owned
 */
export async function getWalletNFTCount(walletAddress) {
  try {
    const nftIds = await getWalletNFTIds(walletAddress);
    return nftIds.length;
  } catch (err) {
    console.error(`[Flow] Error getting wallet NFT count:`, err);
    return 0;
  }
}

/**
 * Get locked status for NFTs by querying Flow blockchain events
 * This queries NFTLocker contract events directly from Flow REST API
 * @param {string[]} nftIds - Array of NFT IDs as strings
 * @returns {Promise<Map<string, boolean>>} Map of nft_id -> is_locked
 */
export async function getLockedStatus(nftIds) {
  if (!nftIds || nftIds.length === 0) {
    return new Map();
  }

  try {
    const accessNode = process.env.FLOW_ACCESS_NODE || "https://rest-mainnet.onflow.org";
    const NFT_LOCKER_CONTRACT = "A.b6f2481eba4df97b.NFTLocker";
    
    // For now, use database as source of truth for locked status
    // Flow events API requires block heights and is complex for historical data
    // We'll update locked status incrementally as events come in via WebSocket
    // For initial sync, use database which has been populated by Snowflake
    
    // TODO: In the future, we can query Flow events API with proper block ranges
    // For now, return empty map to preserve existing database values
    console.log(`[Flow] Using database for locked status (Flow events API requires block height ranges)`);
    return new Map();

    // Build map of NFT ID -> latest lock/unlock timestamp
    const lockMap = new Map(); // nft_id -> { lockedAt, unlockedAt }
    
    // Process locked events - extract NFT ID from event payload
    for (const event of lockedEvents) {
      try {
        // Event payload structure: { id: { value: "123" }, to: { value: "0x..." } }
        const payload = event.payload || {};
        const fields = payload.fields || [];
        const idField = fields.find(f => f.name === "id");
        const nftId = idField?.value?.value || payload.id?.value;
        
        if (nftId && nftIds.includes(nftId.toString())) {
          const nftIdStr = nftId.toString();
          const timestamp = new Date(event.blockTimestamp).getTime();
          const current = lockMap.get(nftIdStr) || { lockedAt: null, unlockedAt: null };
          if (!current.lockedAt || current.lockedAt < timestamp) {
            lockMap.set(nftIdStr, {
              lockedAt: timestamp,
              unlockedAt: current.unlockedAt
            });
          }
        }
      } catch (e) {
        // Skip malformed events
        continue;
      }
    }

    // Process unlocked events
    for (const event of unlockedEvents) {
      try {
        const payload = event.payload || {};
        const fields = payload.fields || [];
        const idField = fields.find(f => f.name === "id");
        const nftId = idField?.value?.value || payload.id?.value;
        
        if (nftId && nftIds.includes(nftId.toString())) {
          const nftIdStr = nftId.toString();
          const timestamp = new Date(event.blockTimestamp).getTime();
          const current = lockMap.get(nftIdStr) || { lockedAt: null, unlockedAt: null };
          if (!current.unlockedAt || current.unlockedAt < timestamp) {
            lockMap.set(nftIdStr, {
              lockedAt: current.lockedAt,
              unlockedAt: timestamp
            });
          }
        }
      } catch (e) {
        continue;
      }
    }

    // Determine final locked status: locked if latest event is lock (or no unlock after lock)
    const result = new Map();
    for (const nftId of nftIds) {
      const nftIdStr = nftId.toString();
      const status = lockMap.get(nftIdStr);
      if (status) {
        // If there's a lock event and either no unlock or unlock is before lock
        const isLocked = status.lockedAt && (!status.unlockedAt || status.unlockedAt < status.lockedAt);
        result.set(nftIdStr, isLocked);
      } else {
        result.set(nftIdStr, false);
      }
    }

    const lockedCount = Array.from(result.values()).filter(v => v === true).length;
    if (lockedCount > 0) {
      console.log(`[Flow] Found ${lockedCount} locked NFTs from blockchain events`);
    }
    
    return result;
  } catch (err) {
    console.error(`[Flow] Error getting locked status:`, err.message);
    // Return all false on error (assume unlocked)
    const result = new Map();
    for (const nftId of nftIds) {
      result.set(nftId.toString(), false);
    }
    return result;
  }
}

// Export for use in other modules
export default {
  getWalletNFTIds,
  getNFTMetadata,
  getNFTFullDetails,
  getWalletNFTsWithMetadata,
  hasAllDayCollection,
  getWalletNFTCount,
  getLockedStatus
};

