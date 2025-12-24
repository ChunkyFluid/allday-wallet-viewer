// Background sync service: Sync wallet holdings from Flow blockchain to Render database
// Uses Cadence for NFT ownership and Snowflake for locked status
// Run this periodically (e.g., every 5-15 minutes) to keep wallets synced

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import * as flowService from "../services/flow-blockchain.js";
import { createSnowflakeConnection, executeSnowflakeWithRetry, delay } from "./snowflake-utils.js";

dotenv.config();

const SYNC_BATCH_SIZE = 50; // Number of wallets to sync per batch
const SYNC_DELAY_MS = 1000; // Delay between batches (1 second)

/**
 * Get ALL NFTs for a wallet - both unlocked (in wallet) and locked (in NFTLocker)
 * Uses Cadence for unlocked NFTs and Snowflake for locked NFTs + acquired dates
 */
async function getAllWalletNFTs(walletAddress) {
  const wallet = walletAddress.toLowerCase();
  const allNFTs = new Map(); // nft_id -> { is_locked, last_event_ts (acquired date) }

  // 1. Get UNLOCKED NFTs from blockchain (NFTs currently in wallet)
  console.log(`[Sync] Getting unlocked NFTs from blockchain...`);
  let unlockedIds = [];
  try {
    unlockedIds = await flowService.getWalletNFTIds(walletAddress);
    for (const id of unlockedIds) {
      allNFTs.set(id.toString(), { is_locked: false, last_event_ts: null });
    }
    console.log(`[Sync] Found ${unlockedIds.length} unlocked NFTs from blockchain`);
  } catch (err) {
    console.error(`[Sync] Error getting unlocked NFTs:`, err.message);
  }

  // 2. Get LOCKED NFTs from Snowflake (NFTs in NFTLocker contract)
  console.log(`[Sync] Getting locked NFTs and acquired dates from Snowflake...`);
  try {
    const conn = await createSnowflakeConnection();

    // Query locked NFTs
    const lockedSql = `
      WITH my_locked_events AS (
        SELECT 
          EVENT_DATA:id::STRING as NFT_ID,
          BLOCK_TIMESTAMP
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
        AND LOWER(EVENT_DATA:to::STRING) = LOWER('${wallet}') 
        AND EVENT_TYPE = 'NFTLocked' 
        AND TX_SUCCEEDED = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
      ),
      my_unlocked_events AS (
        SELECT 
          EVENT_DATA:id::STRING as NFT_ID,
          BLOCK_TIMESTAMP
        FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
        WHERE EVENT_CONTRACT = 'A.b6f2481eba4df97b.NFTLocker' 
        AND LOWER(EVENT_DATA:from::STRING) = LOWER('${wallet}') 
        AND EVENT_TYPE = 'NFTUnlocked' 
        AND TX_SUCCEEDED = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT DESC) = 1
      )
      SELECT l.NFT_ID
      FROM my_locked_events l
      LEFT JOIN my_unlocked_events u ON l.NFT_ID = u.NFT_ID 
        AND u.BLOCK_TIMESTAMP >= l.BLOCK_TIMESTAMP
      WHERE u.NFT_ID IS NULL
    `;
    const lockedRows = await executeSnowflakeWithRetry(conn, lockedSql, { maxRetries: 2 });

    for (const row of lockedRows) {
      const nftId = row.NFT_ID.toString();
      allNFTs.set(nftId, { is_locked: true, last_event_ts: null });
    }
    console.log(`[Sync] Found ${lockedRows.length} locked NFTs`);

    // Query ACQUIRED dates (first deposit to wallet) for all NFTs
    const acquiredSql = `
      SELECT 
        EVENT_DATA:id::STRING as NFT_ID,
        BLOCK_TIMESTAMP as ACQUIRED_DATE
      FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS 
      WHERE EVENT_CONTRACT = 'A.e4cf4bdc1751c65d.AllDay'
        AND LOWER(EVENT_DATA:to::STRING) = LOWER('${wallet}')
        AND EVENT_TYPE = 'Deposit'
        AND TX_SUCCEEDED = true
      QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_DATA:id::STRING ORDER BY BLOCK_HEIGHT ASC) = 1
    `;
    const acquiredRows = await executeSnowflakeWithRetry(conn, acquiredSql, { maxRetries: 2 });

    // Update NFTs with their acquired dates
    for (const row of acquiredRows) {
      const nftId = row.NFT_ID.toString();
      if (allNFTs.has(nftId)) {
        const existing = allNFTs.get(nftId);
        existing.last_event_ts = new Date(row.ACQUIRED_DATE);
        allNFTs.set(nftId, existing);
      }
    }
    console.log(`[Sync] Got acquired dates for ${acquiredRows.length} NFTs`);

  } catch (err) {
    console.error(`[Sync] Error getting locked NFTs:`, err.message);
  }

  // Set default date for any NFTs without acquired date
  const now = new Date();
  for (const [nftId, data] of allNFTs.entries()) {
    if (!data.last_event_ts) {
      data.last_event_ts = now;
      allNFTs.set(nftId, data);
    }
  }

  return allNFTs;
}

/**
 * Sync a single wallet from blockchain to database
 */
async function syncWallet(walletAddress) {
  try {
    console.log(`[Sync] Syncing wallet ${walletAddress.substring(0, 8)}...`);

    // Get ALL NFTs - both unlocked (from blockchain) and locked (from Snowflake)
    const allNFTs = await getAllWalletNFTs(walletAddress);

    if (allNFTs.size === 0) {
      // Wallet appears empty from blockchain/Snowflake - but NEVER delete locked NFTs!

      // Get current locked count
      const currentLocked = await pgQuery(
        `SELECT COUNT(*) as count FROM holdings WHERE wallet_address = $1 AND is_locked = true`,
        [walletAddress.toLowerCase()]
      );
      const lockedCount = parseInt(currentLocked.rows[0].count);

      // Only delete the UNLOCKED NFTs, keep the locked ones
      const deleteResult = await pgQuery(
        `DELETE FROM holdings WHERE wallet_address = $1 AND (is_locked IS NOT TRUE)`,
        [walletAddress.toLowerCase()]
      );

      if (deleteResult.rowCount > 0) {
        console.log(`[Sync] ✅ Removed ${deleteResult.rowCount} unlocked holdings for wallet ${walletAddress.substring(0, 8)}..., preserved ${lockedCount} locked`);
      } else if (lockedCount > 0) {
        console.log(`[Sync] ⚠️ Wallet ${walletAddress.substring(0, 10)}... appears empty but has ${lockedCount} locked NFTs - preserving them`);
      }
      return { added: 0, removed: deleteResult.rowCount, current: lockedCount, locked: lockedCount };
    }

    const nftIdStrings = Array.from(allNFTs.keys());
    let lockedCount = Array.from(allNFTs.values()).filter(v => v.is_locked).length;

    // Get current holdings from database
    const currentResult = await pgQuery(
      `SELECT nft_id, is_locked FROM holdings WHERE wallet_address = $1`,
      [walletAddress.toLowerCase()]
    );

    const currentNftIds = new Set(currentResult.rows.map(r => r.nft_id));
    const newNftIds = new Set(nftIdStrings);

    // Find NFTs to add
    const toAdd = nftIdStrings.filter(id => !currentNftIds.has(id));

    // Find NFTs to remove - SAFEGUARD:
    // Only remove NFTs that are NOT locked. If it's locked, we never delete it
    // because the Snowflake query might have missed it.
    const toRemove = Array.from(currentNftIds).filter(id => {
      const isStillInCollection = newNftIds.has(id);
      if (isStillInCollection) return false;

      const wasLockedInDb = currentResult.rows.find(r => r.nft_id === id)?.is_locked === true;
      if (wasLockedInDb) {
        // console.log(`[Sync] ⚠️ Preserving locked NFT ${id} - not in new sync but keeping in DB`);
        return false;
      }

      return true; // Safe to remove unlocked NFT that is no longer in collection
    });


    // Find NFTs where locked status changed
    const toUpdateLocked = nftIdStrings.filter(id => {
      if (currentNftIds.has(id)) {
        const currentRow = currentResult.rows.find(r => r.nft_id === id);
        const newLockedStatus = allNFTs.get(id)?.is_locked || false;
        return currentRow && currentRow.is_locked !== newLockedStatus;
      }
      return false;
    });

    // Add new NFTs with locked status (in batches of 100 to avoid param limit)
    if (toAdd.length > 0) {
      const BATCH_SIZE = 100;
      for (let i = 0; i < toAdd.length; i += BATCH_SIZE) {
        const batch = toAdd.slice(i, i + BATCH_SIZE);
        const values = batch.map((nftId, idx) =>
          `($${idx * 5 + 1}, $${idx * 5 + 2}, $${idx * 5 + 3}, $${idx * 5 + 4}, $${idx * 5 + 5})`
        ).join(', ');

        const params = batch.flatMap(nftId => {
          const nftData = allNFTs.get(nftId) || { is_locked: false, last_event_ts: new Date() };
          return [
            walletAddress.toLowerCase(),
            nftId,
            nftData.is_locked,
            nftData.last_event_ts,
            new Date()
          ];
        });

        await pgQuery(
          `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
           VALUES ${values}
           ON CONFLICT (wallet_address, nft_id) 
           DO UPDATE SET 
             is_locked = EXCLUDED.is_locked,
             acquired_at = EXCLUDED.acquired_at,
             last_synced_at = NOW()`,
          params
        );
      }
    }

    // Update locked status for existing NFTs that changed
    if (toUpdateLocked.length > 0) {
      for (const nftId of toUpdateLocked) {
        const nftData = allNFTs.get(nftId) || { is_locked: false };
        await pgQuery(
          `UPDATE holdings 
           SET is_locked = $1, last_synced_at = NOW()
           WHERE wallet_address = $2 AND nft_id = $3`,
          [nftData.is_locked, walletAddress.toLowerCase(), nftId]
        );
      }
    }

    // Update last_synced_at for existing NFTs - REMOVED because holdings table has no last_synced_at
    /*
    const existingNftIds = nftIdStrings.filter(id => currentNftIds.has(id) && !toUpdateLocked.includes(id));
    if (existingNftIds.length > 0) {
      await pgQuery(
        `UPDATE wallet_holdings 
         SET last_synced_at = NOW()
         WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
        [walletAddress.toLowerCase(), existingNftIds]
      );
    }
    */

    // Remove NFTs that are no longer in the wallet
    if (toRemove.length > 0) {
      await pgQuery(
        `DELETE FROM holdings 
         WHERE wallet_address = $1 AND nft_id = ANY($2::text[]) AND (is_locked IS NOT TRUE)`,
        [walletAddress.toLowerCase(), toRemove]
      );
    }

    if (toAdd.length > 0 || toRemove.length > 0 || toUpdateLocked.length > 0) {
      console.log(`[Sync] ✅ Wallet ${walletAddress.substring(0, 8)}... - Added: ${toAdd.length}, Removed: ${toRemove.length}, Locked Updated: ${toUpdateLocked.length}, Current: ${allNFTs.size}, Locked: ${lockedCount}`);
    } else {
      console.log(`[Sync] ✅ Wallet ${walletAddress.substring(0, 8)}... - No changes (${allNFTs.size} NFTs, ${lockedCount} locked)`);
    }

    return {
      added: toAdd.length,
      removed: toRemove.length,
      lockedUpdated: toUpdateLocked.length,
      current: allNFTs.size,
      locked: lockedCount
    };
  } catch (err) {
    console.error(`[Sync] ❌ Error syncing wallet ${walletAddress.substring(0, 8)}...:`, err.message);
    throw err;
  }
}

/**
 * Sync all wallets that have been queried recently
 */
async function syncRecentWallets() {
  try {
    console.log("[Sync] Starting sync of recently queried wallets...");

    const recentWalletsResult = await pgQuery(
      `SELECT DISTINCT wallet_address
       FROM holdings 
       WHERE acquired_at > NOW() - INTERVAL '24 hours'
       GROUP BY wallet_address
       ORDER BY MAX(acquired_at) DESC
       LIMIT 1000`
    );

    const wallets = recentWalletsResult.rows.map(r => r.wallet_address);

    if (wallets.length === 0) {
      console.log("[Sync] No recent wallets to sync");
      return { synced: 0, total: 0 };
    }

    console.log(`[Sync] Found ${wallets.length} recent wallets to sync`);
    return await syncSpecificWallets(wallets);
  } catch (err) {
    console.error("[Sync] ❌ Fatal error:", err);
    throw err;
  }
}

/**
 * Sync specific wallets (passed as command line arguments)
 */
async function syncSpecificWallets(walletAddresses) {
  console.log(`[Sync] Syncing ${walletAddresses.length} specific wallets...`);

  let synced = 0;
  let errors = 0;

  for (const wallet of walletAddresses) {
    try {
      await syncWallet(wallet);
      synced++;
    } catch (err) {
      errors++;
      console.error(`[Sync] Failed to sync wallet ${wallet}:`, err.message);
    }

    // Small delay between wallets
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  console.log(`[Sync] ✅ Completed: ${synced} wallets synced, ${errors} errors`);
  return { synced, errors };
}

/**
 * Sync wallets that haven't been synced recently
 */
async function syncStaleWallets() {
  try {
    console.log("[Sync] Starting sync of stale wallets...");

    const staleWalletsResult = await pgQuery(
      `SELECT DISTINCT wallet_address, MIN(last_synced_at) as last_synced_at
       FROM holdings 
       WHERE last_synced_at < NOW() - INTERVAL '1 hour'
          OR last_synced_at IS NULL
       GROUP BY wallet_address
       ORDER BY MIN(last_synced_at) NULLS FIRST
       LIMIT 500`
    );

    const wallets = staleWalletsResult.rows.map(r => r.wallet_address);

    if (wallets.length === 0) {
      console.log("[Sync] No stale wallets to sync");
      return { synced: 0, total: 0 };
    }

    console.log(`[Sync] Found ${wallets.length} stale wallets to sync`);
    return await syncSpecificWallets(wallets);
  } catch (err) {
    console.error("[Sync] ❌ Fatal error:", err);
    throw err;
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);

  if (args.length > 0 && !args[0].startsWith('--')) {
    // Sync specific wallets from command line
    await syncSpecificWallets(args);
  } else if (args.includes('--stale')) {
    // Sync stale wallets
    await syncStaleWallets();
  } else {
    // Default: sync recent wallets
    await syncRecentWallets();
  }
}

// Run if executed directly
import { fileURLToPath } from 'url';
import path from 'path';

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);

if (isMain) {
  main()
    .then(() => {
      console.log("[Sync] ✅ Sync complete");
      process.exit(0);
    })
    .catch((err) => {
      console.error("[Sync] 💥 Fatal error:", err);
      process.exit(1);
    });
}

export { syncWallet, syncRecentWallets, syncStaleWallets, syncSpecificWallets };
