// Blockchain-only wallet sync script
// Syncs wallet holdings directly from Flow blockchain - NO SNOWFLAKE REQUIRED
// Uses:
//   - Flow blockchain for unlocked NFTs (in wallet)
//   - NFTLocker contract for locked NFTs
//   - Local Postgres for metadata (already synced)

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import * as flowService from "../services/flow-blockchain.js";

dotenv.config();

const SYNC_BATCH_SIZE = 20; // Number of wallets to sync in parallel
const SYNC_DELAY_MS = 100; // Delay between batches to avoid rate limiting

/**
 * Get ALL NFTs for a wallet from blockchain only (no Snowflake)
 * Falls back to existing database locked status if blockchain query fails
 * @param {string} walletAddress - Flow wallet address
 * @param {Map} existingLockedStatus - Map of nft_id -> is_locked from database
 * @returns {Promise<Map<string, {is_locked: boolean}>>} Map of nft_id -> status
 */
async function getWalletNFTsFromBlockchain(walletAddress, existingLockedStatus = new Map()) {
  const wallet = walletAddress.toLowerCase();
  const allNFTs = new Map();

  try {
    // Get unlocked NFTs from wallet (these are the NFTs in the user's collection)
    const unlocked = await flowService.getWalletNFTIds(walletAddress);

    // Try to get locked NFTs from NFTLocker contract
    let locked = [];
    let lockerQuerySucceeded = false;
    try {
      locked = await flowService.getLockedNFTIds(walletAddress);
      lockerQuerySucceeded = true; // Query worked (even if result is empty)
    } catch (lockErr) {
      // NFTLocker query failed - we'll use existing database values as fallback
      console.log(`[Blockchain Sync] NFTLocker query failed for ${walletAddress.substring(0, 10)}...: ${lockErr.message}`);
    }

    // Build a set of locked NFT IDs for quick lookup
    const lockedSet = new Set(locked.map(id => id.toString()));

    // Add unlocked NFTs (from wallet collection)
    for (const id of unlocked) {
      const idStr = id.toString();
      // NFTs in the wallet are unlocked
      allNFTs.set(idStr, { is_locked: false });
    }

    // Add locked NFTs (from NFTLocker contract)
    for (const id of locked) {
      allNFTs.set(id.toString(), { is_locked: true });
    }

    // If NFTLocker query failed, check existing database for any locked NFTs
    // that we should preserve (they're not in the wallet, so they might still be locked)
    if (!lockerQuerySucceeded) {
      for (const [nftId, wasLocked] of existingLockedStatus) {
        if (wasLocked && !allNFTs.has(nftId)) {
          // This NFT was locked in DB and not in wallet - keep it as locked
          allNFTs.set(nftId, { is_locked: true });
        }
      }
    }

    const unlockedCount = Array.from(allNFTs.values()).filter(v => !v.is_locked).length;
    const lockedCount = Array.from(allNFTs.values()).filter(v => v.is_locked).length;
    console.log(`[Blockchain Sync] ${walletAddress.substring(0, 10)}... - ${unlockedCount} unlocked, ${lockedCount} locked (${allNFTs.size} total)`);

    return allNFTs;
  } catch (err) {
    console.error(`[Blockchain Sync] Error for ${walletAddress.substring(0, 10)}...:`, err.message);
    throw err;
  }
}

/**
 * Sync a single wallet from blockchain to database
 * @param {string} walletAddress - Flow wallet address
 */
async function syncWallet(walletAddress) {
  try {
    const walletLower = walletAddress.toLowerCase();

    // Get current holdings from database first (to preserve locked status if needed)
    const currentResult = await pgQuery(
      `SELECT nft_id, is_locked FROM holdings WHERE wallet_address = $1`,
      [walletLower]
    );

    // Build map of existing locked status
    const existingLockedStatus = new Map();
    for (const row of currentResult.rows) {
      existingLockedStatus.set(row.nft_id, row.is_locked);
    }

    // Get ALL NFTs from blockchain (with fallback to existing locked status)
    const allNFTs = await getWalletNFTsFromBlockchain(walletAddress, existingLockedStatus);

    if (allNFTs.size === 0) {
      // Wallet appears empty from blockchain - but NEVER delete locked NFTs!
      // They might still be locked, we just can't query them from the blockchain.
      const lockedCount = Array.from(existingLockedStatus.values()).filter(v => v === true).length;

      // Only delete the UNLOCKED NFTs, keep the locked ones
      const deleteResult = await pgQuery(
        `DELETE FROM holdings WHERE wallet_address = $1 AND (is_locked IS NOT TRUE)`,
        [walletLower]
      );

      if (deleteResult.rowCount > 0) {
        console.log(`[Blockchain Sync] ✅ Removed ${deleteResult.rowCount} unlocked holdings, preserved ${lockedCount} locked`);
      } else if (lockedCount > 0) {
        console.log(`[Blockchain Sync] ⚠️ Wallet ${walletAddress.substring(0, 10)}... appears empty but has ${lockedCount} locked NFTs - preserving them`);
      }
      return { added: 0, removed: deleteResult.rowCount || 0, current: lockedCount };
    }

    const nftIdStrings = Array.from(allNFTs.keys());
    const lockedCount = Array.from(allNFTs.values()).filter(v => v.is_locked).length;

    // Already have currentResult from above
    const currentNftIds = new Set(currentResult.rows.map(r => r.nft_id));
    const newNftIds = new Set(nftIdStrings);

    // Find NFTs to add
    const toAdd = nftIdStrings.filter(id => !currentNftIds.has(id));

    // Find NFTs to remove - CRITICAL SAFEGUARD:
    // NEVER remove NFTs that were previously marked as locked!
    // Only remove NFTs that were unlocked AND are no longer in the wallet's blockchain collection.
    // This prevents data loss when the locked NFT query fails or returns incomplete data.
    const toRemove = Array.from(currentNftIds).filter(id => {
      if (newNftIds.has(id)) return false; // Still in wallet, don't remove

      // Check if this NFT was locked in the database
      const wasLocked = existingLockedStatus.get(id) === true;
      if (wasLocked) {
        // NEVER delete locked NFTs - they might still be locked, we just can't query them
        console.log(`[Blockchain Sync] ⚠️ Preserving locked NFT ${id} - not deleting`);
        return false;
      }

      return true; // Was unlocked and no longer in wallet - safe to remove
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

    // Add new NFTs (in batches)
    if (toAdd.length > 0) {
      const BATCH_SIZE = 100;
      for (let i = 0; i < toAdd.length; i += BATCH_SIZE) {
        const batch = toAdd.slice(i, i + BATCH_SIZE);
        const values = batch.map((_, idx) =>
          `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, NOW(), NOW())`
        ).join(', ');

        const params = batch.flatMap(nftId => {
          const nftData = allNFTs.get(nftId) || { is_locked: false };
          return [walletLower, nftId, nftData.is_locked];
        });

        await pgQuery(
          `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
           VALUES ${values}
           ON CONFLICT (wallet_address, nft_id) 
           DO UPDATE SET 
             is_locked = EXCLUDED.is_locked,
             last_synced_at = NOW()`,
          params
        );
      }
    }

    // Update locked status for changed NFTs
    if (toUpdateLocked.length > 0) {
      for (const nftId of toUpdateLocked) {
        const nftData = allNFTs.get(nftId) || { is_locked: false };
        await pgQuery(
          `UPDATE holdings 
           SET is_locked = $1, last_synced_at = NOW()
           WHERE wallet_address = $2 AND nft_id = $3`,
          [nftData.is_locked, walletLower, nftId]
        );
      }
    }

    // Update last_synced_at for unchanged NFTs
    const unchanged = nftIdStrings.filter(id =>
      currentNftIds.has(id) && !toUpdateLocked.includes(id)
    );
    if (unchanged.length > 0) {
      await pgQuery(
        `UPDATE holdings 
         SET last_synced_at = NOW()
         WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
        [walletLower, unchanged]
      );
    }

    // Remove NFTs no longer in wallet
    if (toRemove.length > 0) {
      await pgQuery(
        `DELETE FROM holdings 
         WHERE wallet_address = $1 AND nft_id = ANY($2::text[]) AND (is_locked IS NOT TRUE)`,
        [walletLower, toRemove]
      );
    }

    const stats = {
      added: toAdd.length,
      removed: toRemove.length,
      updated: toUpdateLocked.length,
      current: allNFTs.size,
      locked: lockedCount
    };

    if (toAdd.length > 0 || toRemove.length > 0 || toUpdateLocked.length > 0) {
      console.log(`[Blockchain Sync] ✅ ${walletAddress.substring(0, 10)}... - +${toAdd.length} -${toRemove.length} ~${toUpdateLocked.length} (${allNFTs.size} total, ${lockedCount} locked)`);
    }

    return stats;
  } catch (err) {
    console.error(`[Blockchain Sync] ❌ Error syncing ${walletAddress.substring(0, 10)}...:`, err.message);
    throw err;
  }
}

/**
 * Sync specific wallets (PARALLEL VERSION)
 */
async function syncSpecificWallets(walletAddresses) {
  console.log(`[Blockchain Sync] Syncing ${walletAddresses.length} wallets from blockchain (${SYNC_BATCH_SIZE} concurrent)...`);

  let synced = 0;
  let errors = 0;
  let totalAdded = 0;
  let totalRemoved = 0;

  // Process wallets in parallel batches
  for (let i = 0; i < walletAddresses.length; i += SYNC_BATCH_SIZE) {
    const batch = walletAddresses.slice(i, i + SYNC_BATCH_SIZE);

    const results = await Promise.allSettled(batch.map(async (wallet) => {
      try {
        return await syncWallet(wallet);
      } catch (err) {
        throw err;
      }
    }));

    // Process results
    for (const result of results) {
      if (result.status === 'fulfilled') {
        synced++;
        totalAdded += result.value.added || 0;
        totalRemoved += result.value.removed || 0;
      } else {
        errors++;
      }
    }

    // Progress update every 100 wallets
    if ((i + batch.length) % 100 === 0 || i + batch.length === walletAddresses.length) {
      const pct = ((i + batch.length) / walletAddresses.length * 100).toFixed(1);
      console.log(`[Blockchain Sync] Progress: ${i + batch.length}/${walletAddresses.length} (${pct}%) - +${totalAdded} -${totalRemoved}`);
    }

    // Small delay between batches to avoid rate limiting
    if (SYNC_DELAY_MS > 0 && i + SYNC_BATCH_SIZE < walletAddresses.length) {
      await new Promise(resolve => setTimeout(resolve, SYNC_DELAY_MS));
    }
  }

  console.log(`[Blockchain Sync] ✅ Completed: ${synced} wallets synced, ${errors} errors, +${totalAdded} -${totalRemoved} NFTs`);
  return { synced, errors, totalAdded, totalRemoved };
}

/**
 * Sync all wallets that have been recently active
 */
async function syncRecentWallets() {
  console.log("[Blockchain Sync] Finding recently active wallets...");

  const result = await pgQuery(
    `SELECT DISTINCT wallet_address 
     FROM holdings 
     WHERE last_synced_at > NOW() - INTERVAL '24 hours'
     ORDER BY wallet_address
     LIMIT 500`
  );

  const wallets = result.rows.map(r => r.wallet_address);

  if (wallets.length === 0) {
    console.log("[Blockchain Sync] No recent wallets to sync");
    return { synced: 0, errors: 0 };
  }

  console.log(`[Blockchain Sync] Found ${wallets.length} recent wallets`);
  return await syncSpecificWallets(wallets);
}

/**
 * Sync all wallets with stale data
 */
async function syncStaleWallets() {
  console.log("[Blockchain Sync] Finding stale wallets...");

  const result = await pgQuery(
    `SELECT wallet_address, MIN(last_synced_at) as oldest_sync
     FROM holdings 
     WHERE last_synced_at < NOW() - INTERVAL '1 hour'
        OR last_synced_at IS NULL
     GROUP BY wallet_address
     ORDER BY MIN(last_synced_at) NULLS FIRST
     LIMIT 200`
  );

  const wallets = result.rows.map(r => r.wallet_address);

  if (wallets.length === 0) {
    console.log("[Blockchain Sync] No stale wallets to sync");
    return { synced: 0, errors: 0 };
  }

  console.log(`[Blockchain Sync] Found ${wallets.length} stale wallets`);
  return await syncSpecificWallets(wallets);
}

/**
 * Sync ALL wallets in database (full refresh)
 */
async function syncAllWallets() {
  console.log("[Blockchain Sync] Getting all wallets from database...");

  const result = await pgQuery(
    `SELECT DISTINCT wallet_address FROM holdings ORDER BY wallet_address`
  );

  const wallets = result.rows.map(r => r.wallet_address);

  if (wallets.length === 0) {
    console.log("[Blockchain Sync] No wallets to sync");
    return { synced: 0, errors: 0 };
  }

  console.log(`[Blockchain Sync] Syncing ALL ${wallets.length} wallets...`);
  return await syncSpecificWallets(wallets);
}

// Main execution
async function main() {
  const args = process.argv.slice(2);

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  BLOCKCHAIN-ONLY WALLET SYNC (No Snowflake Required)");
  console.log("═══════════════════════════════════════════════════════════\n");

  if (args.includes('--all')) {
    // Sync ALL wallets
    await syncAllWallets();
  } else if (args.includes('--stale')) {
    // Sync stale wallets
    await syncStaleWallets();
  } else if (args.includes('--recent')) {
    // Sync recent wallets
    await syncRecentWallets();
  } else if (args.length > 0 && !args[0].startsWith('--')) {
    // Sync specific wallets from command line
    await syncSpecificWallets(args);
  } else {
    // Default: sync stale wallets
    console.log("Usage:");
    console.log("  node sync_wallets_from_blockchain_only.js <wallet1> <wallet2> ...  - Sync specific wallets");
    console.log("  node sync_wallets_from_blockchain_only.js --recent                 - Sync recently active wallets");
    console.log("  node sync_wallets_from_blockchain_only.js --stale                  - Sync stale wallets");
    console.log("  node sync_wallets_from_blockchain_only.js --all                    - Sync ALL wallets");
    console.log("");
    console.log("Running --stale by default...\n");
    await syncStaleWallets();
  }
}

// Run if executed directly
main()
  .then(() => {
    console.log("\n[Blockchain Sync] ✅ Complete!");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n[Blockchain Sync] 💥 Fatal error:", err);
    process.exit(1);
  });

export { syncWallet, syncSpecificWallets, syncRecentWallets, syncStaleWallets, syncAllWallets };
