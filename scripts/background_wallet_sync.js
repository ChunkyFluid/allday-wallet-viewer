// Background wallet sync service
// Runs continuously, syncing wallets from blockchain to database
// Keeps the database up-to-date for fast queries

import * as dotenv from "dotenv";
import { syncRecentWallets, syncStaleWallets } from "./sync_wallets_from_blockchain.js";
import { syncLeaderboards } from "./sync_leaderboards.js";

dotenv.config();

const SYNC_INTERVAL_MS = 5 * 60 * 1000; // Sync every 5 minutes
const STALE_SYNC_INTERVAL_MS = 15 * 60 * 1000; // Sync stale wallets every 15 minutes
const LEADERBOARD_SYNC_INTERVAL_MS = 30 * 60 * 1000; // Sync leaderboards every 30 minutes

let isRunning = false;
let lastRecentSync = 0;
let lastStaleSync = 0;
let lastLeaderboardSync = 0;

async function runSync() {
  if (isRunning) {
    console.log("[Background Sync] Previous sync still running, skipping...");
    return;
  }
  
  isRunning = true;
  const now = Date.now();
  
  try {
    // Sync recent wallets every 5 minutes
    if (now - lastRecentSync >= SYNC_INTERVAL_MS) {
      console.log("[Background Sync] Starting recent wallets sync...");
      await syncRecentWallets();
      lastRecentSync = now;
    }
    
    // Sync stale wallets every 15 minutes
    if (now - lastStaleSync >= STALE_SYNC_INTERVAL_MS) {
      console.log("[Background Sync] Starting stale wallets sync...");
      await syncStaleWallets();
      lastStaleSync = now;
    }
    
    // Sync leaderboards every 30 minutes
    if (now - lastLeaderboardSync >= LEADERBOARD_SYNC_INTERVAL_MS) {
      console.log("[Background Sync] Starting leaderboards sync...");
      await syncLeaderboards();
      lastLeaderboardSync = now;
    }
  } catch (err) {
    console.error("[Background Sync] Error during sync:", err);
  } finally {
    isRunning = false;
  }
}

// Run sync immediately on start
console.log("[Background Sync] Starting background wallet sync service...");
runSync();

// Then run every minute (will check if it's time to sync)
setInterval(() => {
  runSync();
}, 60 * 1000); // Check every minute

// Keep process alive
process.on('SIGINT', () => {
  console.log("[Background Sync] Shutting down...");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log("[Background Sync] Shutting down...");
  process.exit(0);
});

