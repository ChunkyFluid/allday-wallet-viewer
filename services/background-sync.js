// Background Wallet Sync Service
// Continuously syncs wallets to keep them up to date
// Run this with: node services/background-sync.js

import { syncRecentWallets, syncStaleWallets } from '../scripts/sync_wallets_from_blockchain.js';
import { syncLeaderboards } from '../scripts/sync_leaderboards.js';
import { watchPackOpenings } from './pack-opening-watcher.js';
import { refreshInsightsSnapshot } from '../routes/insights.js';

const RECENT_SYNC_INTERVAL = 5 * 60 * 1000; // 5 minutes
const STALE_SYNC_INTERVAL = 15 * 60 * 1000; // 15 minutes
const LEADERBOARD_SYNC_INTERVAL = 60 * 60 * 1000; // 60 minutes
const INSIGHTS_SYNC_INTERVAL = 6 * 60 * 60 * 1000; // 6 hours

console.log("[Background Sync] Starting continuous wallet sync service...");
console.log(`[Background Sync] Recent wallets: every ${RECENT_SYNC_INTERVAL / 1000 / 60} minutes`);
console.log(`[Background Sync] Stale wallets: every ${STALE_SYNC_INTERVAL / 1000 / 60} minutes`);
console.log(`[Background Sync] Leaderboards: every ${LEADERBOARD_SYNC_INTERVAL / 1000 / 60} minutes`);
console.log(`[Background Sync] Insights: every ${INSIGHTS_SYNC_INTERVAL / 1000 / 60 / 60} hours`);

// Sync recently accessed wallets frequently
async function syncRecentLoop() {
    try {
        console.log("\n[Background Sync] === Syncing Recently Accessed Wallets ===");
        await syncRecentWallets();
        console.log("[Background Sync] Recent wallet sync complete");
    } catch (err) {
        console.error("[Background Sync] Error syncing recent wallets:", err.message);
    }

    // Schedule next run
    setTimeout(syncRecentLoop, RECENT_SYNC_INTERVAL);
}

// Sync stale wallets less frequently
async function syncStaleLoop() {
    try {
        console.log("\n[Background Sync] === Syncing Stale Wallets ===");
        await syncStaleWallets();
        console.log("[Background Sync] Stale wallet sync complete");
    } catch (err) {
        console.error("[Background Sync] Error syncing stale wallets:", err.message);
    }

    // Schedule next run
    setTimeout(syncStaleLoop, STALE_SYNC_INTERVAL);
}

// Sync leaderboard snapshots
async function syncLeaderboardLoop() {
    try {
        console.log("\n[Background Sync] === Syncing Leaderboard Snapshots ===");
        await syncLeaderboards();
        console.log("[Background Sync] Leaderboard sync complete");
    } catch (err) {
        console.error("[Background Sync] Error syncing leaderboards:", err.message);
    }

    // Schedule next run
    setTimeout(syncLeaderboardLoop, LEADERBOARD_SYNC_INTERVAL);
}

// Sync insights snapshots
async function syncInsightsLoop() {
    try {
        console.log("\n[Background Sync] === Syncing Insights Snapshots ===");
        await refreshInsightsSnapshot();
        console.log("[Background Sync] Insights sync complete");
    } catch (err) {
        console.error("[Background Sync] Error syncing insights:", err.message);
    }

    // Schedule next run
    setTimeout(syncInsightsLoop, INSIGHTS_SYNC_INTERVAL);
}

// Start loops
syncRecentLoop();
setTimeout(syncStaleLoop, 30000); // Start stale sync after 30 seconds
setTimeout(syncLeaderboardLoop, 60000); // Start leaderboard sync after 1 minute
setTimeout(syncInsightsLoop, 120000); // Start insights sync after 2 minutes

// Start pack opening watcher (runs independently)
watchPackOpenings().catch(err => {
    console.error("[Background Sync] Failed to start pack watcher:", err.message);
});
console.log("[Background Sync] Pack opening watcher started");

// Handle graceful shutdown
process.on('SIGTERM', () => {
    console.log("[Background Sync] Shutting down gracefully...");
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log("[Background Sync] Shutting down gracefully...");
    process.exit(0);
});
