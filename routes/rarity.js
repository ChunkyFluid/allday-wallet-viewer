// Rarity Routes - Rarity scoring and leaderboard endpoints
import { pgQuery } from "../db.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Cache for rarity leaderboard
let rarityLeaderboardCache = null;
let rarityLeaderboardCacheTime = 0;
const RARITY_LEADERBOARD_CACHE_TTL = 30 * 60 * 1000; // 30 minutes

const RARITY_LEADERBOARD_SNAPSHOT_FILE = path.join(process.cwd(), "public", "data", "rarity_leaderboard_snapshot.json");

// Known contract/holding addresses to exclude from leaderboard
const EXCLUDED_LEADERBOARD_WALLETS = [
    '0xe4cf4bdc1751c65d', // NFL All Day contract
    '0xb6f2481eba4df97b', // Huge custodial/system wallet  
    '0x4eb8a10cb9f87357', // NFT Storefront contract
    '0xf919ee77447b7497', // Dapper wallet / marketplace
    '0x4eded0de73c5b00c', // Another system wallet
    '0x0b2a3299cc857e29', // Pack distribution
];

/**
 * Compute rarity score for a single wallet
 */
async function computeRarityScore(walletAddress) {
    const result = await pgQuery(
        `SELECT 
      h.nft_id, m.serial_number, m.jersey_number, m.tier, m.max_mint_size,
      m.first_name, m.last_name, m.edition_id
    FROM holdings h
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

/**
 * Register rarity-related routes
 */
export function registerRarityRoutes(app) {
    // Rarity Score Leaderboard - cached for performance
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
        FROM holdings
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
            FROM holdings 
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
        FROM holdings h
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
}
