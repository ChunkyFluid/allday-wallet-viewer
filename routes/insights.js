// routes/insights.js
import pool from "../db/pool.js";
import { syncLeaderboards } from "../scripts/sync_leaderboards.js";

/**
 * Ensure insights_snapshot table exists
 */
export async function ensureInsightsSnapshotTable() {
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

/**
 * Refresh insights snapshot (runs all queries and caches result)
 */
export async function refreshInsightsSnapshot() {
  try {
    await ensureInsightsSnapshotTable();

    console.log("Refreshing insights snapshot...");
    const startTime = Date.now();

    // 1. Sync leaderboards first (prerequisite for insights)
    console.log("[Insights] Syncing leaderboards before snapshot...");
    await syncLeaderboards();

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
        "\u226410": Number(lowSerials.serial_10) || 0,
        "\u2264100": Number(lowSerials.serial_100) || 0,
        "\u22641000": Number(lowSerials.serial_1000) || 0
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
        "\u226450 (Ultra)": Number(editionSizes.ultra_limited) || 0,
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
    console.log(`\u2705 Insights snapshot refreshed in ${duration}s`);
    return { ok: true, duration };
  } catch (err) {
    console.error("Error refreshing insights snapshot:", err);
    return { ok: false, error: err.message };
  }
}

/**
 * Registers insights routes
 */
export function registerInsightsRoutes(app) {
  // GET /api/insights - return cached snapshot
  app.get("/api/insights", async (req, res) => {
    try {
      await ensureInsightsSnapshotTable();

      const result = await pool.query(`
        SELECT data, updated_at
        FROM insights_snapshot
        WHERE id = 1;
      `);

      if (!result.rows.length) {
        return res.status(503).json({
          ok: false,
          error: "Insights snapshot not available. Please refresh first.",
          needsRefresh: true
        });
      }

      const snapshot = result.rows[0].data;
      snapshot.updated_at = result.rows[0].updated_at;

      return res.json(snapshot);
    } catch (err) {
      console.error("GET /api/insights error:", err);
      return res.status(500).json({
        ok: false,
        error: "Failed to load insights: " + (err.message || String(err))
      });
    }
  });

  // POST /api/insights/refresh - manually trigger refresh
  app.post("/api/insights/refresh", async (req, res) => {
    const result = await refreshInsightsSnapshot();
    if (result.ok) {
      return res.json({
        ok: true,
        message: `Snapshot refreshed in ${result.duration}s`,
        updated_at: new Date().toISOString()
      });
    } else {
      return res.status(500).json({
        ok: false,
        error: "Failed to refresh insights snapshot: " + result.error
      });
    }
  });
}
