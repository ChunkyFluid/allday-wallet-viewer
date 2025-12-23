// Sync leaderboard snapshot tables from wallet_holdings data
// Run this periodically to keep leaderboards up-to-date

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);

dotenv.config();

async function syncLeaderboards() {
  console.log("[Leaderboards] === Begin sync ===");
  const startedAt = Date.now();
  console.log("[Leaderboards] Starting sync...");
  const startTime = Date.now();

  try {
    // 1. SYNC TOP_WALLETS_SNAPSHOT - Main leaderboard
    console.log("[Leaderboards] Syncing top_wallets_snapshot...");

    await pgQuery(`
      CREATE TABLE IF NOT EXISTS top_wallets_snapshot (
        wallet_address TEXT PRIMARY KEY,
        display_name TEXT,
        total_moments INTEGER DEFAULT 0,
        unlocked_moments INTEGER DEFAULT 0,
        locked_moments INTEGER DEFAULT 0,
        tier_common INTEGER DEFAULT 0,
        tier_uncommon INTEGER DEFAULT 0,
        tier_rare INTEGER DEFAULT 0,
        tier_legendary INTEGER DEFAULT 0,
        tier_ultimate INTEGER DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Truncate and repopulate
    await pgQuery(`TRUNCATE top_wallets_snapshot`);

    await pgQuery(`
      INSERT INTO top_wallets_snapshot (
        wallet_address, display_name, total_moments, unlocked_moments, locked_moments,
        tier_common, tier_uncommon, tier_rare, tier_legendary, tier_ultimate, updated_at
      )
      SELECT 
        h.wallet_address,
        COALESCE(p.display_name, h.wallet_address) as display_name,
        COUNT(*)::int as total_moments,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false) = false)::int as unlocked_moments,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false) = true)::int as locked_moments,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int as tier_common,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int as tier_uncommon,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int as tier_rare,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int as tier_legendary,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int as tier_ultimate,
        NOW()
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      GROUP BY h.wallet_address, p.display_name
      HAVING COUNT(*) > 0
    `);

    const topWalletsCount = await pgQuery(`SELECT COUNT(*)::int as cnt FROM top_wallets_snapshot`);
    console.log(`[Leaderboards] ✅ top_wallets_snapshot: ${topWalletsCount.rows[0].cnt} wallets`);

    // 2. SYNC TOP_WALLETS_BY_TEAM_SNAPSHOT
    console.log("[Leaderboards] Syncing top_wallets_by_team_snapshot...");

    await pgQuery(`
      CREATE TABLE IF NOT EXISTS top_wallets_by_team_snapshot (
        wallet_address TEXT,
        team_name TEXT,
        display_name TEXT,
        total_moments INTEGER DEFAULT 0,
        tier_common INTEGER DEFAULT 0,
        tier_uncommon INTEGER DEFAULT 0,
        tier_rare INTEGER DEFAULT 0,
        tier_legendary INTEGER DEFAULT 0,
        tier_ultimate INTEGER DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (wallet_address, team_name)
      )
    `);

    await pgQuery(`TRUNCATE top_wallets_by_team_snapshot`);

    await pgQuery(`
      INSERT INTO top_wallets_by_team_snapshot (
        wallet_address, team_name, display_name, total_moments,
        tier_common, tier_uncommon, tier_rare, tier_legendary, tier_ultimate, updated_at
      )
      SELECT 
        h.wallet_address,
        COALESCE(m.team_name, 'Unknown') as team_name,
        COALESCE(p.display_name, h.wallet_address) as display_name,
        COUNT(*)::int as total_moments,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int as tier_common,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int as tier_uncommon,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int as tier_rare,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int as tier_legendary,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int as tier_ultimate,
        NOW()
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      WHERE m.team_name IS NOT NULL AND m.team_name != ''
      GROUP BY h.wallet_address, m.team_name, p.display_name
      HAVING COUNT(*) > 0
    `);

    const byTeamCount = await pgQuery(`SELECT COUNT(*)::int as cnt FROM top_wallets_by_team_snapshot`);
    console.log(`[Leaderboards] ✅ top_wallets_by_team_snapshot: ${byTeamCount.rows[0].cnt} entries`);

    // 3. SYNC TOP_WALLETS_BY_TIER_SNAPSHOT
    console.log("[Leaderboards] Syncing top_wallets_by_tier_snapshot...");

    await pgQuery(`
      CREATE TABLE IF NOT EXISTS top_wallets_by_tier_snapshot (
        wallet_address TEXT,
        tier TEXT,
        display_name TEXT,
        total_moments INTEGER DEFAULT 0,
        tier_common INTEGER DEFAULT 0,
        tier_uncommon INTEGER DEFAULT 0,
        tier_rare INTEGER DEFAULT 0,
        tier_legendary INTEGER DEFAULT 0,
        tier_ultimate INTEGER DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (wallet_address, tier)
      )
    `);

    await pgQuery(`TRUNCATE top_wallets_by_tier_snapshot`);

    await pgQuery(`
      INSERT INTO top_wallets_by_tier_snapshot (
        wallet_address, tier, display_name, total_moments,
        tier_common, tier_uncommon, tier_rare, tier_legendary, tier_ultimate, updated_at
      )
      SELECT 
        h.wallet_address,
        LOWER(COALESCE(m.tier, 'unknown')) as tier,
        COALESCE(p.display_name, h.wallet_address) as display_name,
        COUNT(*)::int as total_moments,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int as tier_common,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int as tier_uncommon,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int as tier_rare,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int as tier_legendary,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int as tier_ultimate,
        NOW()
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      GROUP BY h.wallet_address, LOWER(COALESCE(m.tier, 'unknown')), p.display_name
      HAVING COUNT(*) > 0
    `);

    const byTierCount = await pgQuery(`SELECT COUNT(*)::int as cnt FROM top_wallets_by_tier_snapshot`);
    console.log(`[Leaderboards] ✅ top_wallets_by_tier_snapshot: ${byTierCount.rows[0].cnt} entries`);

    // 4. SYNC TOP_WALLETS_BY_VALUE_SNAPSHOT
    console.log("[Leaderboards] Syncing top_wallets_by_value_snapshot...");

    await pgQuery(`
      CREATE TABLE IF NOT EXISTS top_wallets_by_value_snapshot (
        wallet_address TEXT PRIMARY KEY,
        display_name TEXT,
        total_moments INTEGER DEFAULT 0,
        tier_common INTEGER DEFAULT 0,
        tier_uncommon INTEGER DEFAULT 0,
        tier_rare INTEGER DEFAULT 0,
        tier_legendary INTEGER DEFAULT 0,
        tier_ultimate INTEGER DEFAULT 0,
        floor_value NUMERIC DEFAULT 0,
        asp_value NUMERIC DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pgQuery(`TRUNCATE top_wallets_by_value_snapshot`);

    await pgQuery(`
      INSERT INTO top_wallets_by_value_snapshot (
        wallet_address, display_name, total_moments,
        tier_common, tier_uncommon, tier_rare, tier_legendary, tier_ultimate,
        floor_value, asp_value, updated_at
      )
      SELECT 
        h.wallet_address,
        COALESCE(p.display_name, h.wallet_address) as display_name,
        COUNT(*)::int as total_moments,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'COMMON')::int as tier_common,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'UNCOMMON')::int as tier_uncommon,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'RARE')::int as tier_rare,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'LEGENDARY')::int as tier_legendary,
        COUNT(*) FILTER (WHERE UPPER(COALESCE(m.tier, '')) = 'ULTIMATE')::int as tier_ultimate,
        COALESCE(SUM(COALESCE(eps.lowest_ask_usd, 0)), 0)::numeric as floor_value,
        COALESCE(SUM(COALESCE(eps.avg_sale_usd, 0)), 0)::numeric as asp_value,
        NOW()
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
      LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
      GROUP BY h.wallet_address, p.display_name
      HAVING COUNT(*) > 0
    `);

    const byValueCount = await pgQuery(`SELECT COUNT(*)::int as cnt FROM top_wallets_by_value_snapshot`);
    console.log(`[Leaderboards] ✅ top_wallets_by_value_snapshot: ${byValueCount.rows[0].cnt} wallets`);

    const elapsed = Date.now() - startTime;
    console.log(`[Leaderboards] ✅ Sync completed in ${elapsed}ms`);

    return {
      topWallets: topWalletsCount.rows[0].cnt,
      byTeam: byTeamCount.rows[0].cnt,
      byTier: byTierCount.rows[0].cnt,
      byValue: byValueCount.rows[0].cnt,
      elapsed
    };

  } catch (err) {
    console.error("[Leaderboards] ❌ Error:", err.message);
    throw err;
  }
}

// Run if executed directly
if (process.argv[1] && process.argv[1] === __filename) {
  console.log("[Leaderboards] Script invoked via CLI");
  syncLeaderboards()
    .then((result) => {
      const finishedAt = new Date().toISOString();
      console.log("[Leaderboards] ✅ Done!", result);
      console.log("[Leaderboards] Finished at:", finishedAt);
      process.exit(0);
    })
    .catch((err) => {
      console.error("[Leaderboards] 💥 Fatal error:", err);
      process.exit(1);
    });
}

export { syncLeaderboards };

