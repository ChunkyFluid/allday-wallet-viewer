// etl_top_wallets_by_tier_snapshot.js
import * as dotenv from "dotenv";
import pool from "./db/pool.js";

dotenv.config();

async function refreshTopWalletsByTierSnapshot() {
  const client = await pool.connect();

  try {
    console.log("Refreshing top_wallets_by_tier_snapshot…");
    await client.query("BEGIN");

    // Create table if it doesn't exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS top_wallets_by_tier_snapshot (
        tier TEXT NOT NULL,
        wallet_address TEXT NOT NULL,
        display_name TEXT,
        total_moments INTEGER NOT NULL,
        unlocked_moments INTEGER NOT NULL,
        locked_moments INTEGER NOT NULL,
        tier_common INTEGER NOT NULL,
        tier_uncommon INTEGER NOT NULL,
        tier_rare INTEGER NOT NULL,
        tier_legendary INTEGER NOT NULL,
        tier_ultimate INTEGER NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (tier, wallet_address)
      );
    `);

    // Create index for faster lookups
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_top_wallets_by_tier_tier 
      ON top_wallets_by_tier_snapshot (tier, total_moments DESC);
    `);

    // Wipe previous snapshot
    await client.query("TRUNCATE TABLE top_wallets_by_tier_snapshot");

    // Recompute from wallet_holdings + metadata, grouped by tier
    await client.query(`
      INSERT INTO top_wallets_by_tier_snapshot (
        tier,
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
      )
      SELECT
        LOWER(ncm.tier) AS tier,
        wh.wallet_address,
        COALESCE(wp.display_name, wh.wallet_address) AS display_name,
        COUNT(*)::int AS total_moments,
        COUNT(*) FILTER (WHERE wh.is_locked = FALSE)::int AS unlocked_moments,
        COUNT(*) FILTER (WHERE wh.is_locked = TRUE)::int AS locked_moments,
        COUNT(*) FILTER (WHERE LOWER(ncm.tier) = 'common')::int    AS tier_common,
        COUNT(*) FILTER (WHERE LOWER(ncm.tier) = 'uncommon')::int  AS tier_uncommon,
        COUNT(*) FILTER (WHERE LOWER(ncm.tier) = 'rare')::int      AS tier_rare,
        COUNT(*) FILTER (WHERE LOWER(ncm.tier) = 'legendary')::int AS tier_legendary,
        COUNT(*) FILTER (WHERE LOWER(ncm.tier) = 'ultimate')::int  AS tier_ultimate,
        now() AS updated_at
      FROM public.wallet_holdings AS wh
      JOIN public.nft_core_metadata AS ncm
        ON ncm.nft_id = wh.nft_id
      LEFT JOIN public.wallet_profiles AS wp
        ON wp.wallet_address = wh.wallet_address
      WHERE 
        ncm.tier IS NOT NULL 
        AND ncm.tier != ''
        AND wh.wallet_address NOT IN (
          '0xe4cf4bdc1751c65d', -- AllDay contract
          '0xb6f2481eba4df97b'  -- huge custodial/system wallet
        )
      GROUP BY
        LOWER(ncm.tier),
        wh.wallet_address,
        COALESCE(wp.display_name, wh.wallet_address);
    `);

    await client.query("COMMIT");
    console.log("top_wallets_by_tier_snapshot refresh complete.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error refreshing top_wallets_by_tier_snapshot:", err);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

refreshTopWalletsByTierSnapshot();

