// etl_wallet_summary_snapshot.js
import * as dotenv from "dotenv";
import pool from "./db/pool.js";

dotenv.config();

async function refreshWalletSummarySnapshot() {
  const client = await pool.connect();

  try {
    console.log("Refreshing wallet_summary_snapshot…");
    await client.query("BEGIN");

    // Clear existing snapshot
    await client.query("TRUNCATE TABLE wallet_summary_snapshot");

    // Rebuild from wallet_holdings + nft_core_metadata
    await client.query(`
      INSERT INTO wallet_summary_snapshot (
        wallet_address,
        moments_total,
        locked_count,
        unlocked_count,
        common_count,
        uncommon_count,
        rare_count,
        legendary_count,
        ultimate_count,
        updated_at
      )
      SELECT
        h.wallet_address,
        COUNT(*)::int AS moments_total,
        COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
        COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int    AS common_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int  AS uncommon_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int      AS rare_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int AS legendary_count,
        COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int  AS ultimate_count,
        now() AS updated_at
      FROM public.wallet_holdings AS h
      JOIN public.nft_core_metadata AS m
        ON m.nft_id = h.nft_id
      GROUP BY h.wallet_address;
    `);

    await client.query("COMMIT");
    console.log("wallet_summary_snapshot refresh complete.");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error refreshing wallet_summary_snapshot:", err);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

refreshWalletSummarySnapshot();
