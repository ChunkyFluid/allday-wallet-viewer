// etl_wallet_summary_snapshot.js
// Pre-compute wallet summaries for fast API responses

import * as dotenv from "dotenv";
import pool from "./db/pool.js";

dotenv.config();

async function refreshWalletSummarySnapshot() {
    const client = await pool.connect();

    try {
        console.log("Creating wallet_summary_snapshot table if needed...");
        
        // Check if table exists with old schema
        const tableExists = await client.query(`
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'wallet_summary_snapshot';
        `);
        
        if (tableExists.rows.length > 0) {
            // Check if table has old schema (has common_count instead of tier_common)
            const hasOldSchema = await client.query(`
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'wallet_summary_snapshot' 
                AND column_name = 'common_count';
            `);
            
            if (hasOldSchema.rows.length > 0) {
                console.log("  ⚠ Table exists with old schema - dropping and recreating...");
                await client.query(`DROP TABLE IF EXISTS wallet_summary_snapshot CASCADE;`);
            }
        }
        
        // Create table with correct schema
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallet_summary_snapshot (
                wallet_address TEXT PRIMARY KEY,
                display_name TEXT,
                moments_total INT NOT NULL DEFAULT 0,
                locked_count INT NOT NULL DEFAULT 0,
                unlocked_count INT NOT NULL DEFAULT 0,
                tier_common INT NOT NULL DEFAULT 0,
                tier_uncommon INT NOT NULL DEFAULT 0,
                tier_rare INT NOT NULL DEFAULT 0,
                tier_legendary INT NOT NULL DEFAULT 0,
                tier_ultimate INT NOT NULL DEFAULT 0,
                floor_value NUMERIC NOT NULL DEFAULT 0,
                asp_value NUMERIC NOT NULL DEFAULT 0,
                priced_moments INT NOT NULL DEFAULT 0,
                last_synced_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log("  ✓ Table created/verified");

        // Create indexes (only if column exists)
        // Check if display_name column exists before creating index
        const colCheck = await client.query(`
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'wallet_summary_snapshot' 
            AND column_name = 'display_name';
        `);
        
        if (colCheck.rows.length > 0) {
            await client.query(`
                CREATE INDEX IF NOT EXISTS idx_wallet_summary_display_name 
                ON wallet_summary_snapshot(display_name);
            `);
            console.log("  ✓ display_name index created/verified");
        } else {
            console.warn("  ⚠ display_name column not found, skipping index creation");
        }

        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_wallet_summary_updated_at 
            ON wallet_summary_snapshot(updated_at);
        `);
        console.log("  ✓ updated_at index created/verified");

        console.log("Refreshing wallet_summary_snapshot…");
        await client.query("BEGIN");

        // Use UPSERT to update existing or insert new
        // This allows incremental updates without truncating
        await client.query(`
            INSERT INTO wallet_summary_snapshot (
                wallet_address,
                display_name,
                moments_total,
                locked_count,
                unlocked_count,
                tier_common,
                tier_uncommon,
                tier_rare,
                tier_legendary,
                tier_ultimate,
                floor_value,
                asp_value,
                priced_moments,
                last_synced_at,
                updated_at
            )
            SELECT
                h.wallet_address,
                COALESCE(wp.display_name, h.wallet_address) AS display_name,
                COUNT(*)::int AS moments_total,
                COUNT(*) FILTER (WHERE COALESCE(h.is_locked, false))::int AS locked_count,
                COUNT(*) FILTER (WHERE NOT COALESCE(h.is_locked, false))::int AS unlocked_count,
                COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int AS tier_common,
                COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int AS tier_uncommon,
                COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int AS tier_rare,
                COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int AS tier_legendary,
                COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int AS tier_ultimate,
                COALESCE(
                    SUM(
                        CASE
                            WHEN eps.lowest_ask_usd IS NOT NULL THEN eps.lowest_ask_usd
                            ELSE 0
                        END
                    ),
                    0
                )::numeric AS floor_value,
                COALESCE(
                    SUM(
                        CASE
                            WHEN eps.avg_sale_usd IS NOT NULL THEN eps.avg_sale_usd
                            ELSE 0
                        END
                    ),
                    0
                )::numeric AS asp_value,
                COUNT(*) FILTER (
                    WHERE eps.lowest_ask_usd IS NOT NULL OR eps.avg_sale_usd IS NOT NULL
                )::int AS priced_moments,
                MAX(h.last_synced_at) AS last_synced_at,
                NOW() AS updated_at
            FROM wallet_holdings h
            JOIN nft_core_metadata m ON m.nft_id = h.nft_id
            LEFT JOIN wallet_profiles wp ON wp.wallet_address = h.wallet_address
            LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
            GROUP BY h.wallet_address, COALESCE(wp.display_name, h.wallet_address)
            ON CONFLICT (wallet_address) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                moments_total = EXCLUDED.moments_total,
                locked_count = EXCLUDED.locked_count,
                unlocked_count = EXCLUDED.unlocked_count,
                tier_common = EXCLUDED.tier_common,
                tier_uncommon = EXCLUDED.tier_uncommon,
                tier_rare = EXCLUDED.tier_rare,
                tier_legendary = EXCLUDED.tier_legendary,
                tier_ultimate = EXCLUDED.tier_ultimate,
                floor_value = EXCLUDED.floor_value,
                asp_value = EXCLUDED.asp_value,
                priced_moments = EXCLUDED.priced_moments,
                last_synced_at = EXCLUDED.last_synced_at,
                updated_at = EXCLUDED.updated_at;
        `);

        // Remove wallets that no longer have holdings
        await client.query(`
            DELETE FROM wallet_summary_snapshot
            WHERE wallet_address NOT IN (
                SELECT DISTINCT wallet_address FROM wallet_holdings
            );
        `);

        await client.query("COMMIT");

        const count = await client.query(`
            SELECT COUNT(*) as count FROM wallet_summary_snapshot;
        `);
        
        console.log(`✅ wallet_summary_snapshot refreshed. Total wallets: ${count.rows[0].count}`);
    } catch (err) {
        await client.query("ROLLBACK");
        console.error("Error refreshing wallet_summary_snapshot:", err);
        throw err;
    } finally {
        client.release();
        await pool.end();
    }
}

refreshWalletSummarySnapshot()
    .then(() => {
        process.exit(0);
    })
    .catch((err) => {
        console.error("Fatal error:", err);
        process.exit(1);
    });
