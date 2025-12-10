// scripts/setup-database.js
// Creates all required tables in a fresh PostgreSQL database
// Run this once after setting up a new database

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function setupDatabase() {
  console.log("🚀 Setting up database tables...\n");

  // Drop existing snapshot tables to recreate with correct schema
  console.log("Dropping existing snapshot tables to recreate with correct schema...");
  await pgQuery(`DROP TABLE IF EXISTS top_wallets_snapshot CASCADE;`);
  await pgQuery(`DROP TABLE IF EXISTS top_wallets_by_team_snapshot CASCADE;`);
  await pgQuery(`DROP TABLE IF EXISTS top_wallets_by_tier_snapshot CASCADE;`);
  await pgQuery(`DROP TABLE IF EXISTS top_wallets_by_value_snapshot CASCADE;`);
  await pgQuery(`DROP TABLE IF EXISTS edition_price_scrape CASCADE;`);
  await pgQuery(`DROP TABLE IF EXISTS wallet_value_history CASCADE;`);
  console.log("✅ Old tables dropped\n");

  // 1. Users table (authentication, subscriptions)
  console.log("Creating users table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS public.users (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT,
      default_wallet_address TEXT,
      display_name TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      stripe_customer_id TEXT,
      stripe_subscription_id TEXT,
      subscription_status TEXT DEFAULT 'free',
      subscription_plan TEXT,
      subscription_expires_at TIMESTAMPTZ
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);`);
  console.log("✅ users table ready\n");

  // 2. NFT Core Metadata table
  console.log("Creating nft_core_metadata table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS nft_core_metadata (
      nft_id        TEXT PRIMARY KEY,
      edition_id    TEXT,
      play_id       TEXT,
      series_id     TEXT,
      set_id        TEXT,
      tier          TEXT,
      serial_number INTEGER,
      max_mint_size INTEGER,
      first_name    TEXT,
      last_name     TEXT,
      team_name     TEXT,
      position      TEXT,
      jersey_number INTEGER,
      series_name   TEXT,
      set_name      TEXT
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_edition ON nft_core_metadata(edition_id);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_team ON nft_core_metadata(team_name);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_tier ON nft_core_metadata(tier);`);
  console.log("✅ nft_core_metadata table ready\n");

  // 3. Wallet Holdings table
  console.log("Creating wallet_holdings table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_holdings (
      wallet_address TEXT NOT NULL,
      nft_id         TEXT NOT NULL,
      is_locked      BOOLEAN NOT NULL DEFAULT FALSE,
      last_event_ts  TIMESTAMPTZ,
      last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (wallet_address, nft_id)
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_wallet ON wallet_holdings(wallet_address);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_nft ON wallet_holdings(nft_id);`);
  console.log("✅ wallet_holdings table ready\n");

  // 4. Wallet Profiles table
  console.log("Creating wallet_profiles table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_profiles (
      wallet_address TEXT PRIMARY KEY,
      display_name   TEXT,
      source         TEXT,
      last_checked   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  console.log("✅ wallet_profiles table ready\n");

  // 5. Edition Price Stats table
  console.log("Creating edition_price_stats table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_stats (
      edition_id TEXT PRIMARY KEY,
      asp_90d NUMERIC,
      low_ask NUMERIC,
      last_updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  console.log("✅ edition_price_stats table ready\n");

  // 5b. Edition Price Scrape table (used for floor/ASP prices)
  console.log("Creating edition_price_scrape table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_scrape (
      edition_id TEXT PRIMARY KEY,
      lowest_ask_usd NUMERIC,
      avg_sale_usd NUMERIC,
      top_sale_usd NUMERIC,
      scraped_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  console.log("✅ edition_price_scrape table ready\n");

  // 5c. Wallet Value History table
  console.log("Creating wallet_value_history table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_value_history (
      id SERIAL PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      floor_value NUMERIC DEFAULT 0,
      asp_value NUMERIC DEFAULT 0,
      moments_count INTEGER DEFAULT 0,
      recorded_at DATE NOT NULL DEFAULT CURRENT_DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(wallet_address, recorded_at)
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_wvh_wallet ON wallet_value_history(wallet_address);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_wvh_date ON wallet_value_history(recorded_at);`);
  console.log("✅ wallet_value_history table ready\n");

  // 6. Sniper Listings table
  console.log("Creating sniper_listings table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS sniper_listings (
      nft_id TEXT PRIMARY KEY,
      listing_id TEXT,
      edition_id TEXT,
      listing_data JSONB NOT NULL,
      listed_at TIMESTAMPTZ,
      is_sold BOOLEAN NOT NULL DEFAULT FALSE,
      is_unlisted BOOLEAN NOT NULL DEFAULT FALSE,
      buyer_address TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_sniper_listings_listed_at ON sniper_listings(listed_at DESC);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_sniper_listings_updated_at ON sniper_listings(updated_at DESC);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_sniper_listings_status ON sniper_listings(is_sold, is_unlisted);`);
  console.log("✅ sniper_listings table ready\n");

  // 7. Edition Price History table
  console.log("Creating edition_price_history table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_price_history (
      id SERIAL PRIMARY KEY,
      edition_id TEXT NOT NULL,
      sale_price NUMERIC(12, 2) NOT NULL,
      sale_date DATE NOT NULL,
      sale_timestamp TIMESTAMPTZ NOT NULL,
      nft_id TEXT,
      serial_number INTEGER,
      buyer_address TEXT,
      seller_address TEXT,
      tx_id TEXT UNIQUE
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_eph_edition ON edition_price_history(edition_id);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_eph_date ON edition_price_history(sale_date);`);
  console.log("✅ edition_price_history table ready\n");

  // 8. Edition Daily Floor table
  console.log("Creating edition_daily_floor table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS edition_daily_floor (
      edition_id TEXT NOT NULL,
      price_date DATE NOT NULL,
      floor_price NUMERIC(12, 2),
      avg_price NUMERIC(12, 2),
      sales_count INTEGER,
      PRIMARY KEY (edition_id, price_date)
    );
  `);
  console.log("✅ edition_daily_floor table ready\n");

  // 9. Wallet Holdings History table
  console.log("Creating wallet_holdings_history table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS wallet_holdings_history (
      id SERIAL PRIMARY KEY,
      wallet_address TEXT NOT NULL,
      nft_id TEXT NOT NULL,
      edition_id TEXT,
      acquired_at TIMESTAMPTZ NOT NULL,
      disposed_at TIMESTAMPTZ
    );
  `);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_whh_wallet ON wallet_holdings_history(wallet_address);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_whh_nft ON wallet_holdings_history(nft_id);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_whh_acquired ON wallet_holdings_history(acquired_at);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_whh_wallet_dates ON wallet_holdings_history(wallet_address, acquired_at, disposed_at);`);
  await pgQuery(`CREATE UNIQUE INDEX IF NOT EXISTS idx_whh_unique ON wallet_holdings_history(wallet_address, nft_id, acquired_at);`);
  console.log("✅ wallet_holdings_history table ready\n");

  // 10. Insights Snapshot table
  console.log("Creating insights_snapshot table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS insights_snapshot (
      id INTEGER PRIMARY KEY DEFAULT 1,
      data JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT single_row CHECK (id = 1)
    );
  `);
  console.log("✅ insights_snapshot table ready\n");

  // 11. Top Wallets Snapshot table
  console.log("Creating top_wallets_snapshot table...");
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
      unique_editions INTEGER DEFAULT 0,
      floor_value NUMERIC DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  console.log("✅ top_wallets_snapshot table ready\n");

  // 12. Top Wallets by Team Snapshot table
  console.log("Creating top_wallets_by_team_snapshot table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS top_wallets_by_team_snapshot (
      wallet_address TEXT,
      team_name TEXT,
      display_name TEXT,
      total_moments INTEGER DEFAULT 0,
      unlocked_moments INTEGER DEFAULT 0,
      locked_moments INTEGER DEFAULT 0,
      tier_common INTEGER DEFAULT 0,
      tier_uncommon INTEGER DEFAULT 0,
      tier_rare INTEGER DEFAULT 0,
      tier_legendary INTEGER DEFAULT 0,
      tier_ultimate INTEGER DEFAULT 0,
      unique_editions INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (wallet_address, team_name)
    );
  `);
  console.log("✅ top_wallets_by_team_snapshot table ready\n");

  // 13. Top Wallets by Tier Snapshot table
  console.log("Creating top_wallets_by_tier_snapshot table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS top_wallets_by_tier_snapshot (
      wallet_address TEXT,
      tier TEXT,
      display_name TEXT,
      total_moments INTEGER DEFAULT 0,
      unlocked_moments INTEGER DEFAULT 0,
      locked_moments INTEGER DEFAULT 0,
      tier_common INTEGER DEFAULT 0,
      tier_uncommon INTEGER DEFAULT 0,
      tier_rare INTEGER DEFAULT 0,
      tier_legendary INTEGER DEFAULT 0,
      tier_ultimate INTEGER DEFAULT 0,
      unique_editions INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (wallet_address, tier)
    );
  `);
  console.log("✅ top_wallets_by_tier_snapshot table ready\n");

  // 14. Top Wallets by Value Snapshot table
  console.log("Creating top_wallets_by_value_snapshot table...");
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS top_wallets_by_value_snapshot (
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
      unique_editions INTEGER DEFAULT 0,
      floor_value NUMERIC DEFAULT 0,
      asp_value NUMERIC DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  console.log("✅ top_wallets_by_value_snapshot table ready\n");

  // Verify all tables
  console.log("📊 Verifying tables...");
  const { rows } = await pgQuery(`
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name;
  `);
  
  console.log("\n✅ Database setup complete! Tables created:");
  rows.forEach(r => console.log(`   - ${r.table_name}`));
  console.log(`\nTotal: ${rows.length} tables\n`);
}

setupDatabase()
  .then(() => {
    console.log("🎉 Database setup finished successfully!");
    process.exit(0);
  })
  .catch((err) => {
    console.error("💥 Database setup failed:", err);
    process.exit(1);
  });
