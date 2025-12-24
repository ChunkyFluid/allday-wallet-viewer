-- Database Schema Redesign - Phase 1: Create New Tables
-- This script creates the new normalized schema alongside existing tables
-- Safe to run in production - NO DATA DELETION, NO DOWNTIME

-- ============================================================================
-- 1. NFTs Table - Master Registry of All NFTs
-- ============================================================================
CREATE TABLE IF NOT EXISTS nfts (
  nft_id TEXT PRIMARY KEY,
  edition_id TEXT NOT NULL,
  serial_number INTEGER,  -- Nullable - some NFTs may not have serial numbers
  max_mint_size INTEGER,
  
  -- Player Info
  first_name TEXT,
  last_name TEXT,
  player_name TEXT GENERATED ALWAYS AS (
    CASE 
      WHEN first_name IS NOT NULL AND last_name IS NOT NULL 
      THEN first_name || ' ' || last_name 
      ELSE NULL 
    END
  ) STORED,
  team_name TEXT,
  position TEXT,
  jersey_number INTEGER,
  
  -- Moment Info
  series_id TEXT,
  series_name TEXT,
  set_id TEXT,
  set_name TEXT,
  play_id TEXT,
  tier TEXT,
  
  -- Metadata tracking
  metadata_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for nfts table
CREATE INDEX IF NOT EXISTS idx_nfts_edition ON nfts(edition_id);
CREATE INDEX IF NOT EXISTS idx_nfts_player ON nfts(last_name, first_name) WHERE last_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nfts_team ON nfts(team_name) WHERE team_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nfts_set ON nfts(set_name) WHERE set_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nfts_tier ON nfts(tier) WHERE tier IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nfts_serial ON nfts(edition_id, serial_number);

COMMENT ON TABLE nfts IS 'Master registry of all NFL All Day NFTs - one row per NFT with immutable metadata';
COMMENT ON COLUMN nfts.player_name IS 'Auto-generated from first_name + last_name';
COMMENT ON COLUMN nfts.metadata_synced_at IS 'Last time metadata was synced from blockchain';

-- ============================================================================
-- 2. Ownership Table - Current Ownership State
-- ============================================================================
CREATE TABLE IF NOT EXISTS ownership (
  wallet_address TEXT NOT NULL,
  nft_id TEXT NOT NULL,
  is_locked BOOLEAN DEFAULT false,
  first_acquired_at TIMESTAMPTZ NOT NULL,  -- NEVER changes after initial insert
  last_synced_at TIMESTAMPTZ DEFAULT NOW(),
  
  PRIMARY KEY (wallet_address, nft_id),
  FOREIGN KEY (nft_id) REFERENCES nfts(nft_id) ON DELETE CASCADE
);

-- Indexes for ownership table
CREATE INDEX IF NOT EXISTS idx_ownership_wallet ON ownership(wallet_address);
CREATE INDEX IF NOT EXISTS idx_ownership_nft ON ownership(nft_id);
CREATE INDEX IF NOT EXISTS idx_ownership_locked ON ownership(is_locked) WHERE is_locked = true;
CREATE INDEX IF NOT EXISTS idx_ownership_acquired ON ownership(first_acquired_at DESC);

COMMENT ON TABLE ownership IS 'Current ownership state - who owns what right now';
COMMENT ON COLUMN ownership.first_acquired_at IS 'IMMUTABLE - first time this wallet acquired this NFT (never updated)';
COMMENT ON COLUMN ownership.last_synced_at IS 'Last time this ownership record was verified via blockchain sync';

-- ============================================================================
-- 3. Ownership History Table - Complete Transfer Log
-- ============================================================================
CREATE TABLE IF NOT EXISTS ownership_history (
  id SERIAL PRIMARY KEY,
  nft_id TEXT NOT NULL,
  from_wallet TEXT,  -- NULL for mint events
  to_wallet TEXT,    -- NULL for burn events
  event_type TEXT NOT NULL CHECK (event_type IN ('MINT', 'TRANSFER', 'LOCK', 'UNLOCK', 'BURN', 'LIST', 'DELIST', 'SALE')),
  
  -- Blockchain details
  block_height BIGINT,
  transaction_id TEXT,
  event_timestamp TIMESTAMPTZ NOT NULL,
  
  -- Optional metadata
  price NUMERIC(10,2),  -- For SALE events
  marketplace TEXT,     -- For LIST/SALE events
  
  created_at TIMESTAMPTZ DEFAULT NOW(),
  
  FOREIGN KEY (nft_id) REFERENCES nfts(nft_id) ON DELETE CASCADE
);

-- Indexes for ownership_history table
CREATE INDEX IF NOT EXISTS idx_ownership_history_nft ON ownership_history(nft_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ownership_history_to_wallet ON ownership_history(to_wallet, event_timestamp DESC) WHERE to_wallet IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ownership_history_from_wallet ON ownership_history(from_wallet, event_timestamp DESC) WHERE from_wallet IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ownership_history_timestamp ON ownership_history(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ownership_history_event_type ON ownership_history(event_type);

COMMENT ON TABLE ownership_history IS 'Append-only log of all NFT ownership changes - never delete from this table';
COMMENT ON COLUMN ownership_history.from_wallet IS 'NULL indicates MINT event (NFT created)';
COMMENT ON COLUMN ownership_history.to_wallet IS 'NULL indicates BURN event (NFT destroyed)';

-- ============================================================================
-- 4. Edition Pricing Table - Market Data per Edition
-- ============================================================================
CREATE TABLE IF NOT EXISTS edition_pricing (
  edition_id TEXT PRIMARY KEY,
  low_ask NUMERIC(10,2),
  average_sale_price NUMERIC(10,2),
  top_sale NUMERIC(10,2),
  total_sales INTEGER DEFAULT 0,
  total_volume NUMERIC(12,2) DEFAULT 0,
  last_sale_price NUMERIC(10,2),
  last_sale_at TIMESTAMPTZ,
  floor_updated_at TIMESTAMPTZ,
  pricing_updated_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for edition_pricing table
CREATE INDEX IF NOT EXISTS idx_edition_pricing_low_ask ON edition_pricing(low_ask) WHERE low_ask IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_edition_pricing_asp ON edition_pricing(average_sale_price) WHERE average_sale_price IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_edition_pricing_volume ON edition_pricing(total_volume DESC);

COMMENT ON TABLE edition_pricing IS 'Current market pricing data per edition - updated frequently from marketplace APIs';
COMMENT ON COLUMN edition_pricing.low_ask IS 'Current lowest asking price (floor price)';
COMMENT ON COLUMN edition_pricing.average_sale_price IS 'Average sale price over recent period';
COMMENT ON COLUMN edition_pricing.total_volume IS 'Total USD volume traded for this edition';

-- ============================================================================
-- Triggers to Update Timestamps
-- ============================================================================

-- Update nfts.updated_at on changes
CREATE OR REPLACE FUNCTION update_nfts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_nfts_updated_at ON nfts;
CREATE TRIGGER trigger_nfts_updated_at
  BEFORE UPDATE ON nfts
  FOR EACH ROW
  EXECUTE FUNCTION update_nfts_updated_at();

-- Update ownership.last_synced_at on changes
CREATE OR REPLACE FUNCTION update_ownership_synced_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_synced_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_ownership_synced_at ON ownership;
CREATE TRIGGER trigger_ownership_synced_at
  BEFORE UPDATE ON ownership
  FOR EACH ROW
  EXECUTE FUNCTION update_ownership_synced_at();

-- ============================================================================
-- Success Message
-- ============================================================================
DO $$ 
BEGIN
  RAISE NOTICE 'Phase 1 Complete: New schema tables created successfully!';
  RAISE NOTICE 'Tables created: nfts, ownership, ownership_history, edition_pricing';
  RAISE NOTICE 'Next step: Run migration script to populate from existing data';
END $$;
