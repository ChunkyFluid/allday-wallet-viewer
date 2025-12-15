-- Trading System Database Schema
-- Run this migration to create tables for P2P trading, listings, and bundles

-- Trades table (P2P trade offers between users)
CREATE TABLE IF NOT EXISTS trades (
  trade_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  initiator_wallet VARCHAR(255) NOT NULL,
  target_wallet VARCHAR(255) NOT NULL,
  status VARCHAR(50) DEFAULT 'pending',  -- pending, accepted, rejected, cancelled, executed
  message TEXT,
  tx_id VARCHAR(255),  -- Flow transaction ID when executed on-chain
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Trade items (moments included in each trade)
CREATE TABLE IF NOT EXISTS trade_items (
  id SERIAL PRIMARY KEY,
  trade_id UUID REFERENCES trades(trade_id) ON DELETE CASCADE,
  nft_id VARCHAR(255) NOT NULL,
  side VARCHAR(50) NOT NULL,  -- 'initiator' (what initiator offers) or 'target' (what initiator requests)
  UNIQUE(trade_id, nft_id)
);

-- Marketplace listings (individual moment sales)
CREATE TABLE IF NOT EXISTS listings (
  listing_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  seller_wallet VARCHAR(255) NOT NULL,
  nft_id VARCHAR(255) NOT NULL UNIQUE,  -- One listing per NFT
  price_usd DECIMAL(10, 2) NOT NULL,
  status VARCHAR(50) DEFAULT 'active',  -- active, sold, cancelled
  buyer_wallet VARCHAR(255),  -- Set when sold
  tx_id VARCHAR(255),  -- Flow transaction ID when sold
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Bundles (grouped moment sales)
CREATE TABLE IF NOT EXISTS bundles (
  bundle_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  seller_wallet VARCHAR(255) NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  price_usd DECIMAL(10, 2) NOT NULL,
  status VARCHAR(50) DEFAULT 'active',  -- active, sold, cancelled
  buyer_wallet VARCHAR(255),  -- Set when sold
  tx_id VARCHAR(255),  -- Flow transaction ID when sold
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Bundle items (moments in each bundle)
CREATE TABLE IF NOT EXISTS bundle_items (
  id SERIAL PRIMARY KEY,
  bundle_id UUID REFERENCES bundles(bundle_id) ON DELETE CASCADE,
  nft_id VARCHAR(255) NOT NULL,
  UNIQUE(bundle_id, nft_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trades_initiator ON trades(initiator_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_target ON trades(target_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trade_items_trade ON trade_items(trade_id);
CREATE INDEX IF NOT EXISTS idx_listings_seller ON listings(seller_wallet);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_bundles_seller ON bundles(seller_wallet);
CREATE INDEX IF NOT EXISTS idx_bundles_status ON bundles(status);
CREATE INDEX IF NOT EXISTS idx_bundle_items_bundle ON bundle_items(bundle_id);
