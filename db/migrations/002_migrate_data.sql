-- Migration Script: Populate New Schema from Existing Tables
-- This migrates data from the old fragmented schema to the new normalized schema
-- Safe to run multiple times (uses UPSERT logic)

-- ============================================================================
-- STEP 1: Populate nfts table from nft_core_metadata_v2
-- ============================================================================
INSERT INTO nfts (
  nft_id,
  edition_id,
  serial_number,
  max_mint_size,
  first_name,
  last_name,
  team_name,
  position,
  jersey_number,
  series_id,
  series_name,
  set_id,
  set_name,
  play_id,
  tier,
  metadata_synced_at
)
SELECT 
  nft_id,
  edition_id,
  serial_number,
  max_mint_size,
  first_name,
  last_name,
  team_name,
  position,
  jersey_number,
  series_id,
  series_name,
  set_id,
  set_name,
  play_id,
  tier,
  NOW() as metadata_synced_at
FROM nft_core_metadata_v2
ON CONFLICT (nft_id) DO UPDATE SET
  edition_id = EXCLUDED.edition_id,
  serial_number = EXCLUDED.serial_number,
  max_mint_size = EXCLUDED.max_mint_size,
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  team_name = EXCLUDED.team_name,
  position = EXCLUDED.position,
  jersey_number = EXCLUDED.jersey_number,
  series_id = EXCLUDED.series_id,
  series_name = EXCLUDED.series_name,
  set_id = EXCLUDED.set_id,
  set_name = EXCLUDED.set_name,
  play_id = EXCLUDED.play_id,
  tier = EXCLUDED.tier,
  metadata_synced_at = EXCLUDED.metadata_synced_at,
  updated_at = NOW();

-- ============================================================================
-- STEP 2: Populate ownership table from wallet_holdings + holdings
-- ============================================================================
-- Strategy: Use wallet_holdings as primary source, join with holdings for acquired_at
INSERT INTO ownership (
  wallet_address,
  nft_id,
  is_locked,
  first_acquired_at,
  last_synced_at
)
SELECT 
  wh.wallet_address,
  wh.nft_id,
  wh.is_locked,
  -- Use acquired_at from holdings if available, otherwise use last_event_ts or NOW()
  COALESCE(h.acquired_at, wh.last_event_ts, NOW()) as first_acquired_at,
  wh.last_synced_at
FROM wallet_holdings wh
LEFT JOIN holdings h ON wh.wallet_address = h.wallet_address AND wh.nft_id = h.nft_id
ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
  is_locked = EXCLUDED.is_locked,
  last_synced_at = EXCLUDED.last_synced_at
  -- NOTE: first_acquired_at is NOT updated - it's immutable!
;

-- ============================================================================
-- STEP 3: Populate edition_pricing from edition_price_stats
-- ============================================================================
INSERT INTO edition_pricing (
  edition_id,
  low_ask,
  average_sale_price,
  top_sale,
  total_sales,
  floor_updated_at,
  pricing_updated_at
)
SELECT 
  edition_id,
  low_ask,
  average_sale_price,
  top_sale,
  total_sales,
  NOW() as floor_updated_at,
  NOW() as pricing_updated_at
FROM edition_price_stats
ON CONFLICT (edition_id) DO UPDATE SET
  low_ask = EXCLUDED.low_ask,
  average_sale_price = EXCLUDED.average_sale_price,
  top_sale = EXCLUDED.top_sale,
  total_sales = EXCLUDED.total_sales,
  floor_updated_at = EXCLUDED.floor_updated_at,
  pricing_updated_at = EXCLUDED.pricing_updated_at;

-- ============================================================================
-- STEP 4: Create initial ownership_history from wallet_holdings
-- ============================================================================
-- Note: This creates TRANSFER events for current ownership
-- Full history would require blockchain event parsing
INSERT INTO ownership_history (
  nft_id,
  from_wallet,
  to_wallet,
  event_type,
  event_timestamp
)
SELECT 
  nft_id,
  NULL as from_wallet,  -- Unknown source
  wallet_address as to_wallet,
  'TRANSFER' as event_type,
  COALESCE(last_event_ts, NOW()) as event_timestamp
FROM wallet_holdings
ON CONFLICT DO NOTHING;  -- ownership_history has no unique constraint, just append

-- ============================================================================
-- Verification Queries
-- ============================================================================
DO $$ 
DECLARE
  nft_count INTEGER;
  ownership_count INTEGER;
  pricing_count INTEGER;
  history_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO nft_count FROM nfts;
  SELECT COUNT(*) INTO ownership_count FROM ownership;
  SELECT COUNT(*) INTO pricing_count FROM edition_pricing;
  SELECT COUNT(*) INTO history_count FROM ownership_history;
  
  RAISE NOTICE '';
  RAISE NOTICE '========================================';
  RAISE NOTICE 'Migration Complete!';
  RAISE NOTICE '========================================';
  RAISE NOTICE 'nfts table: % rows', nft_count;
  RAISE NOTICE 'ownership table: % rows', ownership_count;
  RAISE NOTICE 'edition_pricing table: % rows', pricing_count;
  RAISE NOTICE 'ownership_history table: % rows', history_count;
  RAISE NOTICE '========================================';
END $$;
