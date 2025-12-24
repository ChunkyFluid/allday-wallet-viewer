# Moment Sniper Sale Tracking Issues - Analysis & Solutions

## Problem Summary
NFT sales are not being properly tracked in the Moment Sniper system. Specifically:
1. Listings remain marked as "active" even after being sold
2. Wallet holdings don't reflect the sale (NFT stays in seller's wallet, doesn't appear in buyer's wallet)
3. Sale events (`ListingCompleted`) from the blockchain are not being captured or processed

## Root Causes Identified

### 1. **Watcher Gap at Startup** ⚠️ CRITICAL
- The sniper system loads the last 500 listings from the database at startup
- The blockchain watcher then starts monitoring from `current_block - 100`
- **Gap**: Listings created between the oldest DB listing and 100 blocks ago are never tracked
- When these listings sell, the watcher sees the `ListingCompleted` event but has no matching listing, so it skips it

### 2. **Overly Strict Event Matching** ⚠️ HIGH
Current logic in `watchForListings()`:
```javascript
if (!existingListing) {
  sniperLog(`[Sniper] ⚠️  ListingCompleted event... but no matching listing found - skipping`);
  continue;
}
```

**Problem**: If a listing isn't in memory or DB, sales are ignored completely
**Impact**: Legitimate sales are missed, creating stale data

### 3. **Wallet  Integration** ⚠️ MEDIUM
  - Sniper tracks listings (`sniper_listings` table)
- Wallet page tracks holdings (`wallet_holdings` table)
- **No automatic sync between them**
- When the sniper sees a sale, it should:
  1. Mark the listing as sold
  2. Update `wallet_holdings` (remove from seller, add to buyer)
  3. Currently only does #1 (and only if it has the listing tracked)

### 4. **Deposit/Withdraw Events Not Integrated** ⚠️ HIGH
- The main WebSocket connection subscribes to `AllDay.Deposit` and `AllDay.Withdraw`
- These events ARE being captured for wallet sync
- But the sniper watcher uses a separate Flow REST API polling system
- **They don't communicate**: A `Withdraw` event (NFT leaving seller's wallet) should trigger sniper to check if it was a sale

### 5. **Historical Listing Discovery** ℹ️ LOW
- The sniper only knows about listings it has seen since it started running
- If the server restarts, it loads 500 listings from DB, but any active listings older than that are lost
- No mechanism to discover "currently listed NFTs" from the marketplace contract

## Solutions

### Immediate Fix (Deployed)
1. ✅ Enabled sniper logging (`SNIPER_LOGGING_ENABLED = true`)
2. ✅ Created manual fix script for Saquon Barkley NFT
3. ✅ Cleaned up duplicate wallet holdings

### Short-Term Fixes (Recommended)

#### Fix A: Relax Matching Requirements
Instead of skipping unknown listings, mark them as sold in a "untracked_sales" table:
```javascript
if (!existingListing && wasPurchased) {
  // Log this sale even if we don't have the listing
  await pgQuery(`
    INSERT INTO untracked_sales (nft_id, listing_id, sale_block, logged_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT DO NOTHING
  `, [nftId, listingResourceId, blockHeight]);
  sniperLog(`[Sniper] 📝 Logged untracked sale for NFT ${nftId}`);
}
```

#### Fix B: Integrate Wallet Events
When a `ListingCompleted` event with `purchased=true` is detected:
1. Remove NFT from seller's `wallet_holdings`
2. Add NFT to buyer's `wallet_holdings`

```javascript
if (wasPurchased && buyerAddr && sellerAddr) {
  // Update wallet holdings
  await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = $2`, [sellerAddr, nftId]);
  await pgQuery(`
    INSERT INTO wallet_holdings (wallet_address, nft_id, last_event_ts, last_synced_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
      last_event_ts = $3,
      last_synced_at = NOW()
  `, [buyerAddr, nftId, new Date()]);
  sniperLog(`[Sniper] 💼 Updated wallet holdings: ${sellerAddr} → ${buyerAddr}`);
}
```

#### Fix C: Periodic Listing Verification
Add a background job that periodically queries Flow blockchain for all listings and marks sold/unlisted ones:
- Run every 5-10 minutes
- Query the NFTStorefrontV2 contract for all active listings
- Compare with `sniper_listings` WHERE `is_sold = FALSE AND is_unlisted = FALSE`
- Mark any missing ones as unlisted

#### Fix D: Historical Backfill on Startup
When the server starts:
1. Query the last listing timestamp from `sniper_listings`
2. Calculate the block height at that time
3. Start the watcher from that block (not current - 100)
4. This ensures no gaps

### Long-Term Improvements

1. **Unified Event System**: Merge the AllDay WebSocket events with Storefront REST polling
2. **Blockchain State Queries**: Instead of relying solely on events, periodically query:
   - Which wallets own which NFTs
   - Which NFTs are currently listed
3. **Event Replay**: Store all raw blockchain events in a separate table for reprocessing if needed

## Testing the Fix

1. Check that the Saquon Barkley listing is now marked as sold:
   ```sql
   SELECT * FROM sniper_listings WHERE nft_id = '10508347';
   ```

2. Wait for a new sale to happen and watch the logs:
   ```
   [Sniper] 📋 ListingCompleted for NFT ...
   [Sniper] ✅ Marked listing ... as SOLD
   [Sniper] 💼 Updated wallet holdings
   ```

3. Verify wallet holdings update automatically

## Files Modified
- `server.js` - Enabled sniper logging
- `scripts/fix_saquon_listing.js` - Manual fix for this specific NFT

## Next Steps
1. Implement Fix B (wallet integration) - PRIORITY 1
2. Implement Fix A (untracked sales logging) - PRIORITY 2  
3. Implement Fix C (periodic verification) - PRIORITY 3
4. Test with real sales and verify wallets update correctly
