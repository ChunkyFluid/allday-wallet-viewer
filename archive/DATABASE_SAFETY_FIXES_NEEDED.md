# Database Safety: Scripts That Need Fixing

## ✅ FIXED
- `scripts/sync_wallet_from_blockchain.js` - Removed DELETE statements, now UPSERT only

## ⚠️ NEEDS FIXING - Contains DELETE FROM statements

### High Priority (Wallet Sync Scripts)
1. **`scripts/batch_sync_wallets.js`** - Line 70
   - `DELETE FROM wallet_holdings WHERE wallet_address = $1`
   - Should use UPSERT instead

2. **`scripts/sync_wallets_from_blockchain.js`** - Lines 133, 230
   - Multiple DELETE statements
   - Should use UPSERT instead

3. **`scripts/sync_wallets_from_blockchain_only.js`** - Lines 115, 125, 229
   - Multiple DELETE statements
   - Should use UPSERT instead

### Medium Priority  
4. **`scripts/cleanup_holdings.js`** - Lines 55, 98
   - Might be intentional cleanup, but should verify

### Low Priority (Debug/Test Scripts)
5. **`scripts/debug_listings.js`** - Line 63
   - Test data cleanup (probably OK)

6. **`scripts/fix_listings_constraint.js`** - Lines 16, 63
   - One-time fix script (probably OK)

7. **`scripts/fix_saquon_listing.js`** - Line 45
   - One-time manual fix (completed, can ignore)

## ✨ Best Practice Going Forward

**NEVER use `DELETE FROM` in sync scripts!**

Instead:
```javascript
// ❌ BAD - Deletes all data
await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1`, [wallet]);

// ✅ GOOD - Preserves data, updates what exists, inserts new
await pgQuery(`
    INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET 
      is_locked = COALESCE(EXCLUDED.is_locked, wallet_holdings.is_locked),
      last_event_ts = NOW()
`, [wallet, nftId, isLocked]);
```

## Current Issue
- `holdings` table has correct locked data (from Snowflake)
- `wallet_holdings` was wiped by DELETE statements in sync scripts
- Running `fix_all_locked_status.js` to restore locked data from holdings → wallet_holdings
