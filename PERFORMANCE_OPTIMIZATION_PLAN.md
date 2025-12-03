# Performance Optimization Plan

This document outlines optimization strategies to dramatically improve ETL speed and overall application performance.

**Status:** Phase 1 Quick Wins ✅ COMPLETE | Phase 2+ IN PROGRESS

## Current Bottlenecks

1. **ETL Process:**
   - Sequential batch processing from Snowflake (rate limiting protects but slows down)
   - Most snapshots use TRUNCATE + INSERT (full rebuild every time)
   - Single-threaded processing
   - Running on same server as API (Render) - causes contention
   - Connection pool limit: 10 connections (may be bottleneck during ETL)

2. **Database:**
   - ~~Missing indexes on frequently queried columns~~ ✅ **FIXED**
   - ~~Large JOINs on every wallet summary request~~ ✅ **FIXED** (now uses snapshot)
   - Most snapshots still use TRUNCATE (top_wallets, editions, etc.)
   - No VACUUM/ANALYZE maintenance scheduled
   - Neon serverless may have connection limits (10 max currently)

3. **Query Performance:**
   - ~~Wallet summaries computed on-the-fly~~ ✅ **FIXED** (uses snapshot now)
   - No caching layer (Redis/memory) for API responses
   - Some snapshots could benefit from incremental updates

## Optimization Strategies

### 🚀 Quick Wins (High Impact, Low Effort)

#### 1. Add Critical Database Indexes
**Impact:** 10-100x faster queries  
**Effort:** Low  
**Files:** New migration script

```sql
-- Indexes for wallet_holdings
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_address ON wallet_holdings(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_nft_id ON wallet_holdings(nft_id);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_last_event_ts ON wallet_holdings(last_event_ts);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_nft ON wallet_holdings(wallet_address, nft_id);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_locked ON wallet_holdings(wallet_address, is_locked);

-- Indexes for nft_core_metadata
CREATE INDEX IF NOT EXISTS idx_nft_metadata_edition_id ON nft_core_metadata(edition_id);
CREATE INDEX IF NOT EXISTS idx_nft_metadata_tier ON nft_core_metadata(tier);
CREATE INDEX IF NOT EXISTS idx_nft_metadata_team ON nft_core_metadata(team_name);

-- Indexes for edition_price_scrape
CREATE INDEX IF NOT EXISTS idx_price_scrape_edition_id ON edition_price_scrape(edition_id);

-- Indexes for wallet_profiles
CREATE INDEX IF NOT EXISTS idx_wallet_profiles_address ON wallet_profiles(wallet_address);
```

#### 2. Create Wallet Summary Snapshot Table ✅ **COMPLETE**
**Impact:** 100-1000x faster wallet lookups  
**Effort:** Medium  
**Status:** ✅ Implemented  
**Files:** `etl_wallet_summary_snapshot.js`, `server.js` (updated API)

✅ **Already Implemented:**
- Uses UPSERT (ON CONFLICT) for incremental updates (no truncate!)
- Automatically removes deleted wallets
- API falls back to live query if snapshot missing
- Significantly faster wallet lookups (5-50ms vs 500ms-5s)

**Future Enhancement:** Only update wallets that changed since last sync (see #3)

```sql
CREATE TABLE IF NOT EXISTS wallet_summary_snapshot (
    wallet_address TEXT PRIMARY KEY,
    display_name TEXT,
    moments_total INT,
    locked_count INT,
    unlocked_count INT,
    tier_common INT,
    tier_uncommon INT,
    tier_rare INT,
    tier_legendary INT,
    tier_ultimate INT,
    floor_value NUMERIC,
    asp_value NUMERIC,
    priced_moments INT,
    last_synced_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_summary_display_name ON wallet_summary_snapshot(display_name);
```

#### 3. Incremental Snapshot Updates
**Impact:** 5-10x faster ETL for snapshots  
**Effort:** Medium-High  
**Files:** Update snapshot ETL scripts  
**Priority:** High (will save significant time)

**Current State:**
- ✅ `wallet_summary_snapshot` - Already uses UPSERT (incremental)
- ❌ `top_wallets_snapshot` - Uses TRUNCATE (full rebuild)
- ❌ `editions_snapshot` - Uses TRUNCATE (full rebuild)
- ❌ `top_wallets_by_*` snapshots - Use TRUNCATE (full rebuild)

**Implementation Strategy:**
1. Track which wallets changed since last holdings sync (via `last_synced_at`)
2. For snapshots like `top_wallets_snapshot`, update only changed wallets:
   ```sql
   -- Delete changed wallets first, then UPSERT
   DELETE FROM top_wallets_snapshot 
   WHERE wallet_address IN (SELECT DISTINCT wallet_address FROM wallet_holdings WHERE last_synced_at > $last_sync);
   
   -- Then UPSERT only those wallets
   INSERT INTO top_wallets_snapshot ...
   ON CONFLICT (wallet_address) DO UPDATE SET ...
   ```
3. Full rebuild only weekly/monthly for data integrity

#### 4. Parallel Batch Processing
**Impact:** 3-5x faster ETL (Snowflake sync)  
**Effort:** Medium  
**Files:** Update `scripts/sync_*_from_snowflake.js`  
**Priority:** Medium (rate limiting already protects Snowflake)

**Current State:**
- ✅ Rate limiting protects Snowflake (2 req/sec default)
- ❌ Batches processed sequentially
- ⚠️ Need to be careful not to overwhelm despite rate limiting

**Implementation Strategy:**
Process multiple Snowflake queries in parallel (each still rate-limited):
```javascript
const BATCHES_IN_PARALLEL = 3;
const batches = []; // Array of batch promises

for (let i = 0; i < BATCHES_IN_PARALLEL; i++) {
    batches.push(fetchBatch(startOffset + i * BATCH_SIZE));
}

const results = await Promise.all(batches);
// Then merge results and insert
```

**Alternative:** Parallel Postgres inserts (multiple connections) while serializing Snowflake queries

### 📊 Medium-Term Improvements

#### 5. Optimize Connection Pooling
**Impact:** Better concurrency, less connection exhaustion  
**Effort:** Low  
**Files:** `db.js`, `db/pool.js`  
**Priority:** High

**Current:** Max 10 connections (may be bottleneck during ETL + API requests)

**Recommendations:**
```javascript
// Increase pool size for ETL scripts
const pool = new Pool({
    // ... other config
    max: 20, // Increase for ETL (Neon free tier allows 25)
    min: 2,  // Keep minimum connections alive
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

// Separate pools for ETL vs API (if needed)
const etlPool = new Pool({ max: 15, ... }); // ETL gets more connections
const apiPool = new Pool({ max: 10, ... });  // API uses fewer
```

#### 6. Database Maintenance (VACUUM/ANALYZE)
**Impact:** Faster queries, better query planning  
**Effort:** Low  
**Files:** New maintenance script  
**Priority:** Medium

Run periodic maintenance:
```sql
-- Update table statistics (helps query planner)
ANALYZE wallet_holdings;
ANALYZE nft_core_metadata;
ANALYZE wallet_summary_snapshot;

-- Clean up dead tuples (if using updates/deletes)
VACUUM ANALYZE wallet_holdings;
```

Schedule weekly or after major ETL runs.

#### 7. Materialized Views Instead of Tables
**Impact:** Faster query planning, automatic refresh options  
**Effort:** Medium  
**Files:** Migrate snapshot tables  
**Priority:** Low (tables work fine, MV is marginal improvement)

Use PostgreSQL materialized views for:
- Automatic refresh scheduling
- Better query optimization hints
- Concurrent refresh option

```sql
CREATE MATERIALIZED VIEW top_wallets_snapshot AS ...;

-- Refresh concurrently (doesn't block reads)
REFRESH MATERIALIZED VIEW CONCURRENTLY top_wallets_snapshot;
```

#### 8. Redis Caching Layer
**Impact:** Eliminate database load for popular wallets  
**Effort:** Medium-High  
**Files:** New caching service  
**Cost:** $10-20/mo (Upstash Redis)  
**Priority:** High (after indexes + snapshots)

**Cache Strategy:**
- Wallet summaries: 5-15 min TTL (invalidate on ETL completion)
- Top wallets lists: 10-30 min TTL
- Edition prices: 1 hour TTL
- Explorer filters: 30 min TTL

**Implementation:**
```javascript
// Simple in-memory cache first (no Redis needed)
const cache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

function getCached(key) {
    const item = cache.get(key);
    if (item && Date.now() < item.expires) {
        return item.data;
    }
    cache.delete(key);
    return null;
}

function setCached(key, data, ttl = CACHE_TTL) {
    cache.set(key, { data, expires: Date.now() + ttl });
}
```

**Redis Benefits:**
- Shared cache across multiple API instances
- Persists across restarts
- Better memory management
- Atomic operations

#### 9. Background Job Queue
**Impact:** Non-blocking ETL, better reliability  
**Effort:** Medium-High  
**Files:** New job queue system  
**Priority:** Medium

**Options:**
1. **pg-boss** (Postgres-based, no extra service) - Recommended
2. **BullMQ** (Redis-based, requires Redis)
3. **Render Cron Jobs** (Simplest, but less control)

**Benefits:**
- ETL runs without blocking API requests
- Automatic retries on failure
- Job monitoring/status
- Can queue multiple jobs

**Simple Implementation (pg-boss):**
```javascript
import PgBoss from 'pg-boss';

const boss = new PgBoss(process.env.DATABASE_URL);

await boss.start();
await boss.work('etl-full-refresh', async (job) => {
    await runETL('full');
});
```

#### 10. Read Replicas
**Impact:** Separate read/write load, zero ETL impact on API  
**Effort:** High (requires Neon upgrade)  
**Cost:** $20-50/mo additional  
**Priority:** Low (only if API still slow after other optimizations)

Use Neon read replicas:
- ETL writes to primary
- API reads from replica
- No contention during ETL
- Automatic replication lag (<1s typically)

**When to Consider:**
- If API still has slowdowns during ETL after implementing other optimizations
- If you have budget for additional infrastructure
- If you need to scale to multiple API instances

### 🏗️ Long-Term Architecture Changes

#### 11. Separate ETL Service
**Impact:** Isolated, scalable ETL, zero impact on API  
**Effort:** High  
**Cost:** $10-30/mo  
**Priority:** Medium (if Render keeps crashing during ETL)

**Options:**
1. **Render Cron Job** - Simplest, uses existing Render account
   - Scheduled job that runs ETL script
   - Separate from web service
   - Free tier available

2. **AWS Lambda + EventBridge** - Serverless, pay-per-use
   - Scheduled via EventBridge (cron)
   - Scales automatically
   - 15 min timeout limit (may need Step Functions for longer)

3. **Google Cloud Run** - Container-based, scheduled
   - Can run for hours if needed
   - Pay for compute time only

4. **Railway/Heroku Scheduler** - Simple cron-like service

**Benefits:**
- ETL can't crash your API service
- Can use more resources for ETL (faster)
- Better monitoring/logging separation
- Cost-effective (ETL runs 1-2x per day)

#### 12. Query Optimization
**Impact:** 2-5x faster queries  
**Effort:** Medium  
**Files:** Review and optimize slow queries  
**Priority:** Medium

**Optimizations:**
1. Use `EXPLAIN ANALYZE` to identify slow queries
2. Avoid `SELECT *` - only select needed columns
3. Use `LIMIT` where appropriate
4. Optimize JOINs (ensure indexes on join keys)
5. Consider covering indexes for frequent queries
6. Use `PREPARE` statements for repeated queries

**Example covering index:**
```sql
-- Index that includes all columns needed for query
CREATE INDEX idx_wallet_summary_covering ON wallet_summary_snapshot 
(wallet_address) INCLUDE (moments_total, floor_value, asp_value);
```

#### 13. Partition Large Tables
**Impact:** Faster queries, easier maintenance  
**Effort:** High  
**Files:** Migration scripts  
**Priority:** Low (only if tables grow very large >100M rows)

Partition `wallet_holdings` by:
- Date range (monthly partitions) - if you keep historical data
- Or by wallet hash (hash partitioning) - for load balancing

**Only consider if:**
- Table exceeds 50-100 million rows
- Queries are slow despite indexes
- You want to archive old data

```sql
-- Convert to partitioned table
ALTER TABLE wallet_holdings PARTITION BY RANGE (last_event_ts);

CREATE TABLE wallet_holdings_2024_01 PARTITION OF wallet_holdings
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

#### 14. Columnar Storage for Analytics (Optional)
**Impact:** 10-100x faster aggregations (for analytics only)  
**Effort:** Very High  
**Cost:** Additional database or service  
**Priority:** Very Low (probably overkill)

**Only consider if:**
- You need complex analytics queries
- You're doing heavy data science/BI work
- Current queries are still too slow after all other optimizations

Options:
- DuckDB (embedded, local file)
- ClickHouse (separate service)
- TimescaleDB (Postgres extension, hypertables)

## Implementation Priority

### Phase 1: Quick Wins ✅ **COMPLETE**
1. ✅ **Add database indexes** - `scripts/add_performance_indexes.js`
2. ✅ **Create wallet summary snapshot** - `etl_wallet_summary_snapshot.js`
3. ✅ **Update API to use snapshot** - `server.js` updated
4. ✅ **Rate limiting for Snowflake** - `scripts/snowflake-utils.js`
5. ✅ **Incremental sync for holdings** - `--incremental` flag

**Result:** ✅ 10-50x faster wallet queries achieved

### Phase 2: ETL Optimization (3-5 days) - **NEXT PRIORITY**
4. ⏳ **Incremental snapshot updates** - Update other snapshots (top_wallets, editions)
5. ⏳ **Optimize connection pooling** - Increase pool size, separate ETL pool
6. ⏳ **Database maintenance** - Add VACUUM/ANALYZE scheduling
7. ⏳ **Parallel batch processing** - Process multiple batches concurrently (carefully)

**Expected Result:** 5-10x faster ETL, less blocking

### Phase 3: Caching & Reliability (1-2 weeks)
8. ⏳ **In-memory caching** - Simple Map-based cache for API (no Redis needed initially)
9. ⏳ **Background job queue** - Use pg-boss or Render cron for ETL
10. ⏳ **Redis caching** - Upgrade to Redis if needed (after in-memory proves useful)

**Expected Result:** Near-instant API responses, non-blocking ETL

### Phase 4: Architecture Overhaul (2-4 weeks, if needed)
11. ⏳ **Separate ETL service** - Move to Render Cron or Lambda
12. ⏳ **Query optimization** - Profile and optimize slow queries
13. ⏳ **Materialized views** - Migrate snapshots (marginal benefit)
14. ⏳ **Read replicas** - Only if API still slow (budget dependent)
15. ⏳ **Partitioning** - Only if tables >100M rows
16. ⏳ **Analytics DB** - Only if heavy analytics needed

## Cost vs Benefit

| Optimization | Impact | Effort | Cost | Status | Priority |
|-------------|--------|--------|------|--------|----------|
| Add Indexes | ⭐⭐⭐⭐⭐ | ⭐ | $0 | ✅ Done | P0 ✅ |
| Wallet Summary Snapshot | ⭐⭐⭐⭐⭐ | ⭐⭐ | $0 | ✅ Done | P0 ✅ |
| Incremental Holdings Sync | ⭐⭐⭐⭐ | ⭐⭐ | $0 | ✅ Done | P0 ✅ |
| Snowflake Rate Limiting | ⭐⭐⭐⭐ | ⭐⭐ | $0 | ✅ Done | P0 ✅ |
| Incremental Snapshot Updates | ⭐⭐⭐⭐ | ⭐⭐⭐ | $0 | ⏳ Next | P1 |
| Optimize Connection Pool | ⭐⭐⭐ | ⭐ | $0 | ⏳ Next | P1 |
| Database Maintenance | ⭐⭐⭐ | ⭐ | $0 | ⏳ Next | P1 |
| Parallel Processing | ⭐⭐⭐ | ⭐⭐⭐ | $0 | ⏳ Next | P2 |
| In-Memory Cache | ⭐⭐⭐ | ⭐⭐ | $0 | ⏳ Future | P2 |
| Background Jobs | ⭐⭐⭐ | ⭐⭐⭐ | $0-5/mo | ⏳ Future | P2 |
| Separate ETL Service | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | $0-30/mo | ⏳ Future | P2 |
| Redis Cache | ⭐⭐⭐ | ⭐⭐⭐ | $10-20/mo | ⏳ Future | P3 |
| Query Optimization | ⭐⭐ | ⭐⭐ | $0 | ⏳ Future | P3 |
| Materialized Views | ⭐⭐ | ⭐⭐⭐ | $0 | ⏳ Future | P3 |
| Read Replicas | ⭐⭐⭐ | ⭐⭐⭐⭐ | $20-50/mo | ⏳ Future | P4 |
| Partitioning | ⭐⭐ | ⭐⭐⭐⭐ | $0 | ⏳ Future | P4 |
| Analytics DB | ⭐⭐ | ⭐⭐⭐⭐⭐ | $20+/mo | ⏳ Future | P4 |

## Recommended Next Steps

### ✅ **Already Complete:**
- Database indexes added
- Wallet summary snapshot implemented
- Incremental holdings sync working
- Snowflake rate limiting in place
- API using snapshot for wallet queries

### 🎯 **Next Priority (This Week):**

1. **Optimize Connection Pool** (30 min)
   ```javascript
   // In db.js or db/pool.js
   max: 20, // Increase from 10
   ```

2. **Incremental Snapshot Updates** (1-2 days)
   - Update `top_wallets_snapshot` to use UPSERT for changed wallets
   - Update `editions_snapshot` to be incremental
   - Keep full rebuild as weekly/monthly option

3. **Database Maintenance Script** (1 hour)
   ```bash
   # Run after major ETL
   node scripts/db_maintenance.js
   ```

4. **Add In-Memory Caching** (2-3 hours)
   - Simple Map-based cache for wallet summaries
   - 5-minute TTL
   - Clear on ETL completion

### 📅 **Next 2 Weeks:**

5. **Background Job Queue** (2-3 days)
   - Use pg-boss or Render Cron
   - Move ETL to scheduled jobs

6. **Parallel Batch Processing** (1 day)
   - Carefully parallelize Snowflake queries
   - Monitor rate limiting

### 🔮 **Future (If Still Needed):**

7. Separate ETL service (if Render keeps crashing)
8. Redis caching (if in-memory not enough)
9. Read replicas (if API still slow during ETL)

## Expected Results

**Current Status:**
- ✅ Wallet queries: **100x faster** (5-50ms vs 500ms-5s)
- ✅ ETL: Still slow but non-blocking with incremental mode
- ✅ Snowflake: Protected from crashes

**After Phase 2:**
- ETL: **5-10x faster** (snapshot incremental updates)
- API: **Instant** (in-memory cache)
- Reliability: **Better** (background jobs, connection optimization)

**After Phase 3:**
- Production-grade performance
- Handle 10x more traffic
- ETL runs in background (no user impact)

## Implementation Notes

### Connection Pool Optimization

Current: `max: 10` connections  
Recommended: `max: 20` for ETL scripts, `max: 10` for API

**Update `db/pool.js`:**
```javascript
const pool = new Pool({
    // ... existing config
    max: parseInt(process.env.DB_POOL_MAX || '10', 10), // Allow override via env
    min: 2,  // Keep connections warm
});
```

**For ETL scripts, create separate pool:**
```javascript
// In ETL scripts
const etlPool = new Pool({
    ...config,
    max: 20, // More connections for ETL
});
```

### Database Maintenance

Run after major ETL runs:
```bash
node scripts/db_maintenance.js
```

Or schedule weekly:
```bash
# Add to cron or scheduled task
0 2 * * 0 node /path/to/scripts/db_maintenance.js
```

This runs `ANALYZE` (updates query planner stats) and `VACUUM` (cleans up dead tuples).

### Monitoring Performance

**Check query performance:**
```sql
-- Find slow queries
SELECT query, mean_exec_time, calls 
FROM pg_stat_statements 
ORDER BY mean_exec_time DESC 
LIMIT 10;
```

**Check index usage:**
```sql
-- See which indexes are actually being used
SELECT schemaname, tablename, indexname, idx_scan 
FROM pg_stat_user_indexes 
ORDER BY idx_scan ASC;
-- Low idx_scan = unused index (can drop to save space)
```

