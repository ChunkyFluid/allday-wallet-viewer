# Snowflake Rate Limiting Configuration

This document describes the rate limiting and throttling protection added to prevent overwhelming Snowflake and crashing services.

## Problem
Previous versions of the ETL scripts were making too many concurrent requests to Snowflake, causing:
- Snowflake server overload and crashes
- Render service crashes
- Service downtime

## Solution
All Snowflake queries now use a centralized utility (`scripts/snowflake-utils.js`) that provides:
1. **Rate Limiting**: Limits requests to a configurable rate (default: 2 requests/second)
2. **Automatic Retries**: Retries failed queries with exponential backoff
3. **Throttle Detection**: Automatically detects Snowflake throttling errors and backs off
4. **Batch Delays**: Adds configurable delays between batch operations

## Configuration

### Environment Variables

Add these to your `.env` file to configure rate limiting:

```bash
# Number of requests per second (default: 2)
# Lower = slower but safer, Higher = faster but more risk
SNOWFLAKE_RATE_LIMIT_RPS=2

# Delay in milliseconds between batches (default: 500ms)
# Increase if you still see throttling issues
SNOWFLAKE_BATCH_DELAY_MS=500
```

### Recommended Settings

**For Production (Safe):**
```bash
SNOWFLAKE_RATE_LIMIT_RPS=1      # 1 request per second
SNOWFLAKE_BATCH_DELAY_MS=1000   # 1 second between batches
```

**For Development (Balanced):**
```bash
SNOWFLAKE_RATE_LIMIT_RPS=2      # 2 requests per second
SNOWFLAKE_BATCH_DELAY_MS=500    # 500ms between batches
```

**For Fast Sync (Risky - only if you have high Snowflake capacity):**
```bash
SNOWFLAKE_RATE_LIMIT_RPS=3      # 3 requests per second
SNOWFLAKE_BATCH_DELAY_MS=250    # 250ms between batches
```

## How It Works

### Rate Limiter
- Tracks the last request time
- Ensures minimum delay between requests (1000ms / RPS)
- Queues requests automatically

### Retry Logic
- Automatically retries up to 3 times on throttle errors
- Uses exponential backoff: 1s, 2s, 4s (with jitter)
- Detects common Snowflake throttle error codes:
  - `250001` - Query timeout
  - `250005` - Statement timeout
  - `390100` - Resource exhausted
  - `390101` - Service unavailable
  - Any error message containing "rate limit", "throttle", etc.

### Batch Delays
- Adds a configurable delay between processing batches
- Prevents rapid-fire batch queries
- Can be set to 0 to disable (not recommended)

## Monitoring

The scripts will log rate limiting activity:

```
✅ Connected to Snowflake as [connection-id]
   Rate limit: 2 requests/second
```

If throttling is detected:
```
⚠️  Snowflake throttled (attempt 1/4). Waiting 1234ms before retry...
   Error: [error message]
```

## Troubleshooting

### Still Getting Throttled?

1. **Reduce the rate limit:**
   ```bash
   SNOWFLAKE_RATE_LIMIT_RPS=1
   ```

2. **Increase batch delays:**
   ```bash
   SNOWFLAKE_BATCH_DELAY_MS=2000  # 2 seconds
   ```

3. **Check Snowflake warehouse size:**
   - Larger warehouses can handle more queries
   - Consider upgrading your Snowflake warehouse

### Queries Taking Too Long?

If rate limiting makes ETL too slow:
1. Ensure you're using incremental mode (`--incremental` flag)
2. Consider upgrading Snowflake warehouse size
3. Gradually increase rate limit (test carefully)

## Files Modified

- `scripts/snowflake-utils.js` - New utility with rate limiting
- `scripts/sync_wallet_holdings_from_snowflake.js` - Uses new utility
- `scripts/sync_nft_core_metadata_from_snowflake.js` - Uses new utility
- `scripts/cleanup_removed_holdings.js` - Uses new utility

## Testing

To test rate limiting without running full ETL:

```bash
# Test with very low rate limit to see it in action
SNOWFLAKE_RATE_LIMIT_RPS=0.5 node scripts/sync_wallet_holdings_from_snowflake.js --incremental
```

You should see delays between requests in the logs.

