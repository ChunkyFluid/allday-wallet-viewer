// scripts/snowflake-utils.js
// Shared utilities for Snowflake queries with rate limiting and retry logic

import snowflake from 'snowflake-sdk';

/**
 * Rate limiter using a simple queue
 */
class RateLimiter {
    constructor(requestsPerSecond = 2) {
        this.requestsPerSecond = requestsPerSecond;
        this.minDelay = 1000 / requestsPerSecond; // ms between requests
        this.lastRequestTime = 0;
        this.queue = [];
        this.processing = false;
    }

    async waitForNextSlot() {
        const now = Date.now();
        const timeSinceLastRequest = now - this.lastRequestTime;
        const waitTime = Math.max(0, this.minDelay - timeSinceLastRequest);
        
        if (waitTime > 0) {
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        this.lastRequestTime = Date.now();
    }

    async execute(fn) {
        await this.waitForNextSlot();
        return fn();
    }
}

// Global rate limiter - 2 requests per second by default (can be overridden via env)
const RATE_LIMIT_RPS = parseFloat(process.env.SNOWFLAKE_RATE_LIMIT_RPS || '2');
const rateLimiter = new RateLimiter(RATE_LIMIT_RPS);

/**
 * Execute a Snowflake query with rate limiting and retry logic
 * @param {Object} connection - Snowflake connection
 * @param {string} sqlText - SQL query to execute
 * @param {Object} options - Optional configuration
 * @param {number} options.maxRetries - Maximum retry attempts (default: 3)
 * @param {number} options.initialDelay - Initial delay in ms for exponential backoff (default: 1000)
 * @param {boolean} options.skipRateLimit - Skip rate limiting for this query (default: false)
 */
async function executeSnowflakeWithRetry(connection, sqlText, options = {}) {
    const {
        maxRetries = 3,
        initialDelay = 1000,
        skipRateLimit = false
    } = options;

    const isThrottleError = (err) => {
        const errorCode = err.code || err.codeStr || '';
        const errorMessage = (err.message || '').toLowerCase();
        
        // Common Snowflake throttling/rate limit error codes and messages
        return (
            errorCode === '250001' || // Query timeout
            errorCode === '250005' || // Statement timeout
            errorCode === '390100' || // Resource exhausted
            errorCode === '390101' || // Service unavailable
            errorMessage.includes('rate limit') ||
            errorMessage.includes('throttle') ||
            errorMessage.includes('too many requests') ||
            errorMessage.includes('service unavailable') ||
            errorMessage.includes('resource exhausted') ||
            errorMessage.includes('query timeout') ||
            errorMessage.includes('statement timeout')
        );
    };

    const executeQuery = () => {
        return new Promise((resolve, reject) => {
            connection.execute({
                sqlText,
                complete(err, stmt, rows) {
                    if (err) {
                        return reject(err);
                    }
                    resolve(rows || []);
                }
            });
        });
    };

    let lastError;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            // Apply rate limiting unless skipped
            if (!skipRateLimit) {
                return await rateLimiter.execute(executeQuery);
            } else {
                return await executeQuery();
            }
        } catch (err) {
            lastError = err;
            
            // Check if it's a throttle error
            if (isThrottleError(err)) {
                if (attempt < maxRetries) {
                    // Exponential backoff with jitter
                    const backoffDelay = initialDelay * Math.pow(2, attempt) + Math.random() * 1000;
                    const nextAttempt = attempt + 1;
                    
                    console.warn(`⚠️  Snowflake throttled (attempt ${nextAttempt}/${maxRetries + 1}). Waiting ${Math.round(backoffDelay)}ms before retry...`);
                    console.warn(`   Error: ${err.message || err.code || 'Unknown error'}`);
                    
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    continue;
                } else {
                    console.error(`❌ Snowflake throttling failed after ${maxRetries + 1} attempts`);
                    throw new Error(`Snowflake query failed after ${maxRetries + 1} retries due to throttling: ${err.message || err.code || 'Unknown error'}`);
                }
            } else {
                // Non-throttle error - don't retry (might be a syntax error, etc.)
                console.error(`❌ Snowflake query error (non-retryable):`, err.message || err.code);
                throw err;
            }
        }
    }

    // Should never reach here, but just in case
    throw lastError || new Error('Unknown error executing Snowflake query');
}

/**
 * Add a delay between operations (useful for batch processing)
 */
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Create a Snowflake connection with better error handling
 */
function createSnowflakeConnection() {
    
    const connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        role: process.env.SNOWFLAKE_ROLE
    });

    return new Promise((resolve, reject) => {
        connection.connect((err, conn) => {
            if (err) {
                console.error("❌ Snowflake connect error:", err);
                return reject(err);
            }
            console.log("✅ Connected to Snowflake as", conn.getId());
            console.log(`   Rate limit: ${RATE_LIMIT_RPS} requests/second`);
            resolve(connection);
        });
    });
}

export {
    executeSnowflakeWithRetry,
    delay,
    createSnowflakeConnection,
    rateLimiter
};

