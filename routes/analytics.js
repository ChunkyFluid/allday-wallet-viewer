// routes/analytics.js
import crypto from "crypto";
import pool from "../db/pool.js";

/**
 * Ensures visit counter tables exist
 */
export async function initVisitCounterTable() {
    try {
        // 1. Ensure table exists
        await pool.query(`
            CREATE TABLE IF NOT EXISTS site_visits (
                id SERIAL PRIMARY KEY,
                total_count INTEGER DEFAULT 0,
                total_hits INTEGER DEFAULT 0,
                last_updated TIMESTAMPTZ DEFAULT NOW()
            );
        `);

        // 2. Ensure total_hits column exists for legacy tables  
        try {
            await pool.query(`ALTER TABLE site_visits ADD COLUMN IF NOT EXISTS total_hits INTEGER DEFAULT 0`);
        } catch (e) { /* ignore if already exists */ }

        // 3. Ensure seed record exists
        await pool.query(`
            INSERT INTO site_visits (id, total_count, total_hits)  
            VALUES (1, 0, 0) ON CONFLICT (id) DO NOTHING
        `);

        // 4. Ensure log table exists
        await pool.query(`
            CREATE TABLE IF NOT EXISTS unique_visits_log (
                visitor_hash TEXT PRIMARY KEY,
                first_seen TIMESTAMPTZ DEFAULT NOW()
            );
        `);
        console.log("📈 Visit counter initialized");
    } catch (err) {
        console.error("Error initializing visit counter table:", err.message);
    }
}

/**
 * Registers analytics-related routes
 * @param {import('express').Application} app 
 */
export function registerAnalyticsRoutes(app) {
    // POST /api/visit - record a visit and return count
    app.post("/api/visit", async (req, res) => {
        try {
            // Always increment total hits
            await pool.query(`
                UPDATE site_visits
                SET total_hits = total_hits + 1, last_updated = NOW()  
                WHERE id = 1
            `);

            // Basic IP/UA hashing for "uniqueness"
            const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
            const ua = req.headers['user-agent'] || 'unknown';
            const hash = crypto.createHash('sha256').update(ip + ua).digest('hex');

            // Attempt to log the unique visit
            const { rowCount } = await pool.query(`
                INSERT INTO unique_visits_log (visitor_hash)
                VALUES ($1)
                ON CONFLICT (visitor_hash) DO NOTHING
            `, [hash]);

            // If it was a new unique visit, increment the unique count
            if (rowCount > 0) {
                await pool.query(`
                    UPDATE site_visits
                    SET total_count = total_count + 1, last_updated = NOW()
                    WHERE id = 1
                `);
            }

            // Return current counts
            const { rows } = await pool.query(`SELECT total_count, total_hits FROM site_visits WHERE id = 1`);
            const count = rows[0]?.total_count || 0;
            const totalHits = rows[0]?.total_hits || 0;
            return res.json({ ok: true, count, totalHits });
        } catch (err) {
            console.error("POST /api/visit error:", err.message);
            return res.json({ ok: false, count: 0, totalHits: 0 });
        }
    });

    // GET /api/visit-count - get current count without incrementing
    app.get("/api/visit-count", async (req, res) => {
        try {
            const { rows } = await pool.query(`SELECT total_count, total_hits FROM site_visits WHERE id = 1`);
            const count = rows[0]?.total_count || 0;
            const totalHits = rows[0]?.total_hits || 0;
            return res.json({ ok: true, count, totalHits });
        } catch (err) {
            console.error("GET /api/visit-count error:", err.message);
            return res.json({ ok: false, count: 0, totalHits: 0 });
        }
    });
}
