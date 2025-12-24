// Set Completion Routes - Track set completion progress
import { pgQuery } from "../db.js";

// Cache for set totals (refreshed every 10 minutes)
let setTotalsCache = null;
let setTotalsCacheTime = 0;
const SET_TOTALS_CACHE_TTL = 10 * 60 * 1000; // 10 minutes

async function ensureSetsCatalogTable() {
    await pgQuery(`
    CREATE TABLE IF NOT EXISTS sets_catalog (
      set_name TEXT PRIMARY KEY,
      total_editions INTEGER DEFAULT 0,
      common INTEGER DEFAULT 0,
      uncommon INTEGER DEFAULT 0,
      rare INTEGER DEFAULT 0,
      legendary INTEGER DEFAULT 0,
      ultimate INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT now()
    );
  `);
}

async function refreshSetsCatalog() {
    await ensureSetsCatalogTable();
    const rows = await pgQuery(`
    INSERT INTO sets_catalog (set_name, total_editions, common, uncommon, rare, legendary, ultimate, updated_at)
    SELECT 
      set_name,
      COUNT(DISTINCT edition_id) as total_editions,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'COMMON' THEN edition_id END) as common,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'UNCOMMON' THEN edition_id END) as uncommon,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'RARE' THEN edition_id END) as rare,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'LEGENDARY' THEN edition_id END) as legendary,
      COUNT(DISTINCT CASE WHEN UPPER(tier) = 'ULTIMATE' THEN edition_id END) as ultimate,
      now()
    FROM nft_core_metadata_v2
    WHERE set_name IS NOT NULL
    GROUP BY set_name
    ON CONFLICT (set_name) DO UPDATE SET
      total_editions = EXCLUDED.total_editions,
      common        = EXCLUDED.common,
      uncommon      = EXCLUDED.uncommon,
      rare          = EXCLUDED.rare,
      legendary     = EXCLUDED.legendary,
      ultimate      = EXCLUDED.ultimate,
      updated_at    = now()
    RETURNING set_name;
  `);
    console.log(`[Set Completion] Refreshed sets_catalog (${rows.rowCount} rows)`);
}

async function getSetTotals() {
    const now = Date.now();
    if (setTotalsCache && (now - setTotalsCacheTime) < SET_TOTALS_CACHE_TTL) {
        return setTotalsCache;
    }

    // Fast path: set_totals_snapshot (prebuilt, small)
    try {
        const snap = await pgQuery(`SELECT set_name, total_editions FROM set_totals_snapshot`);
        if (snap.rowCount > 0) {
            setTotalsCache = new Map(snap.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
            setTotalsCacheTime = now;
            console.log(`[Set Completion] Loaded set totals from set_totals_snapshot (${snap.rowCount} rows)`);
            return setTotalsCache;
        }
    } catch (err) {
        console.warn("[Set Completion] set_totals_snapshot not available, falling back:", err.message);
    }

    await ensureSetsCatalogTable();
    // Try to read from catalog if it exists and is fresh
    const catalog = await pgQuery(
        `SELECT set_name, total_editions, updated_at FROM sets_catalog WHERE updated_at > now() - interval '15 minutes'`
    );
    if (catalog.rowCount > 0) {
        setTotalsCache = new Map(catalog.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
        setTotalsCacheTime = now;
        console.log(`[Set Completion] Loaded set totals from sets_catalog (${catalog.rowCount} rows)`);
        return setTotalsCache;
    }

    // Fallback: rebuild catalog, then return as map
    await refreshSetsCatalog();
    const fresh = await pgQuery(`SELECT set_name, total_editions FROM sets_catalog`);
    setTotalsCache = new Map(fresh.rows.map(r => [r.set_name, parseInt(r.total_editions)]));
    setTotalsCacheTime = now;
    console.log(`[Set Completion] Cached ${setTotalsCache.size} set totals (rebuilt)`);
    return setTotalsCache;
}

/**
 * Register set completion routes
 */
export function registerSetRoutes(app) {
    // Public: list all sets with total editions
    app.get("/api/set-completion/all", async (_req, res) => {
        try {
            const setTotals = await getSetTotals();

            const sets = Array.from(setTotals.entries())
                .map(([set_name, total]) => ({
                    set_name,
                    total,
                    is_team_set: false // computed on wallet fetch; keeps this endpoint fast
                }))
                .sort((a, b) => a.set_name.localeCompare(b.set_name));
            return res.json({ ok: true, sets });
        } catch (err) {
            console.error("Error in /api/set-completion/all:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });

    app.get("/api/set-completion", async (req, res) => {
        try {
            const wallet = (req.query.wallet || "").toString().trim().toLowerCase();
            if (!wallet) return res.status(400).json({ ok: false, error: "Missing ?wallet=" });

            const startTime = Date.now();
            let stepTime = Date.now();

            // Get cached set totals (fast - from memory)
            const setTotals = await getSetTotals();
            console.log(`[Set Completion] Step 1 - getSetTotals: ${Date.now() - stepTime}ms`);
            stepTime = Date.now();

            // Query only what the user owns (much faster - single pass)
            const result = await pgQuery(`
        SELECT 
          m.set_name,
          COUNT(DISTINCT m.edition_id) as owned_editions,
          COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'COMMON' THEN m.edition_id END) as common,
          COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'UNCOMMON' THEN m.edition_id END) as uncommon,
          COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'RARE' THEN m.edition_id END) as rare,
          COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'LEGENDARY' THEN m.edition_id END) as legendary,
          COUNT(DISTINCT CASE WHEN UPPER(m.tier) = 'ULTIMATE' THEN m.edition_id END) as ultimate
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND m.set_name IS NOT NULL
        GROUP BY m.set_name
        ORDER BY m.set_name`,
                [wallet]
            );
            console.log(`[Set Completion] Step 2 - owned editions: ${Date.now() - stepTime}ms`);
            stepTime = Date.now();

            // Cost to complete (sum lowest ask of missing editions) using set_editions_snapshot for speed
            const costRes = await pgQuery(
                `
        WITH owned AS (
          SELECT DISTINCT m.edition_id
          FROM wallet_holdings h
          JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
          WHERE h.wallet_address = $1
        ),
        missing AS (
          SELECT s.set_name, s.edition_id, s.lowest_ask_usd
          FROM set_editions_snapshot s
          LEFT JOIN owned o ON s.edition_id = o.edition_id
          WHERE o.edition_id IS NULL
        )
        SELECT set_name, COALESCE(SUM(lowest_ask_usd), 0) AS cost_to_complete
        FROM missing
        GROUP BY set_name;
        `,
                [wallet]
            );
            const costMap = new Map(costRes.rows.map(r => [r.set_name, Number(r.cost_to_complete) || 0]));
            console.log(`[Set Completion] Step 3 - cost to complete: ${Date.now() - stepTime}ms`);
            stepTime = Date.now();

            // Team totals (for team-level tracking)
            const teamTotalsRes = await pgQuery(`
        SELECT team_name, COUNT(DISTINCT edition_id) AS total_editions
        FROM nft_core_metadata_v2
        WHERE team_name IS NOT NULL AND team_name <> ''
        GROUP BY team_name
      `);
            const teamTotals = new Map(teamTotalsRes.rows.map(r => [(r.team_name || "").toLowerCase(), parseInt(r.total_editions)]));
            console.log(`[Set Completion] Step 4 - team totals: ${Date.now() - stepTime}ms`);
            stepTime = Date.now();

            // Merge with cached totals
            const sets = result.rows.map(r => {
                const total = setTotals.get(r.set_name) || 0;
                const owned = parseInt(r.owned_editions);
                const cost = costMap.get(r.set_name) || 0;
                return {
                    set_name: r.set_name,
                    total,
                    owned,
                    completion: total > 0 ? Math.round(owned * 1000 / total) / 10 : 0,
                    cost_to_complete: cost,
                    // classify team set if set name contains a known team name
                    is_team_set: Array.from(teamTotals.keys()).some(t => (r.set_name || "").toLowerCase().includes(t)),
                    by_tier: {
                        Common: parseInt(r.common) || 0,
                        Uncommon: parseInt(r.uncommon) || 0,
                        Rare: parseInt(r.rare) || 0,
                        Legendary: parseInt(r.legendary) || 0,
                        Ultimate: parseInt(r.ultimate) || 0
                    }
                };
            }).sort((a, b) => b.completion - a.completion || a.set_name.localeCompare(b.set_name));

            // Team progress
            const teamOwnedRes = await pgQuery(
                `
        SELECT m.team_name, COUNT(DISTINCT m.edition_id) AS owned_editions
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND m.team_name IS NOT NULL AND m.team_name <> ''
        GROUP BY m.team_name;
        `,
                [wallet]
            );
            const teamProgress = teamOwnedRes.rows.map(r => {
                const key = (r.team_name || "").toLowerCase();
                const total = teamTotals.get(key) || 0;
                const owned = parseInt(r.owned_editions);
                return {
                    team_name: r.team_name,
                    total,
                    owned,
                    completion: total > 0 ? Math.round(owned * 1000 / total) / 10 : 0
                };
            }).sort((a, b) => b.completion - a.completion || a.team_name.localeCompare(b.team_name));

            const summary = {
                total_sets: sets.length,
                completed_sets: sets.filter(s => s.completion === 100).length,
                in_progress: sets.filter(s => s.completion > 0 && s.completion < 100).length,
                avg_completion: sets.length > 0 ? sets.reduce((a, b) => a + b.completion, 0) / sets.length : 0
            };

            const elapsed = Date.now() - startTime;
            console.log(`[Set Completion] Query for ${wallet.substring(0, 10)}... completed in ${elapsed}ms (${sets.length} sets)`);

            return res.json({ ok: true, summary, sets, team_progress: teamProgress });
        } catch (err) {
            console.error("Error in /api/set-completion:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });
}
