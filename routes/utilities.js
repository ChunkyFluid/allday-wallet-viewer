// Utility Routes - Serial finder and wallet comparison endpoints
import { pgQuery } from "../db.js";

/**
 * Register utility routes
 */
export function registerUtilityRoutes(app) {
    // Serial Finder - find NFTs with specific serial numbers
    app.get("/api/serial-finder", async (req, res) => {
        try {
            const serial = parseInt(req.query.serial);
            const tierParam = (req.query.tier || "").trim();
            const player = (req.query.player || "").trim();
            const team = (req.query.team || "").trim();
            const seriesParam = (req.query.series || "").trim();
            const setParam = (req.query.set || "").trim();
            const positionParam = (req.query.position || "").trim();

            // Parse comma-separated values for multi-select filters
            const tiers = tierParam ? tierParam.split(',').map(t => t.trim().toUpperCase()).filter(Boolean) : [];
            const seriesList = seriesParam ? seriesParam.split(',').map(s => s.trim()).filter(Boolean) : [];
            const sets = setParam ? setParam.split(',').map(s => s.trim()).filter(Boolean) : [];
            const positions = positionParam ? positionParam.split(',').map(p => p.trim().toUpperCase()).filter(Boolean) : [];

            if (!serial || serial < 1) return res.status(400).json({ ok: false, error: "Missing ?serial=" });

            let query = `
        SELECT 
          h.wallet_address, h.is_locked,
          m.nft_id, m.first_name, m.last_name, m.team_name, m.tier, m.set_name,
          m.serial_number, m.jersey_number, m.series_name, m.position,
          p.display_name
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
        WHERE m.serial_number = $1
      `;
            const params = [serial];
            let paramIndex = 2;

            // Tier filter (multi-select with IN clause)
            if (tiers.length > 0) {
                const placeholders = tiers.map((_, i) => `$${paramIndex + i}`).join(', ');
                query += ` AND UPPER(m.tier) IN (${placeholders})`;
                params.push(...tiers);
                paramIndex += tiers.length;
            }

            if (player) {
                query += ` AND TRIM(COALESCE(m.first_name, '') || ' ' || COALESCE(m.last_name, '')) ILIKE $${paramIndex}`;
                params.push(player);
                paramIndex++;
            }

            if (team) {
                query += ` AND m.team_name ILIKE $${paramIndex}`;
                params.push(`%${team}%`);
                paramIndex++;
            }

            // Series filter (multi-select)
            if (seriesList.length > 0) {
                const conditions = seriesList.map((_, i) => `m.series_name ILIKE $${paramIndex + i}`).join(' OR ');
                query += ` AND (${conditions})`;
                seriesList.forEach(s => params.push(`%${s}%`));
                paramIndex += seriesList.length;
            }

            // Set filter (multi-select)
            if (sets.length > 0) {
                const conditions = sets.map((_, i) => `m.set_name ILIKE $${paramIndex + i}`).join(' OR ');
                query += ` AND (${conditions})`;
                sets.forEach(s => params.push(`%${s}%`));
                paramIndex += sets.length;
            }

            // Position filter (multi-select with IN clause)
            if (positions.length > 0) {
                const placeholders = positions.map((_, i) => `$${paramIndex + i}`).join(', ');
                query += ` AND UPPER(m.position) IN (${placeholders})`;
                params.push(...positions);
                paramIndex += positions.length;
            }

            query += ` ORDER BY m.tier DESC, m.last_name LIMIT 500`;

            const result = await pgQuery(query, params);

            const uniqueOwners = new Set(result.rows.map(r => r.wallet_address)).size;
            const jerseyMatches = result.rows.filter(r => r.serial_number == r.jersey_number).length;

            return res.json({
                ok: true,
                serial,
                count: result.rowCount,
                unique_owners: uniqueOwners,
                jersey_matches: jerseyMatches,
                rows: result.rows
            });
        } catch (err) {
            console.error("Error in /api/serial-finder:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });

    // Wallet Comparison - compare two wallets
    app.get("/api/wallet-compare", async (req, res) => {
        try {
            const w1 = (req.query.wallet1 || "").toString().trim().toLowerCase();
            const w2 = (req.query.wallet2 || "").toString().trim().toLowerCase();

            if (!w1 || !w2) return res.status(400).json({ ok: false, error: "Missing wallet1 or wallet2" });

            const statsQuery = `
        SELECT 
          h.wallet_address,
          p.display_name,
          COUNT(*)::int as total,
          COUNT(*) FILTER (WHERE h.is_locked)::int as locked,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'COMMON')::int as common,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'UNCOMMON')::int as uncommon,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'RARE')::int as rare,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'LEGENDARY')::int as legendary,
          COUNT(*) FILTER (WHERE UPPER(m.tier) = 'ULTIMATE')::int as ultimate,
          COALESCE(SUM(eps.lowest_ask_usd), 0)::numeric as floor_value
        FROM wallet_holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
        LEFT JOIN edition_price_scrape eps ON eps.edition_id = m.edition_id
        WHERE h.wallet_address = $1
        GROUP BY h.wallet_address, p.display_name
      `;

            const [r1, r2] = await Promise.all([
                pgQuery(statsQuery, [w1]),
                pgQuery(statsQuery, [w2])
            ]);

            const s1 = r1.rows[0] || { total: 0, locked: 0, common: 0, uncommon: 0, rare: 0, legendary: 0, ultimate: 0, floor_value: 0 };
            const s2 = r2.rows[0] || { total: 0, locked: 0, common: 0, uncommon: 0, rare: 0, legendary: 0, ultimate: 0, floor_value: 0 };

            // Shared editions
            const sharedResult = await pgQuery(
                `SELECT COUNT(DISTINCT m1.edition_id)::int as shared_editions,
                COUNT(DISTINCT CONCAT(m1.first_name, m1.last_name))::int as shared_players
         FROM wallet_holdings h1
         JOIN nft_core_metadata_v2 m1 ON m1.nft_id = h1.nft_id
         WHERE h1.wallet_address = $1
           AND m1.edition_id IN (
             SELECT m2.edition_id FROM wallet_holdings h2
             JOIN nft_core_metadata_v2 m2 ON m2.nft_id = h2.nft_id
             WHERE h2.wallet_address = $2
           )`,
                [w1, w2]
            );

            return res.json({
                ok: true,
                wallet1: { address: w1, display_name: s1.display_name, ...s1 },
                wallet2: { address: w2, display_name: s2.display_name, ...s2 },
                shared: sharedResult.rows[0] || { shared_editions: 0, shared_players: 0 }
            });
        } catch (err) {
            console.error("Error in /api/wallet-compare:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });
}
