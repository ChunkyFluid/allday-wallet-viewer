// Search Routes - NFT search endpoints
import { pgQuery } from "../db.js";

// Advanced NFT search with multiple filters
export function registerSearchRoutes(app) {
    app.get("/api/search-nfts-advanced", async (req, res) => {
        try {
            const player = (req.query.player || "").toString().trim();
            const team = (req.query.team || "").toString().trim();
            const tier = (req.query.tier || "").toString().trim().toUpperCase();
            const set = (req.query.set || "").toString().trim();
            const series = (req.query.series || "").toString().trim();
            const serial = parseInt(req.query.serial) || null;
            const limit = Math.min(parseInt(req.query.limit) || 50, 100);

            const conditions = [];
            const params = [];
            let paramIndex = 1;

            if (player) {
                conditions.push(`CONCAT(COALESCE(m.first_name,''), ' ', COALESCE(m.last_name,'')) ILIKE $${paramIndex}`);
                params.push(`%${player}%`);
                paramIndex++;
            }

            if (team) {
                conditions.push(`m.team_name = $${paramIndex}`);
                params.push(team);
                paramIndex++;
            }

            if (tier) {
                conditions.push(`UPPER(m.tier) = $${paramIndex}`);
                params.push(tier);
                paramIndex++;
            }

            if (set) {
                conditions.push(`m.set_name = $${paramIndex}`);
                params.push(set);
                paramIndex++;
            }

            if (series) {
                conditions.push(`m.series_name = $${paramIndex}`);
                params.push(series);
                paramIndex++;
            }

            if (serial) {
                conditions.push(`m.serial_number = $${paramIndex}`);
                params.push(serial);
                paramIndex++;
            }

            if (conditions.length === 0) {
                return res.status(400).json({ ok: false, error: "At least one filter is required" });
            }

            const whereClause = conditions.join(' AND ');
            params.push(limit);

            const result = await pgQuery(
                `SELECT 
          m.nft_id, m.edition_id, m.first_name, m.last_name, m.team_name,
          m.set_name, m.series_name, m.tier, m.serial_number, m.max_mint_size,
          m.jersey_number,
          h.wallet_address, p.display_name as owner_name
        FROM nft_core_metadata_v2 m
        LEFT JOIN wallet_holdings h ON h.nft_id = m.nft_id
        LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
        WHERE ${whereClause}
        ORDER BY 
          CASE m.tier 
            WHEN 'ULTIMATE' THEN 1 
            WHEN 'LEGENDARY' THEN 2 
            WHEN 'RARE' THEN 3 
            WHEN 'UNCOMMON' THEN 4 
            ELSE 5 
          END,
          m.serial_number ASC
        LIMIT $${paramIndex}`,
                params
            );

            return res.json({
                ok: true,
                count: result.rowCount,
                rows: result.rows
            });
        } catch (err) {
            console.error("Error in /api/search-nfts-advanced:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });

    // Search NFTs by player name, team, set, series, etc.
    app.get("/api/search-nfts", async (req, res) => {
        try {
            const q = (req.query.q || "").toString().trim();
            const limit = Math.min(parseInt(req.query.limit) || 20, 50);

            if (!q || q.length < 2) {
                return res.status(400).json({ ok: false, error: "Search query too short (min 2 chars)" });
            }

            const pattern = `%${q}%`;

            // Search by player name, team, set name, or series
            const result = await pgQuery(
                `SELECT 
          m.nft_id, m.edition_id, m.first_name, m.last_name, m.team_name,
          m.set_name, m.series_name, m.tier, m.serial_number, m.max_mint_size,
          m.jersey_number,
          h.wallet_address, p.display_name as owner_name
        FROM nft_core_metadata_v2 m
        LEFT JOIN wallet_holdings h ON h.nft_id = m.nft_id
        LEFT JOIN wallet_profiles p ON p.wallet_address = h.wallet_address
        WHERE 
          CONCAT(COALESCE(m.first_name,''), ' ', COALESCE(m.last_name,'')) ILIKE $1
          OR m.team_name ILIKE $1
          OR m.set_name ILIKE $1
          OR m.series_name ILIKE $1
          OR m.nft_id::text = $2
          OR m.edition_id::text = $2
        ORDER BY 
          CASE m.tier 
            WHEN 'ULTIMATE' THEN 1 
            WHEN 'LEGENDARY' THEN 2 
            WHEN 'RARE' THEN 3 
            WHEN 'UNCOMMON' THEN 4 
            ELSE 5 
          END,
          m.serial_number ASC
        LIMIT $3`,
                [pattern, q, limit]
            );

            return res.json({
                ok: true,
                count: result.rowCount,
                rows: result.rows
            });
        } catch (err) {
            console.error("Error in /api/search-nfts:", err);
            return res.status(500).json({ ok: false, error: err.message });
        }
    });
}
