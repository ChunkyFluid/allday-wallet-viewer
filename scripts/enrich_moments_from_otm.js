// scripts/enrich_moments_from_otm.js
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";
import dotenv from "dotenv";
import { parse } from "csv-parse";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const pool = new Pool({
    host: process.env.PGHOST,
    port: process.env.PGPORT ? Number(process.env.PGPORT) : 5432,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
    ssl: { rejectUnauthorized: false }
});

pool.on("error", (err) => {
    console.error("Postgres pool error:", err);
});

function fileExists(filePath) {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

function parseEditionIdFromLink(link) {
    if (!link) return null;
    const str = String(link);
    const m = str.match(/edition\/(\d+)/);
    return m ? m[1] : null;
}

function splitPlayerName(name) {
    if (!name) return { firstName: null, lastName: null };
    const parts = String(name).trim().split(/\s+/);
    if (parts.length === 1) {
        return { firstName: parts[0], lastName: null };
    }
    return {
        firstName: parts[0],
        lastName: parts.slice(1).join(" ")
    };
}

async function enrichFromOTM() {
    const dataDir = path.join(__dirname, "..", "data");
    const filePath = path.join(dataDir, "otm_values.csv");

    console.log("Looking for OTM CSV at:", filePath);

    if (!fileExists(filePath)) {
        console.error(
            "❌ otm_values.csv not found in /data. Put your NFL All Day Values CSV there and name it otm_values.csv."
        );
        await pool.end();
        process.exit(1);
    }

    console.log("Found otm_values.csv, starting enrich (editions: player + team + set metadata)...");

    const client = await pool.connect();

    const stream = fs.createReadStream(filePath).pipe(
        parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        })
    );

    let totalRows = 0;
    let editionsMatched = 0;
    let editionsUpdated = 0;
    let failed = 0;

    try {
        for await (const row of stream) {
            totalRows += 1;

            const link = row["Link"] || row["link"];
            const editionId = parseEditionIdFromLink(link);
            if (!editionId) {
                continue;
            }

            const playerName = row["Player Name"] || row["player name"] || "";
            const { firstName, lastName } = splitPlayerName(playerName);

            const teamName = row["Team"] || row["team"] || null;
            const position = row["Position"] || row["position"] || null;

            const jerseyNumberRaw = row["Jersey Number"] || row["jersey number"] || null;
            const jerseyNumber = jerseyNumberRaw ? Number(String(jerseyNumberRaw).replace(/[^\d]/g, "")) || null : null;

            const setName = row["Set"] || row["set"] || null;
            const seriesName = row["Series"] || row["series"] || null;
            const tier = row["Tier"] || row["tier"] || null;

            const setIdRaw = row["Set ID"] || row["set id"] || null;
            const setId = setIdRaw != null ? String(setIdRaw) : null;

            const playIdRaw = row["Play ID"] || row["play id"] || null;
            const playId = playIdRaw != null ? String(playIdRaw) : null;

            try {
                const edRes = await client.query(
                    `
          UPDATE editions
          SET
            set_id        = COALESCE($2, set_id),
            set_name      = COALESCE($3, set_name),
            series_name   = COALESCE($4, series_name),
            tier          = COALESCE($5, tier),
            play_id       = COALESCE($6, play_id),
            first_name    = COALESCE($7, first_name),
            last_name     = COALESCE($8, last_name),
            team_name     = COALESCE($9, team_name),
            position      = COALESCE($10, position),
            jersey_number = COALESCE($11, jersey_number)
          WHERE edition_id = $1
          `,
                    [
                        editionId,
                        setId,
                        setName,
                        seriesName,
                        tier,
                        playId,
                        firstName,
                        lastName,
                        teamName,
                        position,
                        jerseyNumber
                    ]
                );

                if (edRes.rowCount > 0) {
                    editionsMatched += 1;
                    editionsUpdated += edRes.rowCount;
                }
            } catch (err) {
                failed += 1;
                console.error(
                    `Row failed for edition_id=${editionId}: ${err.code || ""} ${err.message || String(err)}`
                );
            }

            if (totalRows % 500 === 0) {
                console.log(
                    `Processed ${totalRows} OTM rows... editions matched: ${editionsMatched}, failed: ${failed}`
                );
            }
        }

        console.log("==========================================");
        console.log("✅ OTM enrich complete (editions: player + team + set metadata).");
        console.log(`Total OTM rows processed: ${totalRows}`);
        console.log(`Editions matched in editions table: ${editionsMatched}`);
        console.log(`Total editions rows updated: ${editionsUpdated}`);
        console.log(`Failed updates: ${failed}`);
        console.log("==========================================");
    } catch (err) {
        console.error("❌ Fatal error while streaming OTM CSV:", err);
    } finally {
        client.release();
        await pool.end();
    }
}

enrichFromOTM().catch((err) => {
    console.error("Unexpected top-level error in enrichFromOTM:", err);
    process.exit(1);
});
