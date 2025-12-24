import { pgQuery } from "./db.js";
import fs from 'fs';
import * as dotenv from 'dotenv';
dotenv.config();

const WALLET = '0xcfd9bad75352b43b';

async function cleanSpecificGhosts() {
    console.log(`Starting Surgical Cleanup for ${WALLET}...`);

    try {
        // Read the ghosts file
        let raw = fs.readFileSync('ghosts.json', 'utf8');
        // Strip BOM if present
        if (raw.charCodeAt(0) === 0xFEFF) {
            raw = raw.slice(1);
        }
        const ghosts = JSON.parse(raw);

        // Extract unique IDs
        const ghostIds = [...new Set(ghosts.map(g => g.id))];
        console.log(`Loaded ${ghostIds.length} unique ghost IDs to remove.`);

        if (ghostIds.length === 0) {
            console.log("No ghosts to clean.");
            process.exit(0);
        }

        // Force strings just in case
        const ids = ghostIds.map(String);

        // Execute DELETE
        // We delete from wallet_holdings regardless of is_locked status if they are in this list
        // because verified events prove they are NOT locked anymore.
        const res = await pgQuery(
            `DELETE FROM wallet_holdings 
             WHERE wallet_address = $1 
             AND nft_id = ANY($2::text[])`,
            [WALLET.toLowerCase(), ids]
        );

        console.log(`✅ Successfully removed ${res.rowCount} ghost records.`);

        // Verify new counts
        const countRes = await pgQuery(
            `SELECT is_locked, COUNT(*) as count FROM wallet_holdings WHERE wallet_address = $1 GROUP BY 1`,
            [WALLET.toLowerCase()]
        );
        console.log("New Counts:", countRes.rows);

    } catch (err) {
        console.error("Error cleaning ghosts:", err.message);
    }
    process.exit(0);
}

cleanSpecificGhosts();
