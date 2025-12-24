import { pgQuery } from '../db.js';

async function fixMetadata() {
    console.log("🛠️ Fixing Metadata Mix-up...");

    // 1. Fix NFT 10576026 (Serial #82) -> Josh Allen (RARE)
    // I incorrectly set this to Drake Maye earlier.
    console.log("Updating 10576026 to Josh Allen...");
    await pgQuery(`
        UPDATE nft_core_metadata_v2
        SET 
            first_name = 'Josh',
            last_name = 'Allen',
            team_name = 'Buffalo Bills',
            tier = 'RARE',
            set_name = 'Metallic Gold LE',
            serial_number = 82,
            series_name = '2025 Season'
        WHERE nft_id = '10576026'
    `);

    // 2. Fix NFT 9166770 (Serial #4) -> Drake Maye (LEGENDARY)
    // Check if it needs updates (e.g. if it looks generic)
    // Based on logs it seemed to be 'LEGENDARY', but let's enforce details just in case.
    console.log("Ensuring 9166770 is Drake Maye (Legendary)...");
    await pgQuery(`
        UPDATE nft_core_metadata_v2
        SET 
            first_name = 'Drake',
            last_name = 'Maye',
            team_name = 'New England Patriots',
            tier = 'LEGENDARY',
            set_name = 'Rookie Revelation',
            serial_number = 4,
            series_name = '2024 Season'
        WHERE nft_id = '9166770'
    `);

    // 3. Ensure they are unlocked?
    await pgQuery(`UPDATE holdings SET is_locked = false WHERE nft_id IN ('10576026', '9166770')`);

    console.log("✅ Metadata corrected for Josh Allen and Drake Maye.");
    process.exit();
}

fixMetadata();
