import { pgQuery } from '../db.js';

async function manualInsert() {
    console.log("Force-inserting metadata for Drake Maye...");

    // Hardcoded for NFT 10576026
    // Based on user report "Lego Drake Maye"
    const nftId = '10576026';

    await pgQuery(`
        INSERT INTO nft_core_metadata_v2 (
            nft_id, 
            first_name,
            last_name, 
            team_name, 
            tier
        ) VALUES (
            $1, $2, $3, $4, $5
        ) ON CONFLICT (nft_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            team_name = EXCLUDED.team_name,
            tier = EXCLUDED.tier
    `, [
        nftId,
        'Drake',
        'Maye',
        'New England Patriots',
        'COMMON'
    ]);

    console.log("✅ Manual Metadata Inserted.");
    process.exit();
}

manualInsert();
