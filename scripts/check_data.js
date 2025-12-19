import { pgQuery } from '../db.js';

async function checkMissingNames() {
    // Find moments with missing player names but have team
    console.log('\n=== Checking moments with missing player names ===');

    const missingNames = await pgQuery(`
        SELECT 
            nft_id, 
            first_name, 
            last_name, 
            team_name, 
            set_name,
            tier,
            edition_id,
            play_id,
            serial_number,
            max_mint_size
        FROM nft_core_metadata_v2 
        WHERE (first_name IS NULL OR first_name = '')
           AND (last_name IS NULL OR last_name = '')
           AND team_name IS NOT NULL
        LIMIT 20
    `);

    console.log(`Found ${missingNames.rows.length} moments with missing names but have team:`);
    missingNames.rows.forEach(r => {
        console.log(`  NFT ${r.nft_id}: ${r.first_name || 'NULL'} ${r.last_name || 'NULL'} - Team: ${r.team_name} - Set: ${r.set_name} - play_id: ${r.play_id}`);
    });

    // Check if these play_ids exist in plays table
    if (missingNames.rows.length > 0) {
        const playIds = [...new Set(missingNames.rows.map(r => r.play_id).filter(Boolean))];
        console.log(`\nChecking play_ids in plays table: ${playIds.join(', ')}`);

        if (playIds.length > 0) {
            const plays = await pgQuery(`
                SELECT play_id, first_name, last_name, team_name
                FROM plays
                WHERE play_id = ANY($1::text[])
            `, [playIds]);

            console.log('Plays table data:');
            plays.rows.forEach(p => {
                console.log(`  play_id ${p.play_id}: ${p.first_name || 'NULL'} ${p.last_name || 'NULL'} - Team: ${p.team_name}`);
            });
        }
    }

    // Count total moments with missing names
    const countResult = await pgQuery(`
        SELECT COUNT(*) as count
        FROM nft_core_metadata_v2 
        WHERE (first_name IS NULL OR first_name = '')
           AND (last_name IS NULL OR last_name = '')
    `);
    console.log(`\nTotal moments with missing player names: ${countResult.rows[0].count}`);

    // Check set distribution of missing names
    const bySet = await pgQuery(`
        SELECT set_name, COUNT(*) as count
        FROM nft_core_metadata_v2 
        WHERE (first_name IS NULL OR first_name = '')
           AND (last_name IS NULL OR last_name = '')
        GROUP BY set_name
        ORDER BY count DESC
        LIMIT 10
    `);
    console.log('\nMissing names by set:');
    bySet.rows.forEach(r => console.log(`  ${r.set_name || 'NULL'}: ${r.count}`));

    process.exit(0);
}

checkMissingNames().catch(e => { console.error(e); process.exit(1); });
