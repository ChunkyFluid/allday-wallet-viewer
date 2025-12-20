/**
 * Fix ??? Team Names - Restore Team Moment metadata
 */

import { pgQuery } from '../db.js';

async function fixTeamMoments() {
    console.log('\n=== FIXING TEAM MOMENT METADATA ===\n');

    console.log('[1/3] Finding Team Moments with missing team data...');
    const broken = await pgQuery(`
    SELECT nft_id, edition_id 
    FROM moments 
    WHERE (team_name IS NULL OR team_name = '' OR team_name = '???')
    AND nft_id IS NOT NULL
    LIMIT 100
  `);

    console.log(`      Found ${broken.rows.length} moments with missing team data\n`);

    if (broken.rows.length === 0) {
        console.log('      ✅ No broken team data found!\n');
        process.exit(0);
    }

    console.log('[2/3] Checking if these are Team Moments (special card type)...');

    // Team Moments have edition_id pattern but missing player data
    // They should show team name instead of player name
    const teamMomentEditions = broken.rows.filter(r => r.edition_id && r.edition_id.includes('TEAM'));

    console.log(`      Found ${teamMomentEditions.length} Team Moment editions\n`);

    console.log('[3/3] Updating metadata from edition IDs...');

    let updated = 0;
    for (const row of teamMomentEditions) {
        // Team Moments have team abbreviation in edition_id
        // Example: "TEAM_PIT_S1_BASE" = Pittsburgh
        const parts = row.edition_id.split('_');
        if (parts.length >= 2 && parts[0] === 'TEAM') {
            const teamAbbr = parts[1];

            // Map abbreviation to full name (you can expand this)
            const teamMap = {
                'PIT': 'Pittsburgh Steelers',
                'PHI': 'Philadelphia Eagles',
                'BAL': 'Baltimore Ravens',
                'DEN': 'Denver Broncos',
                'CLE': 'Cleveland Browns',
                'KC': 'Kansas City Chiefs'
                // Add more as needed
            };

            const teamName = teamMap[teamAbbr] || teamAbbr;

            await pgQuery(`
        UPDATE moments 
        SET team_name = $1, 
            first_name = 'Team Moment',
            last_name = $2
        WHERE nft_id = $3
      `, [teamName, teamName, row.nft_id]);

            updated++;
            if (updated % 10 === 0) {
                console.log(`      Progress: ${updated}/${teamMomentEditions.length}`);
            }
        }
    }

    console.log(`      ✅ Updated ${updated} Team Moments\n`);

    // For non-Team editions, try to fetch from API
    console.log('      Checking non-Team moments...');
    const nonTeam = broken.rows.filter(r => !r.edition_id || !r.edition_id.includes('TEAM'));

    if (nonTeam.length > 0) {
        console.log(`      ⚠️  ${nonTeam.length} moments still need metadata from API`);
        console.log(`      These may need a metadata refresh script\n`);
    }

    console.log('=== COMPLETE ===\n');
    process.exit(0);
}

fixTeamMoments().catch(err => {
    console.error('\n❌ ERROR:', err.message);
    process.exit(1);
});
