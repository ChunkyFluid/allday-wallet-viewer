/**
 * Fix Unknown/Missing Metadata
 * Finds NFTs with missing player data and attempts to fill from various sources
 */

import { pgQuery } from '../db.js';

async function fixUnknownMetadata() {
    console.log('=== Fixing Unknown/Missing Metadata ===\n');

    try {
        // Step 1: Find NFTs with unknown/missing data
        console.log('Step 1: Finding NFTs with missing metadata...');

        const unknowns = await pgQuery(`
      SELECT DISTINCT edition_id, nft_id,
        first_name, last_name, team_name, set_name, tier
      FROM nft_core_metadata_v2
      WHERE first_name IS NULL 
         OR last_name IS NULL 
         OR first_name = '' 
         OR last_name = ''
         OR first_name = 'Unknown'
         OR last_name = 'Unknown'
      LIMIT 100
    `);

        console.log(`Found ${unknowns.rows.length} NFTs with missing/unknown metadata\n`);

        if (unknowns.rows.length === 0) {
            console.log('✅ No unknowns found! Database is clean.');
            process.exit(0);
        }

        // Display sample
        console.log('Sample of unknowns:');
        unknowns.rows.slice(0, 10).forEach((row, i) => {
            console.log(`  ${i + 1}. NFT ${row.nft_id}: ${row.first_name || '?'} ${row.last_name || '?'}`);
            console.log(`      Set: ${row.set_name || 'Unknown'}, Tier: ${row.tier || 'Unknown'}`);
        });

        console.log('\n' + '='.repeat(60));
        console.log('NEXT STEPS TO FIX:');
        console.log('='.repeat(60));
        console.log('');
        console.log('Option 1: Fetch from NFL All Day Official API');
        console.log('  Use FindLabs client or direct GraphQL queries');
        console.log('');
        console.log('Option 2: Fetch from Flow Blockchain');
        console.log('  Query NFT metadata events directly');
        console.log('');
        console.log('Option 3: Manual CSV Import');
        console.log('  Export unknowns, manually fix, re-import');
        console.log('');
        console.log('Which approach would you like to use?');
        console.log('');

        // Export list for manual fixing if needed
        console.log('Exporting list to fix_unknowns.csv...');
        const csvLines = ['edition_id,nft_id,current_first_name,current_last_name,set_name,tier'];
        unknowns.rows.forEach(row => {
            csvLines.push(`${row.edition_id},${row.nft_id},"${row.first_name || ''}","${row.last_name || ''}","${row.set_name || ''}","${row.tier || ''}"`);
        });

        const fs = await import('fs');
        fs.writeFileSync('fix_unknowns.csv', csvLines.join('\n'));
        console.log('✅ Exported to fix_unknowns.csv');

    } catch (error) {
        console.error('Error:', error.message);
        process.exit(1);
    }

    process.exit(0);
}

fixUnknownMetadata();
