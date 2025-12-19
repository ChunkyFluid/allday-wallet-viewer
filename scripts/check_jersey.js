import { pgQuery } from '../db.js';

async function checkJersey() {
    // Check if jersey_number is populated for any records
    const result = await pgQuery(`
        SELECT 
            COUNT(*) AS total,
            COUNT(jersey_number) AS with_jersey,
            COUNT(*) FILTER (WHERE jersey_number > 0) AS with_valid_jersey
        FROM nft_core_metadata_v2
    `);
    console.log('Jersey number stats:', result.rows[0]);

    // Check Watson specifically
    const watson = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, jersey_number, team_name 
        FROM nft_core_metadata_v2 
        WHERE last_name ILIKE 'Watson' AND first_name ILIKE 'Deshaun' 
        LIMIT 5
    `);
    console.log('Watson moments:', watson.rows);

    // Check plays table  
    const plays = await pgQuery(`
        SELECT play_id, first_name, last_name, jersey_number
        FROM plays
        WHERE last_name ILIKE 'Watson' AND first_name ILIKE 'Deshaun'
        LIMIT 5
    `);
    console.log('Plays table Watson:', plays.rows);

    process.exit(0);
}

checkJersey().catch(e => { console.error(e); process.exit(1); });
