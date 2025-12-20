import { pgQuery } from '../db.js';

async function cleanup() {
    console.log('\n--- DATABASE CLEANUP: DROPPING LEGACY TABLES ---\n');

    const legacyTables = ['nft_core_metadata', 'nfts'];

    for (const table of legacyTables) {
        try {
            console.log(`Dropping ${table}...`);
            await pgQuery(`DROP TABLE IF EXISTS ${table} CASCADE`);
            console.log(`✅ ${table} dropped successfully.`);
        } catch (err) {
            console.error(`❌ Error dropping ${table}:`, err.message);
        }
    }

    console.log('\n--- CLEANUP COMPLETE ---\n');

    // Check remaining size
    const dbSizeQuery = `SELECT pg_size_pretty(pg_database_size(current_database())) as db_size`;
    const dbRes = await pgQuery(dbSizeQuery);
    console.log(`New Database Size: ${dbRes.rows[0].db_size}`);

    process.exit(0);
}

cleanup();
