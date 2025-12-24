import { pgQuery } from './db.js';

async function cleanupNewSchema() {
    console.log('=== Cleaning Up Failed Migration ===\n');

    try {
        // Drop new tables
        console.log('Dropping new schema tables...');

        await pgQuery(`
      DROP TABLE IF EXISTS ownership_history CASCADE;
      DROP TABLE IF EXISTS ownership CASCADE;
      DROP TABLE IF EXISTS nfts CASCADE;
      DROP TABLE IF EXISTS edition_pricing CASCADE;
    `);

        console.log('✅ New schema tables dropped\n');

        // Verify current schema
        console.log('Verifying current working schema...\n');

        const tables = await pgQuery(`
      SELECT table_name, 
        (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        AND table_name IN ('nft_core_metadata_v2', 'wallet_holdings', 'holdings', 'edition_price_stats')
      ORDER BY table_name
    `);

        console.log('Current working schema:');
        tables.rows.forEach(table => {
            console.log(`  ✅ ${table.table_name} (${table.column_count} columns)`);
        });

        // Check row counts
        console.log('\n\nRow counts:');

        const metadata = await pgQuery(`SELECT COUNT(*) as count FROM nft_core_metadata_v2`);
        console.log(`  nft_core_metadata_v2: ${metadata.rows[0].count.toLocaleString()} NFTs`);

        const holdings = await pgQuery(`SELECT COUNT(*) as count FROM wallet_holdings`);
        console.log(`  wallet_holdings: ${holdings.rows[0].count.toLocaleString()} holdings`);

        const holdingsTable = await pgQuery(`SELECT COUNT(*) as count FROM holdings`);
        console.log(`  holdings: ${holdingsTable.rows[0].count.toLocaleString()} records`);

        const pricing = await pgQuery(`SELECT COUNT(*) as count FROM edition_price_stats`);
        console.log(`  edition_price_stats: ${pricing.rows[0].count.toLocaleString()} editions`);

        console.log('\n✅ Current schema intact and working!');
        console.log('✅ Migration aborted successfully');

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        process.exit();
    }
}

cleanupNewSchema();
