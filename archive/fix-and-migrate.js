import { pgQuery } from './db.js';

async function fixSchemaAndMigrate() {
    try {
        console.log('=== Fixing Schema and Re-running Migration ===\n');

        // Drop and recreate nfts table with correct schema
        console.log('Step 1: Dropping and recreating nfts table...');

        await pgQuery(`
      DROP TABLE IF EXISTS ownership_history CASCADE;
      DROP TABLE IF EXISTS ownership CASCADE;
      DROP TABLE IF EXISTS nfts CASCADE;
      DROP TABLE IF EXISTS edition_pricing CASCADE;
    `);

        console.log('✅ Old tables dropped\n');

        // Now run the creation scripts again
        console.log('Step 2: Running migration...\n');

        const fs = await import('fs');
        const path = await import('path');
        const { fileURLToPath } = await import('url');

        const __filename = fileURLToPath(import.meta.url);
        const __dirname = path.dirname(__filename);

        const createSchema = fs.readFileSync(
            path.join(__dirname, 'db', 'migrations', '001_create_new_schema.sql'),
            'utf8'
        );

        await pgQuery(createSchema);
        console.log('✅ Schema created\n');

        const migrateData = fs.readFileSync(
            path.join(__dirname, 'db', 'migrations', '002_migrate_data.sql'),
            'utf8'
        );

        await pgQuery(migrateData);
        console.log('✅ Data migrated\n');

        console.log('Success! Checking results...\n');

        const stats = await pgQuery(`
      SELECT 
        (SELECT COUNT(*) FROM nfts) as nfts,
        (SELECT COUNT(*) FROM ownership) as ownership,
        (SELECT COUNT(*) FROM edition_pricing) as pricing
    `);

        console.log('New tables populated:');
        console.log(`  nfts: ${stats.rows[0].nfts}`);
        console.log(`  ownership: ${stats.rows[0].ownership}`);
        console.log(`  edition_pricing: ${stats.rows[0].pricing}`);

    } catch (error) {
        console.error('Error:', error.message);
        console.error(error);
        process.exit(1);
    }

    process.exit(0);
}

fixSchemaAndMigrate();
