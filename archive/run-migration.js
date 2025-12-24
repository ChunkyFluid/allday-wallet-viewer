import { pgQuery } from './db.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function runMigration() {
    console.log('=== Database Schema Redesign - Phase 1 ===\n');

    try {
        // Step 1: Create new schema
        console.log('Step 1: Creating new schema tables...');
        const createSchema = fs.readFileSync(
            path.join(__dirname, 'db', 'migrations', '001_create_new_schema.sql'),
            'utf8'
        );

        await pgQuery(createSchema);
        console.log('✅ Schema created successfully\n');

        // Step 2: Migrate data
        console.log('Step 2: Migrating data from existing tables...');
        console.log('This may take a few minutes for large databases...\n');

        const migrateData = fs.readFileSync(
            path.join(__dirname, 'db', 'migrations', '002_migrate_data.sql'),
            'utf8'
        );

        await pgQuery(migrateData);
        console.log('✅ Data migration completed\n');

        // Step 3: Verify migration
        console.log('Step 3: Verifying migration...\n');

        const verification = await pgQuery(`
      SELECT 
        (SELECT COUNT(*) FROM nfts) as nfts_count,
        (SELECT COUNT(*) FROM ownership) as ownership_count,
        (SELECT COUNT(*) FROM edition_pricing) as pricing_count,
        (SELECT COUNT(*) FROM ownership_history) as history_count,
        (SELECT COUNT(*) FROM nft_core_metadata_v2) as old_metadata_count,
        (SELECT COUNT(*) FROM wallet_holdings) as old_holdings_count
    `);

        const stats = verification.rows[0];
        console.log('Migration Statistics:');
        console.log('─'.repeat(50));
        console.log(`New Tables:`);
        console.log(`  nfts:              ${stats.nfts_count.toLocaleString()} rows`);
        console.log(`  ownership:         ${stats.ownership_count.toLocaleString()} rows`);
        console.log(`  edition_pricing:   ${stats.pricing_count.toLocaleString()} rows`);
        console.log(`  ownership_history: ${stats.history_count.toLocaleString()} rows`);
        console.log('');
        console.log(`Old Tables (still active):`);
        console.log(`  nft_core_metadata_v2: ${stats.old_metadata_count.toLocaleString()} rows`);
        console.log(`  wallet_holdings:      ${stats.old_holdings_count.toLocaleString()} rows`);
        console.log('─'.repeat(50));

        // Sample query test
        console.log('\nStep 4: Testing sample queries...\n');

        const sampleWallet = '0x93914b2bfb28d59d'; // Kaladin49

        const testQuery = await pgQuery(`
      SELECT 
        n.nft_id,
        n.player_name,
        n.team_name,
        n.tier,
        n.serial_number,
        n.set_name,
        o.is_locked,
        o.first_acquired_at,
        ep.low_ask,
        ep.average_sale_price
      FROM ownership o
      INNER JOIN nfts n ON o.nft_id = n.nft_id
      LEFT JOIN edition_pricing ep ON n.edition_id = ep.edition_id
      WHERE o.wallet_address = $1
      ORDER BY o.first_acquired_at DESC
      LIMIT 5
    `, [sampleWallet]);

        console.log(`Most recent 5 NFTs for ${sampleWallet}:`);
        testQuery.rows.forEach((row, i) => {
            console.log(`  ${i + 1}. ${row.player_name || 'Unknown'} #${row.serial_number}`);
            console.log(`     ${row.set_name} (${row.tier})`);
            console.log(`     Acquired: ${new Date(row.first_acquired_at).toLocaleDateString()}`);
            console.log(`     Floor: ${row.low_ask ? '$' + row.low_ask : 'N/A'}`);
        });

        console.log('\n' + '='.repeat(50));
        console.log('✅ Phase 1 Complete!');
        console.log('='.repeat(50));
        console.log('\nNext Steps:');
        console.log('1. ✅ New schema created and populated');
        console.log('2. ⏳ Update sync scripts to write to new tables (Phase 2)');
        console.log('3. ⏳ Update application queries to read from new tables (Phase 3)');
        console.log('4. ⏳ Deprecate old tables after validation (Phase 4)');
        console.log('\nOld tables are still active - no disruption to production!');

    } catch (error) {
        console.error('\n❌ Migration failed:', error.message);
        console.error(error);
        process.exit(1);
    }

    process.exit(0);
}

runMigration();
