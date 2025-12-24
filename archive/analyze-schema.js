import { pgQuery } from './db.js';

async function analyzeSchema() {
    try {
        console.log('=== DATABASE SCHEMA ANALYSIS ===\n');

        // Get all tables and their row counts
        const tablesQuery = `
      SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
      FROM pg_tables
      WHERE schemaname = 'public'
      ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
    `;

        const tables = await pgQuery(tablesQuery);
        console.log('Tables by size:');
        tables.rows.forEach(row => {
            console.log(`  ${row.tablename.padEnd(40)} ${row.size}`);
        });

        console.log('\n=== KEY TABLES SCHEMA ===\n');

        // Check wallet_holdings structure
        const whCols = await pgQuery(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'wallet_holdings'
      ORDER BY ordinal_position
    `);
        console.log('wallet_holdings:');
        whCols.rows.forEach(r => console.log(`  ${r.column_name.padEnd(25)} ${r.data_type.padEnd(30)} ${r.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`));

        // Check holdings structure
        const hCols = await pgQuery(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'holdings'
      ORDER BY ordinal_position
    `);
        console.log('\nholdings:');
        hCols.rows.forEach(r => console.log(`  ${r.column_name.padEnd(25)} ${r.data_type.padEnd(30)} ${r.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`));

        // Check nft_core_metadata_v2 structure
        const metaCols = await pgQuery(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'nft_core_metadata_v2'
      ORDER BY ordinal_position
    `);
        console.log('\nnft_core_metadata_v2:');
        metaCols.rows.forEach(r => console.log(`  ${r.column_name.padEnd(25)} ${r.data_type.padEnd(30)} ${r.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`));

        // Check editions structure
        const edCols = await pgQuery(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'editions'
      ORDER BY ordinal_position
    `);
        console.log('\neditions:');
        edCols.rows.forEach(r => console.log(`  ${r.column_name.padEnd(25)} ${r.data_type.padEnd(30)} ${r.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`));

        console.log('\n=== SAMPLE DATA OVERLAP ===\n');

        // Check what data exists in each table
        const sampleNft = await pgQuery(`
      SELECT nft_id FROM wallet_holdings LIMIT 1
    `);

        if (sampleNft.rows.length > 0) {
            const nftId = sampleNft.rows[0].nft_id;
            console.log(`Sample NFT ID: ${nftId}\n`);

            // Check wallet_holdings
            const wh = await pgQuery('SELECT * FROM wallet_holdings WHERE nft_id = $1', [nftId]);
            console.log('In wallet_holdings:');
            console.log(JSON.stringify(wh.rows[0], null, 2));

            // Check holdings
            const h = await pgQuery('SELECT * FROM holdings WHERE nft_id = $1', [nftId]);
            console.log('\nIn holdings:');
            console.log(JSON.stringify(h.rows[0], null, 2));

            // Check metadata
            const meta = await pgQuery('SELECT * FROM nft_core_metadata_v2 WHERE nft_id = $1', [nftId]);
            console.log('\nIn nft_core_metadata_v2:');
            if (meta.rows.length > 0) {
                console.log(JSON.stringify(meta.rows[0], null, 2));
            } else {
                console.log('  NOT FOUND');
            }
        }

        console.log('\n=== PROBLEM: ACQUIRED_AT RESET ===\n');

        // Find locked NFTs that had acquired_at changed recently
        const recentChanges = await pgQuery(`
      SELECT 
        wh.nft_id,
        wh.wallet_address,
        wh.is_locked,
        wh.last_event_ts,
        h.acquired_at,
        h.created_at
      FROM wallet_holdings wh
      LEFT JOIN holdings h ON wh.wallet_address = h.wallet_address AND wh.nft_id = h.nft_id
      WHERE wh.wallet_address = '0x93914b2bfb28d59d'
        AND wh.is_locked = true
      ORDER BY h.acquired_at DESC NULLS LAST
      LIMIT 5
    `);

        console.log('Recent locked NFTs for Kaladin49:');
        recentChanges.rows.forEach(row => {
            console.log(`  NFT ${row.nft_id}:`);
            console.log(`    Locked: ${row.is_locked}`);
            console.log(`    Last Event: ${row.last_event_ts}`);
            console.log(`    Acquired: ${row.acquired_at}`);
            console.log(`    Created: ${row.created_at}`);
            console.log('');
        });

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        process.exit();
    }
}

analyzeSchema();
