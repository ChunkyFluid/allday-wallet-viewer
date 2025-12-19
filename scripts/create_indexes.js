import { pgQuery } from '../db.js';

/**
 * Create indexes to speed up set-completion queries
 */
async function createIndexes() {
    console.log('Creating indexes for set-completion performance...\n');

    const indexes = [
        // wallet_holdings indexes
        {
            name: 'idx_wallet_holdings_wallet_address',
            sql: 'CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_address ON wallet_holdings(wallet_address)'
        },
        {
            name: 'idx_wallet_holdings_nft_id',
            sql: 'CREATE INDEX IF NOT EXISTS idx_wallet_holdings_nft_id ON wallet_holdings(nft_id)'
        },
        {
            name: 'idx_wallet_holdings_wallet_nft',
            sql: 'CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_nft ON wallet_holdings(wallet_address, nft_id)'
        },
        {
            name: 'idx_wallet_holdings_wallet_locked',
            sql: 'CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_locked ON wallet_holdings(wallet_address, is_locked)'
        },

        // nft_core_metadata_v2 indexes
        {
            name: 'idx_nft_metadata_v2_nft_id',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_nft_id ON nft_core_metadata_v2(nft_id)'
        },
        {
            name: 'idx_nft_metadata_v2_edition_id',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_edition_id ON nft_core_metadata_v2(edition_id)'
        },
        {
            name: 'idx_nft_metadata_v2_set_name',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_set_name ON nft_core_metadata_v2(set_name)'
        },
        {
            name: 'idx_nft_metadata_v2_team_name',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_team_name ON nft_core_metadata_v2(team_name)'
        },
        {
            name: 'idx_nft_metadata_v2_tier',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_tier ON nft_core_metadata_v2(tier)'
        },
        // Composite index for the set-completion join
        {
            name: 'idx_nft_metadata_v2_nft_set_tier',
            sql: 'CREATE INDEX IF NOT EXISTS idx_nft_metadata_v2_nft_set_tier ON nft_core_metadata_v2(nft_id, set_name, tier, edition_id)'
        },

        // set_editions_snapshot indexes
        {
            name: 'idx_set_editions_snapshot_edition_id',
            sql: 'CREATE INDEX IF NOT EXISTS idx_set_editions_snapshot_edition_id ON set_editions_snapshot(edition_id)'
        },
        {
            name: 'idx_set_editions_snapshot_set_name',
            sql: 'CREATE INDEX IF NOT EXISTS idx_set_editions_snapshot_set_name ON set_editions_snapshot(set_name)'
        },

        // edition_price_scrape indexes
        {
            name: 'idx_edition_price_scrape_edition_id',
            sql: 'CREATE INDEX IF NOT EXISTS idx_edition_price_scrape_edition_id ON edition_price_scrape(edition_id)'
        }
    ];

    for (const idx of indexes) {
        try {
            console.log(`Creating ${idx.name}...`);
            const start = Date.now();
            await pgQuery(idx.sql);
            console.log(`  ✅ Done in ${Date.now() - start}ms`);
        } catch (err) {
            console.log(`  ⚠️ ${err.message}`);
        }
    }

    console.log('\n✅ All indexes created!');
    process.exit(0);
}

createIndexes().catch(e => {
    console.error('Error creating indexes:', e);
    process.exit(1);
});
