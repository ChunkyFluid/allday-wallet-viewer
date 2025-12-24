import { pgQuery } from './db.js';
import fs from 'fs';

const DRY_RUN = process.env.DRY_RUN !== 'false';
const ADDRESS = '0xcfd9bad75352b43b';

async function cleanupJunglerules() {
    console.log(`\n=== Junglerules Surgical Cleanup ===`);
    console.log(`Mode: ${DRY_RUN ? 'DRY RUN' : 'LIVE EXECUTION'}`);
    console.log(`Address: ${ADDRESS}\n`);

    // Load audit results
    const audit = JSON.parse(fs.readFileSync('junglerules_audit_results.json', 'utf-8'));

    const unlockedGhosts = audit.unlocked.ghosts;
    const lockedGhosts = audit.locked.ghosts;

    console.log(`Found ${unlockedGhosts.length} unlocked ghosts`);
    console.log(`Found ${lockedGhosts.length} locked ghosts`);
    console.log(`\n⚠️  WARNING: Blockchain query returned 0 locked moments.`);
    console.log(`This means ALL ${lockedGhosts.length} locked moments appear to be ghosts.`);
    console.log(`\nFor safety, this script will ONLY remove unlocked ghosts.`);
    console.log(`Locked ghosts require manual verification.\n`);

    // Phase 1: Remove unlocked ghosts only
    if (unlockedGhosts.length > 0) {
        console.log(`\n--- Phase 1: Unlocked Ghosts ---`);
        console.log(`Will remove ${unlockedGhosts.length} unlocked ghost moments:`);
        console.log(`Sample IDs: ${unlockedGhosts.slice(0, 5).join(', ')}...`);

        if (!DRY_RUN) {
            console.log(`\nDeleting from wallet_holdings...`);
            const whResult = await pgQuery(
                `DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
                [ADDRESS, unlockedGhosts]
            );
            console.log(`  Deleted ${whResult.rowCount} rows from wallet_holdings`);

            console.log(`Deleting from holdings...`);
            const hResult = await pgQuery(
                `DELETE FROM holdings WHERE wallet_address = $1 AND nft_id = ANY($2::text[])`,
                [ADDRESS, unlockedGhosts]
            );
            console.log(`  Deleted ${hResult.rowCount} rows from holdings`);
        } else {
            console.log(`\n[DRY RUN] Would delete ${unlockedGhosts.length} unlocked ghosts.`);
        }
    }

    // Phase 2: Report on locked ghosts (but don't delete yet)
    console.log(`\n--- Phase 2: Locked Ghosts (REPORT ONLY) ---`);
    console.log(`Found ${lockedGhosts.length} locked ghosts that need manual verification.`);
    console.log(`Blockchain returned 0 locked moments, which suggests:`);
    console.log(`  1. Junglerules unlocked all their moments, OR`);
    console.log(`  2. The blockchain query needs to be verified`);
    console.log(`\nRecommendation: Manually check NFL All Day site for Junglerules' locked count.`);

    // Verify new counts
    if (!DRY_RUN) {
        console.log(`\n--- Verification ---`);
        const newCounts = await pgQuery(`
            SELECT 
                COUNT(*) FILTER (WHERE is_locked = false) as unlocked,
                COUNT(*) FILTER (WHERE is_locked = true) as locked,
                COUNT(*) as total
            FROM wallet_holdings 
            WHERE wallet_address = $1
        `, [ADDRESS]);
        console.log(`New counts for Junglerules:`);
        console.log(`  Unlocked: ${newCounts.rows[0].unlocked}`);
        console.log(`  Locked: ${newCounts.rows[0].locked}`);
        console.log(`  Total: ${newCounts.rows[0].total}`);
    }

    console.log(`\n=== Cleanup Complete ===\n`);
    process.exit(0);
}

cleanupJunglerules().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
