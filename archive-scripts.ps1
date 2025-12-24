# Archive Diagnostic Scripts
# Moves one-off investigation scripts to /archive directory

Write-Host "=== Archiving Diagnostic Scripts ===" -ForegroundColor Cyan
Write-Host ""

$archiveDir = ".\archive"
$archived = 0

# Bo Nix investigation
$boNixFiles = @(
    "check-bo-nix.js",
    "check-bo-nix-final.js",
    "find-kaladin49-wallets.js",
    "sync-kaladin49.js"
)

# Junglerules
$jungleruleFiles = @(
    "audit-junglerules-ghosts.js",
    "cleanup-junglerules.js",
    "collect-junglerules-data.js",
    "investigate-junglerules.js",
    "prep-junglerules-fix.js",
    "verify-junglerules-locked.js",
    "check-local-junglerules.js",
    "junglerules_audit_results.json",
    "junglerules_counts.txt"
)

# Ghost cleanup
$ghostFiles = @(
    "global-locked-cleanup.js",
    "identify-locked-ghosts-snowflake.js",
    "fix-locked-ghosts.js",
    "clean-specific-ghosts.js",
    "check-locked-ghosts.js",
    "find-ghost-sample.js",
    "check-ghost-events.js",
    "check-ghost-events-smart.js",
    "check-ghost-owner-snowflake.js",
    "ghosts.json",
    "locked_ghosts.json",
    "locked_ghosts_v2.json",
    "locked_ghosts_v3.json",
    "unlocked_ghosts.json",
    "unlocked_ghosts_v2.json"
)

# Schema investigations
$schemaFiles = @(
    "analyze-schema.js",
    "check-schema.js",
    "debug-db.js",
    "check-db-state.js",
    "count-missing-metadata.js"
)

# Snowflake checks
$snowflakeFiles = @(
    "batch-check-snowflake.cjs",
    "check-snowflake-nft.cjs",
    "check-snowflake-unknowns.cjs",
    "quantify-snowflake-unknowns.cjs",
    "snowflake-schema-check.cjs",
    "snowflake-schema-list.cjs",
    "snowflake.log",
    "snowflake_batch_check.json"
)

# Contract inspections
$contractFiles = @(
    "fetch-allday-contract.js",
    "fetch-locker-contract.js",
    "fetch-market-contract.js",
    "inspect-capabilities.js",
    "inspect-marketplace.js",
    "inspect-nfts.js",
    "verify-locker-interface.js",
    "allday_contract.cdc",
    "contract_code.txt",
    "contract_utf8.cdc",
    "full_contract.cadence",
    "market_contract.cdc"
)

# Current session diagnostics
$currentSessionFiles = @(
    "check-shedeur.js",
    "diagnose-shedeur.js",
    "emergency-restore.js",
    "find-locked.js",
    "add-shedeur-31.js",
    "add-nft-8511045.js",
    "restore-dates-properly.js",
    "cleanup-schema.js"
)

# Metadata investigations
$metadataFiles = @(
    "fetch-metadata-from-flow.js",
    "fetch-missing-metadata.js",
    "investigate-base-missing.js",
    "investigate-unknowns.js",
    "fix_unknowns.csv"
)

# Misc
$miscFiles = @(
    "find-discrepancy.js",
    "fix-nft.js",
    "verify-draft.js",
    "verify-global-lock.js",
    "investigate-locked-real-status.js",
    "test-blockchain-fetch.js",
    "test-connections.js",
    "test-simple-fetch.js",
    "test-wallet-ids.js",
    "debug-npm.js",
    "check-empty.js",
    "check-market-listings.js",
    "check-missing-ids.js",
    "check-missing-prices.js",
    "check-play-overlap.js",
    "check-price.js",
    "check-runtime-type.js",
    "check-sets-missing.js",
    "check-specific-nft.js",
    "find-sample.js",
    "get-null-sample.js",
    "null_names_sample.json",
    "audit_details.txt",
    "local_results.txt",
    "check-child-accounts.js",
    "NFLAllDayWalletGrab.sql",
    "nflad-cookies.json"
)

# Migration files (failed)
$migrationFiles = @(
    "run-migration.js",
    "fix-and-migrate.js"
)

# Old documentation
$oldDocs = @(
    "DATABASE_SAFETY_FIXES_NEEDED.md",
    "SNIPER_FIX_SUMMARY.md",
    "SNIPER_SALES_ISSUES.md"
)

# Combine all
$allFiles = $boNixFiles + $jungleruleFiles + $ghostFiles + $schemaFiles + $snowflakeFiles + $contractFiles + $currentSessionFiles + $metadataFiles + $miscFiles + $migrationFiles + $oldDocs

Write-Host "Found $($allFiles.Count) files to archive" -ForegroundColor Yellow
Write-Host ""

foreach ($file in $allFiles) {
    if (Test-Path $file) {
        Move-Item -Path $file -Destination $archiveDir -Force
        $archived++
        Write-Host "  ✅ $file" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "=== Archive Complete ===" -ForegroundColor Cyan
Write-Host "Archived: $archived files" -ForegroundColor Green
Write-Host "Location: $archiveDir" -ForegroundColor Gray
Write-Host ""
Write-Host "Root directory is now clean!" -ForegroundColor Green
