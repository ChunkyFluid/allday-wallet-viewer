// Fetch metadata for unknown NFTs from the blockchain
// Used when we encounter NFTs that aren't in our nft_core_metadata table
// This happens when new editions are minted after our Snowflake sync

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import * as flowService from "../services/flow-blockchain.js";

dotenv.config();

/**
 * Find NFT IDs in wallet_holdings that don't have metadata
 */
async function findUnknownNFTs(limit = 100) {
  const result = await pgQuery(`
    SELECT DISTINCT h.nft_id, h.wallet_address
    FROM wallet_holdings h
    LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
    WHERE m.nft_id IS NULL
    LIMIT $1
  `, [limit]);

  return result.rows;
}

/**
 * Fetch metadata for a single NFT from the blockchain and insert into database
 */
async function fetchAndStoreNFTMetadata(nftId, walletAddress) {
  try {
    // Get metadata from blockchain
    const metadata = await flowService.getNFTFullDetails(walletAddress, parseInt(nftId));

    if (!metadata) {
      console.log(`[Metadata] No metadata found for NFT ${nftId}`);
      return false;
    }

    // Map blockchain metadata to our schema
    // Note: On-chain metadata may not have all fields (like first_name, last_name separately)
    const nftData = {
      nft_id: nftId,
      edition_id: metadata.editionId || null,
      play_id: metadata.playId?.toString() || null,
      series_id: metadata.seriesId?.toString() || null,
      set_id: metadata.setId?.toString() || null,
      tier: metadata.tier || null,
      serial_number: metadata.serialNumber || null,
      max_mint_size: metadata.maxMintSize || null,
      // On-chain, we often get combined player name
      first_name: null,
      last_name: metadata.playerName || null,
      team_name: metadata.teamName || null,
      position: metadata.position || null,
      jersey_number: null,
      series_name: null,
      set_name: null
    };

    // Insert into database
    await pgQuery(`
      INSERT INTO nft_core_metadata_v2 (
        nft_id, edition_id, play_id, series_id, set_id, tier,
        serial_number, max_mint_size, first_name, last_name,
        team_name, position, jersey_number, series_name, set_name
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
      ON CONFLICT (nft_id) DO UPDATE SET
        edition_id = EXCLUDED.edition_id,
        play_id = EXCLUDED.play_id,
        series_id = EXCLUDED.series_id,
        set_id = EXCLUDED.set_id,
        tier = EXCLUDED.tier,
        serial_number = EXCLUDED.serial_number,
        max_mint_size = EXCLUDED.max_mint_size,
        first_name = COALESCE(EXCLUDED.first_name, nft_core_metadata_v2.first_name),
        last_name = COALESCE(EXCLUDED.last_name, nft_core_metadata_v2.last_name),
        team_name = COALESCE(EXCLUDED.team_name, nft_core_metadata_v2.team_name),
        position = COALESCE(EXCLUDED.position, nft_core_metadata_v2.position),
        jersey_number = COALESCE(EXCLUDED.jersey_number, nft_core_metadata_v2.jersey_number),
        series_name = COALESCE(EXCLUDED.series_name, nft_core_metadata_v2.series_name),
        set_name = COALESCE(EXCLUDED.set_name, nft_core_metadata_v2.set_name)
    `, [
      nftData.nft_id,
      nftData.edition_id,
      nftData.play_id,
      nftData.series_id,
      nftData.set_id,
      nftData.tier,
      nftData.serial_number,
      nftData.max_mint_size,
      nftData.first_name,
      nftData.last_name,
      nftData.team_name,
      nftData.position,
      nftData.jersey_number,
      nftData.series_name,
      nftData.set_name
    ]);

    console.log(`[Metadata] ✅ Stored metadata for NFT ${nftId}: ${nftData.tier || 'unknown tier'} - ${nftData.last_name || 'unknown'}`);
    return true;
  } catch (err) {
    console.error(`[Metadata] ❌ Error fetching metadata for NFT ${nftId}:`, err.message);
    return false;
  }
}

/**
 * Process all unknown NFTs
 */
async function processUnknownNFTs() {
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  FETCH UNKNOWN NFT METADATA (from blockchain)");
  console.log("═══════════════════════════════════════════════════════════\n");

  const unknownNFTs = await findUnknownNFTs(500);

  if (unknownNFTs.length === 0) {
    console.log("[Metadata] ✅ All NFTs have metadata!");
    return { processed: 0, success: 0, failed: 0 };
  }

  console.log(`[Metadata] Found ${unknownNFTs.length} NFTs without metadata\n`);

  let success = 0;
  let failed = 0;

  for (const { nft_id, wallet_address } of unknownNFTs) {
    const result = await fetchAndStoreNFTMetadata(nft_id, wallet_address);
    if (result) {
      success++;
    } else {
      failed++;
    }

    // Small delay to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 200));
  }

  console.log(`\n[Metadata] ✅ Complete: ${success} success, ${failed} failed`);
  return { processed: unknownNFTs.length, success, failed };
}

// Export for use as a module
export { findUnknownNFTs, fetchAndStoreNFTMetadata, processUnknownNFTs };

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  processUnknownNFTs()
    .then(() => process.exit(0))
    .catch(err => {
      console.error("Fatal error:", err);
      process.exit(1);
    });
}
