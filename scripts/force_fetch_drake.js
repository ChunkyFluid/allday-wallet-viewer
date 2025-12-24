import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import * as flowService from "../services/flow-blockchain.js";

dotenv.config();

async function fetchAndStoreNFTMetadata(nftId, walletAddress) {
    try {
        // Get metadata from blockchain
        const metadata = await flowService.getNFTFullDetails(walletAddress, parseInt(nftId));

        if (!metadata) {
            console.log(`[Metadata] No metadata found for NFT ${nftId}`);
            return false;
        }

        // Map blockchain metadata to our schema
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

        console.log("Mapped Data:", nftData);

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

        console.log(`[Metadata] ✅ Stored metadata for NFT ${nftId}`);
        return true;
    } catch (err) {
        console.error(`[Metadata] ❌ Error fetching metadata for NFT ${nftId}:`, err.message);
        return false;
    }
}

async function forceFetch() {
    const nftId = '10576026';
    const wallet = '0x93914b2bfb28d59d';
    console.log(`Force fetching metadata for Drake Maye (${nftId})...`);
    await fetchAndStoreNFTMetadata(nftId, wallet);
    process.exit();
}

forceFetch();
