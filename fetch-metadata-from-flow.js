import { pgQuery } from './db.js';
import { getNFTFullDetails } from './services/flow-blockchain.js';

async function fetchMetadataFromFlow(walletAddr, nftIds) {
    try {
        for (const nftId of nftIds) {
            console.log(`Fetching NFT ${nftId} from Flow...`);
            const details = await getNFTFullDetails(walletAddr, nftId);

            if (!details) {
                console.warn(`Could not find details for ${nftId} on Flow.`);
                continue;
            }

            console.log(`Found details for ${nftId}:`, JSON.stringify(details).slice(0, 100) + '...');

            // Map Flow details to DB columns
            // Note: Full details from Cadence might have different field names
            const metadata = details.metadata || {};
            const editionData = details.editionData || {};

            await pgQuery(`
          INSERT INTO nft_core_metadata_v2 (
              nft_id, edition_id, first_name, last_name, team_name,
              set_name, series_name, tier, serial_number, max_mint_size,
              jersey_number, play_type, series_number, set_number,
              player_id, team_id, season
          ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
              $11, $12, $13, $14, $15, $16, $17
          )
          ON CONFLICT (nft_id) DO UPDATE SET
              edition_id = EXCLUDED.edition_id,
              first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
              team_name = EXCLUDED.team_name,
              set_name = EXCLUDED.set_name,
              series_name = EXCLUDED.series_name,
              tier = EXCLUDED.tier,
              serial_number = EXCLUDED.serial_number,
              max_mint_size = EXCLUDED.max_mint_size,
              jersey_number = EXCLUDED.jersey_number,
              play_type = EXCLUDED.play_type,
              series_number = EXCLUDED.series_number,
              set_number = EXCLUDED.set_number,
              player_id = EXCLUDED.player_id,
              team_id = EXCLUDED.team_id,
              season = EXCLUDED.season
      `, [
                nftId.toString(),
                details.editionID || details.editionId,
                metadata.playerFirstName || '',
                metadata.playerLastName || '',
                metadata.teamName || '',
                metadata.setName || '',
                metadata.seriesName || '',
                details.tier || metadata.tier || '',
                details.serialNumber || '',
                details.maxMintSize || '',
                metadata.jerseyNumber || null,
                metadata.playType || '',
                metadata.seriesNumber || null,
                metadata.setNumber || null,
                metadata.playerID || metadata.playerId || '',
                metadata.teamID || metadata.teamId || '',
                metadata.season || ''
            ]);

            console.log(`Successfully backfilled NFT ${nftId} from Flow.`);
        }

    } catch (err) {
        console.error('Error:', err);
    } finally {
        process.exit(0);
    }
}

const CHUNKY_ADDR = '0x7541bafd155b683e';
const MISSING_IDS = ['6063904', '6049871'];
fetchMetadataFromFlow(CHUNKY_ADDR, MISSING_IDS);
