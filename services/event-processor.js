// services/event-processor.js
// Blockchain event processor for the new normalized schema
// Handles all NFL All Day events and updates the database in real-time

import { pgQuery } from "../db.js";

const ALLDAY_CONTRACT = "A.e4cf4bdc1751c65d.AllDay";
const NFTLOCKER_CONTRACT = "0xb6f2481eba4df97b"; // NFTLocker holds locked NFTs - don't track as wallet owner

// All event types we want to process
export const ALLDAY_EVENT_TYPES = [
    // Transfer events (existing)
    `${ALLDAY_CONTRACT}.Deposit`,
    `${ALLDAY_CONTRACT}.Withdraw`,
    // Metadata events (new)
    `${ALLDAY_CONTRACT}.MomentNFTMinted`,
    `${ALLDAY_CONTRACT}.MomentNFTBurned`,
    `${ALLDAY_CONTRACT}.EditionCreated`,
    `${ALLDAY_CONTRACT}.PlayCreated`,
    `${ALLDAY_CONTRACT}.SeriesCreated`,
    `${ALLDAY_CONTRACT}.SetCreated`,
    // Lock/Unlock events
    "A.b6f2481eba4df97b.NFTLocker.NFTLocked",
    "A.b6f2481eba4df97b.NFTLocker.NFTUnlocked"
];

// Extract value from Cadence JSON format
function extractCadenceValue(field) {
    if (!field) return null;
    if (field.value !== undefined) {
        if (typeof field.value === 'object' && field.value.value !== undefined) {
            return field.value.value;
        }
        return field.value;
    }
    return field;
}

// Parse event payload from various formats
function parseEventPayload(event) {
    let payload = {};

    if (event.payload) {
        if (typeof event.payload === 'string') {
            try {
                payload = JSON.parse(Buffer.from(event.payload, 'base64').toString());
            } catch {
                try {
                    payload = JSON.parse(event.payload);
                } catch {
                    payload = {};
                }
            }
        } else if (typeof event.payload === 'object') {
            payload = event.payload;
        }
    }

    return payload;
}

// Extract fields from Cadence JSON-CDC format
function extractFields(payload) {
    const result = {};

    if (payload.value && payload.value.fields) {
        for (const field of payload.value.fields) {
            result[field.name] = extractCadenceValue(field);
        }
    } else {
        // Direct object format
        Object.assign(result, payload);
    }

    return result;
}

// Process SeriesCreated event
export async function handleSeriesCreated(event, payload) {
    const fields = extractFields(payload);
    const seriesId = fields.id?.toString();
    const seriesName = fields.name?.toString() || '';

    if (!seriesId) {
        console.log('[Event] SeriesCreated: Missing series ID');
        return;
    }

    try {
        await pgQuery(`
      INSERT INTO series (series_id, series_name)
      VALUES ($1, $2)
      ON CONFLICT (series_id) DO UPDATE SET series_name = EXCLUDED.series_name
    `, [seriesId, seriesName]);

        console.log(`[Event] ✅ SeriesCreated: ${seriesName} (ID: ${seriesId})`);
    } catch (err) {
        console.error(`[Event] Error handling SeriesCreated:`, err.message);
    }
}

// Process SetCreated event
export async function handleSetCreated(event, payload) {
    const fields = extractFields(payload);
    const setId = fields.id?.toString();
    const setName = fields.name?.toString() || '';

    if (!setId) {
        console.log('[Event] SetCreated: Missing set ID');
        return;
    }

    try {
        await pgQuery(`
      INSERT INTO sets (set_id, set_name)
      VALUES ($1, $2)
      ON CONFLICT (set_id) DO UPDATE SET set_name = EXCLUDED.set_name
    `, [setId, setName]);

        console.log(`[Event] ✅ SetCreated: ${setName} (ID: ${setId})`);
    } catch (err) {
        console.error(`[Event] Error handling SetCreated:`, err.message);
    }
}

// Process PlayCreated event
export async function handlePlayCreated(event, payload) {
    const fields = extractFields(payload);
    const playId = fields.id?.toString();

    if (!playId) {
        console.log('[Event] PlayCreated: Missing play ID');
        return;
    }

    // Extract player metadata from the metadata array
    let firstName = null, lastName = null, teamName = null, position = null, jerseyNumber = null;

    if (fields.metadata && Array.isArray(fields.metadata)) {
        for (const meta of fields.metadata) {
            const key = meta.key || meta.name;
            const value = meta.value;

            if (key === 'playerFirstName') firstName = value;
            else if (key === 'playerLastName') lastName = value;
            else if (key === 'teamName') teamName = value;
            else if (key === 'playerPosition') position = value;
            else if (key === 'playerNumber') jerseyNumber = parseInt(value) || null;
        }
    }

    try {
        await pgQuery(`
      INSERT INTO plays (play_id, first_name, last_name, team_name, position, jersey_number)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (play_id) DO UPDATE SET
        first_name = COALESCE(EXCLUDED.first_name, plays.first_name),
        last_name = COALESCE(EXCLUDED.last_name, plays.last_name),
        team_name = COALESCE(EXCLUDED.team_name, plays.team_name),
        position = COALESCE(EXCLUDED.position, plays.position),
        jersey_number = COALESCE(EXCLUDED.jersey_number, plays.jersey_number)
    `, [playId, firstName, lastName, teamName, position, jerseyNumber]);

        console.log(`[Event] ✅ PlayCreated: ${firstName || 'Unknown'} ${lastName || ''} (ID: ${playId})`);
    } catch (err) {
        console.error(`[Event] Error handling PlayCreated:`, err.message);
    }
}

// Process EditionCreated event
export async function handleEditionCreated(event, payload) {
    const fields = extractFields(payload);
    const editionId = fields.id?.toString();
    const playId = fields.playID?.toString();
    const seriesId = fields.seriesID?.toString();
    const setId = fields.setID?.toString();
    const tier = fields.tier?.toString();
    const maxMintSize = parseInt(fields.maxMintSize) || null;

    if (!editionId) {
        console.log('[Event] EditionCreated: Missing edition ID');
        return;
    }

    try {
        await pgQuery(`
      INSERT INTO editions (edition_id, play_id, series_id, set_id, tier, max_mint_size)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (edition_id) DO UPDATE SET
        play_id = EXCLUDED.play_id,
        series_id = EXCLUDED.series_id,
        set_id = EXCLUDED.set_id,
        tier = EXCLUDED.tier,
        max_mint_size = EXCLUDED.max_mint_size
    `, [editionId, playId, seriesId, setId, tier, maxMintSize]);

        console.log(`[Event] ✅ EditionCreated: ${tier || 'Unknown'} (ID: ${editionId}, Play: ${playId})`);
    } catch (err) {
        console.error(`[Event] Error handling EditionCreated:`, err.message);
    }
}

// Process MomentNFTMinted event
export async function handleMomentNFTMinted(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();
    const editionId = fields.editionID?.toString();
    const serialNumber = parseInt(fields.serialNumber) || null;
    const mintedAt = event.block_timestamp ? new Date(event.block_timestamp) : new Date();

    if (!nftId) {
        console.log('[Event] MomentNFTMinted: Missing NFT ID');
        return;
    }

    try {
        // Safe check for nfts table
        await pgQuery(`
      INSERT INTO nfts (nft_id, edition_id, serial_number, minted_at)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (nft_id) DO UPDATE SET
        edition_id = EXCLUDED.edition_id,
        serial_number = EXCLUDED.serial_number,
        minted_at = COALESCE(nfts.minted_at, EXCLUDED.minted_at)
    `, [nftId, editionId, serialNumber, mintedAt]).catch(err => {
            if (!err.message.includes('relation "nfts" does not exist')) {
                throw err;
            }
            // Silently skip if nfts table doesn't exist
        });

        console.log(`[Event] ✅ MomentNFTMinted: NFT ${nftId} (Edition: ${editionId}, Serial: ${serialNumber})`);
    } catch (err) {
        console.error(`[Event] Error handling MomentNFTMinted:`, err.message);
    }
}

// Process MomentNFTBurned event
export async function handleMomentNFTBurned(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();
    const burnedAt = event.block_timestamp ? new Date(event.block_timestamp) : new Date();

    if (!nftId) {
        console.log('[Event] MomentNFTBurned: Missing NFT ID');
        return;
    }

    try {
        // Safe check for nfts table
        await pgQuery(`
      UPDATE nfts SET burned_at = $1 WHERE nft_id = $2
    `, [burnedAt, nftId]).catch(err => {
            if (!err.message.includes('relation "nfts" does not exist')) {
                throw err;
            }
            // Silently skip if nfts table doesn't exist
        });

        // Also remove from holdings
        await pgQuery(`
      DELETE FROM holdings WHERE nft_id = $1
    `, [nftId]);

        console.log(`[Event] ✅ MomentNFTBurned: NFT ${nftId} (Removed from all holdings)`);
    } catch (err) {
        console.error(`[Event] Error handling MomentNFTBurned:`, err.message);
    }
}

// Process Deposit event (NFT transferred TO a wallet)
export async function handleDeposit(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();
    const toAddr = fields.to?.toString()?.toLowerCase();
    const acquiredAt = event.block_timestamp ? new Date(event.block_timestamp) : new Date();

    if (!nftId || !toAddr) {
        console.log('[Event] Deposit: Missing NFT ID or to address');
        return;
    }

    // Ignore deposits TO the NFTLocker contract (these are locked NFTs)
    // The original owner should remain the owner, just with is_locked = true
    if (toAddr === NFTLOCKER_CONTRACT.toLowerCase()) {
        console.log(`[Event] Deposit: Ignoring deposit to NFTLocker contract for NFT ${nftId}`);
        return;
    }

    try {
        // 1. Remove from ANY other wallet (Transfer logic)
        // This is critical for "instant" removal when an NFT moves to a contract (like GiftPack) or another user.
        const deleteResult = await pgQuery(
            `DELETE FROM holdings WHERE nft_id = $1 AND wallet_address != $2`,
            [nftId, toAddr]
        );

        if (deleteResult.rowCount > 0) {
            console.log(`[Event] 🗑️  Removed NFT ${nftId} from previous owner(s)`);
        }

        // 2. Add to NEW owner
        await pgQuery(`
      INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
      VALUES ($1, $2, FALSE, $3, NOW())
      ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
        is_locked = FALSE,
        acquired_at = COALESCE(holdings.acquired_at, EXCLUDED.acquired_at),
        last_synced_at = NOW()
    `, [toAddr, nftId, acquiredAt]);

        console.log(`[Event] ✅ Transferred NFT ${nftId} → ${toAddr.substring(0, 10)}...`);
    } catch (err) {
        console.error(`[Event] Error handling Deposit:`, err.message);
    }
}

// Process Withdraw event (NFT transferred FROM a wallet)
export async function handleWithdraw(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();
    const fromAddr = fields.from?.toString()?.toLowerCase();

    if (!nftId || !fromAddr) {
        console.log('[Event] Withdraw: Missing NFT ID or from address');
        return;
    }

    // Ignore withdrawals FROM the NFTLocker contract (these are unlocked NFTs)
    // The owner is still tracked separately
    if (fromAddr === NFTLOCKER_CONTRACT.toLowerCase()) {
        console.log(`[Event] Withdraw: Ignoring withdraw from NFTLocker contract for NFT ${nftId}`);
        return;
    }

    try {
        // MODIFIED: We no longer delete on Withdraw to prevent losing locked NFTs.
        // Ownership is now updated in handleDeposit (Transfer on Deposit).
        console.log(`[Event] Withdrawal detected for NFT ${nftId} ← ${fromAddr.substring(0, 10)}... (waiting for Deposit to change ownership)`);
    } catch (err) {
        console.error(`[Event] Error handling Withdraw:`, err.message);
    }
}

// Process NFTLocked event
export async function handleNFTLocked(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();

    if (!nftId) {
        console.log('[Event] NFTLocked: Missing NFT ID');
        return;
    }

    try {
        // Update is_locked in holdings table
        await pgQuery(`UPDATE holdings SET is_locked = TRUE WHERE nft_id = $1`, [nftId]);

        console.log(`[Event] ✅ NFTLocked: NFT ${nftId}`);
    } catch (err) {
        console.error(`[Event] Error handling NFTLocked:`, err.message);
    }
}

// Process NFTUnlocked event
export async function handleNFTUnlocked(event, payload) {
    const fields = extractFields(payload);
    const nftId = fields.id?.toString();

    if (!nftId) {
        console.log('[Event] NFTUnlocked: Missing NFT ID');
        return;
    }

    try {
        // Update is_locked in holdings table
        await pgQuery(`UPDATE holdings SET is_locked = FALSE WHERE nft_id = $1`, [nftId]);

        console.log(`[Event] ✅ NFTUnlocked: NFT ${nftId}`);
    } catch (err) {
        console.error(`[Event] Error handling NFTUnlocked:`, err.message);
    }
}

// Main event processor - routes events to appropriate handlers
export async function processBlockchainEvent(event) {
    try {
        const eventType = event.type || '';
        const payload = parseEventPayload(event);

        // Route to appropriate handler based on event type
        if (eventType.endsWith('.SeriesCreated')) {
            await handleSeriesCreated(event, payload);
        } else if (eventType.endsWith('.SetCreated')) {
            await handleSetCreated(event, payload);
        } else if (eventType.endsWith('.PlayCreated')) {
            await handlePlayCreated(event, payload);
        } else if (eventType.endsWith('.EditionCreated')) {
            await handleEditionCreated(event, payload);
        } else if (eventType.endsWith('.MomentNFTMinted')) {
            await handleMomentNFTMinted(event, payload);
        } else if (eventType.endsWith('.MomentNFTBurned')) {
            await handleMomentNFTBurned(event, payload);
        } else if (eventType.endsWith('.Deposit')) {
            await handleDeposit(event, payload);
        } else if (eventType.endsWith('.Withdraw')) {
            await handleWithdraw(event, payload);
        } else if (eventType.endsWith('.NFTLocked')) {
            await handleNFTLocked(event, payload);
        } else if (eventType.endsWith('.NFTUnlocked')) {
            await handleNFTUnlocked(event, payload);
        } else {
            console.log(`[Event] Unknown event type: ${eventType}`);
        }
    } catch (err) {
        console.error(`[Event] Error processing blockchain event:`, err.message);
    }
}

export default {
    ALLDAY_EVENT_TYPES,
    processBlockchainEvent,
    handleSeriesCreated,
    handleSetCreated,
    handlePlayCreated,
    handleEditionCreated,
    handleMomentNFTMinted,
    handleMomentNFTBurned,
    handleDeposit,
    handleWithdraw,
    handleNFTLocked,
    handleNFTUnlocked
};
