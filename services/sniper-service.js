import { pgQuery } from "../db.js";
import pool from "../db/pool.js";
import fetch from "node-fetch";
import path from "path";
import fs from "fs";

// ============================================================
// CONFIG & CONSTANTS
// ============================================================

const SNIPER_LOGGING_ENABLED = true;
const STOREFRONT_CONTRACT = "A.4eb8a10cb9f87357.NFTStorefront";
const FLOW_REST_API = "https://rest-mainnet.onflow.org";
const FLOW_LIGHT_NODE_URL = "https://rest-mainnet.onflow.org";
const FLOOR_CACHE_TTL = 5 * 60 * 1000;
const MAX_FLOOR_CACHE_SIZE = 300;
const MAX_SNIPER_LISTINGS = 1500;
const MAX_SEEN_NFTS = 500;
const MAX_SOLD_NFTS = 500;

const RARITY_LEADERBOARD_SNAPSHOT_FILE = path.join(process.cwd(), "public", "data", "rarity_leaderboard_snapshot.json");

// ============================================================
// STATE (Caches & In-memory Tracking)
// ============================================================

export const sniperListings = [];
export const seenListingNfts = new Map();
export const soldNfts = new Map();
export const unlistedNfts = new Map();
export const floorPriceCache = new Map();

export let isWatchingListings = false;
export let lastCheckedBlock = 0;

// ============================================================
// LOGGING HELPERS
// ============================================================

export function sniperLog(...args) {
    if (SNIPER_LOGGING_ENABLED) console.log(...args);
}
export function sniperWarn(...args) {
    if (SNIPER_LOGGING_ENABLED) console.warn(...args);
}
export function sniperError(...args) {
    if (SNIPER_LOGGING_ENABLED) console.error(...args);
}

// ============================================================
// DISPLAY NAME UTILITY
// ============================================================

export async function getDisplayName(walletAddress) {
    if (!walletAddress) return null;
    const address = walletAddress.toLowerCase();

    try {
        const result = await pgQuery(
            `SELECT display_name, last_checked FROM wallet_profiles WHERE wallet_address = $1 LIMIT 1`,
            [address]
        );

        const now = Date.now();
        const oneDay = 24 * 60 * 60 * 1000;

        if (result.rows.length > 0) {
            const row = result.rows[0];
            if (row.display_name && (now - new Date(row.last_checked).getTime() < oneDay)) {
                return row.display_name;
            }
            const oneHour = 60 * 60 * 1000;
            if (!row.display_name && (now - new Date(row.last_checked).getTime() < oneHour)) {
                return address;
            }
        }

        let displayName = null;
        try {
            const res = await fetch(`https://open.meetdapper.com/profile?address=${address}`, {
                headers: { 'user-agent': 'allday-wallet-viewer/1.0' },
                signal: AbortSignal.timeout(5000)
            });
            if (res.ok) {
                const data = await res.json();
                displayName = data?.displayName || null;
            }
        } catch (err) { }

        await pgQuery(`
      INSERT INTO wallet_profiles (wallet_address, display_name, source, last_checked)
      VALUES ($1, $2, 'dapper', NOW())
      ON CONFLICT (wallet_address) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, wallet_profiles.display_name),
        last_checked = NOW()
    `, [address, displayName]);

        return displayName || address;
    } catch (err) {
        return address;
    }
}

// ============================================================
// DEAL SCORE CALCULATOR
// ============================================================

export function calculateRealDealScore(listing) {
    const { listingPrice, floor, avgSale, serialNumber, jerseyNumber, maxMint } = listing;

    if (!listingPrice || listingPrice <= 0) return 0;

    let efmv = floor || avgSale || 0;

    if (floor && avgSale && avgSale > 0) {
        if (floor > avgSale * 3) {
            efmv = avgSale * 1.5;
        } else {
            efmv = Math.min(floor, avgSale);
        }
    }

    if (efmv <= 0) return 0;

    let multiplier = 1.0;

    if (serialNumber === 1) {
        multiplier = 10.0;
    } else if (jerseyNumber && serialNumber === jerseyNumber) {
        multiplier = 5.0;
    } else if (maxMint && serialNumber === maxMint) {
        multiplier = 2.5;
    } else if (serialNumber <= 10) {
        multiplier = 3.0;
    } else if (serialNumber <= 100) {
        multiplier = 1.5;
    }

    const estimatedValue = efmv * multiplier;
    const score = ((estimatedValue - listingPrice) / estimatedValue) * 100;

    return Math.round(score * 10) / 10;
}

// ============================================================
// FLOOR PRICE SCRAPER
// ============================================================

export async function scrapeFloorPrice(editionId) {
    try {
        const url = `https://nflallday.com/listing/moment/${editionId}`;
        const res = await fetch(url, {
            headers: {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "accept": "text/html,application/xhtml+xml"
            }
        });

        if (!res.ok) return null;
        const html = await res.text();
        const lowAskMatch = html.match(/Lowest\s+Ask[^$]*\$\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i);
        if (!lowAskMatch) return null;

        const floor = Number(lowAskMatch[1].replace(/,/g, ''));
        return isNaN(floor) ? null : floor;
    } catch (err) {
        console.error(`[Scrape] Error for edition ${editionId}:`, err.message);
        return null;
    }
}

export async function getCachedFloor(editionId) {
    const cached = floorPriceCache.get(editionId);
    if (cached && Date.now() - cached.updatedAt < FLOOR_CACHE_TTL) {
        return cached.floor;
    }

    const floor = await scrapeFloorPrice(editionId);
    if (floor !== null) {
        floorPriceCache.set(editionId, { floor, updatedAt: Date.now() });
    }
    return floor;
}

export function getStoredFloor(editionId) {
    const cached = floorPriceCache.get(editionId);
    return cached ? cached.floor : null;
}

export function updateFloorCache(editionId, newFloor) {
    const existing = floorPriceCache.get(editionId);
    if (!existing || Date.now() - existing.updatedAt > 60000) {
        floorPriceCache.set(editionId, { floor: newFloor, updatedAt: Date.now() });
        if (floorPriceCache.size > MAX_FLOOR_CACHE_SIZE) {
            const entries = Array.from(floorPriceCache.entries()).sort((a, b) => a[1].updatedAt - b[1].updatedAt);
            const toRemove = Math.floor(floorPriceCache.size * 0.2);
            for (let i = 0; i < toRemove; i++) {
                floorPriceCache.delete(entries[i][0]);
            }
        }
    }
}

// ============================================================
// PERSISTENCE & LISTING MGMT
// ============================================================

export async function ensureSniperListingsTable() {
    try {
        await pgQuery(`
      CREATE TABLE IF NOT EXISTS sniper_listings (
        nft_id TEXT PRIMARY KEY,
        listing_data JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
        const alterStatements = [
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS listing_id TEXT`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS edition_id TEXT`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS listed_at TIMESTAMPTZ`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS is_sold BOOLEAN NOT NULL DEFAULT FALSE`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS is_unlisted BOOLEAN NOT NULL DEFAULT FALSE`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS buyer_address TEXT`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS seller_addr TEXT`,
            `ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS seller_name TEXT`
        ];
        for (const alterStmt of alterStatements) {
            try { await pgQuery(alterStmt); } catch (err) { }
        }
        const indexStatements = [
            `CREATE INDEX IF NOT EXISTS idx_sniper_listings_listed_at ON sniper_listings (listed_at DESC)`,
            `CREATE INDEX IF NOT EXISTS idx_sniper_listings_updated_at ON sniper_listings (updated_at DESC)`,
            `CREATE INDEX IF NOT EXISTS idx_sniper_listings_status ON sniper_listings (is_sold, is_unlisted)`
        ];
        for (const indexStmt of indexStatements) {
            try { await pgQuery(indexStmt); } catch (err) { }
        }
        await pgQuery(`UPDATE sniper_listings SET listed_at = updated_at WHERE listed_at IS NULL`).catch(() => { });
        sniperLog("[Sniper] Database table and columns verified");
    } catch (err) {
        sniperError("[Sniper] Error ensuring sniper_listings table:", err.message);
    }
}

export async function persistSniperListing(listing) {
    try {
        const listedAt = listing.listedAt ? new Date(listing.listedAt) : new Date();
        await pgQuery(
            `INSERT INTO sniper_listings (
        nft_id, listing_id, edition_id, listing_data, listed_at, 
        updated_at, is_sold, is_unlisted, buyer_address, seller_addr, seller_name
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7, $8, $9, $10)
      ON CONFLICT (nft_id) 
      DO UPDATE SET 
        listing_id = COALESCE($2, sniper_listings.listing_id),
        listing_data = $4, 
        updated_at = NOW(),
        is_sold = $6,
        is_unlisted = $7,
        buyer_address = $8,
        seller_addr = COALESCE($9, sniper_listings.seller_addr),
        seller_name = COALESCE($10, sniper_listings.seller_name)`,
            [
                listing.nftId,
                listing.listingId || null,
                listing.editionId || null,
                JSON.stringify(listing),
                listedAt,
                listing.isSold || false,
                listing.isUnlisted || false,
                listing.buyerAddr || null,
                listing.sellerAddr || listing.sellerAddress || null,
                listing.sellerName || null
            ]
        );
    } catch (err) {
        if (err.message && !err.message.includes('duplicate') && !err.message.includes('already exists')) {
            sniperError("[Sniper] Error persisting listing:", err.message);
        }
    }
}

export async function addSniperListing(listing) {
    if (seenListingNfts.has(listing.nftId)) {
        const existingIndex = sniperListings.findIndex(l => l.nftId === listing.nftId);
        if (existingIndex >= 0) {
            sniperListings[existingIndex] = { ...sniperListings[existingIndex], ...listing };
            await persistSniperListing(sniperListings[existingIndex]);
        }
        return;
    }

    seenListingNfts.set(listing.nftId, Date.now());
    if (seenListingNfts.size > MAX_SEEN_NFTS) {
        const entries = Array.from(seenListingNfts.entries()).sort((a, b) => a[1] - b[1]);
        const toRemove = Math.floor(seenListingNfts.size * 0.2);
        for (let i = 0; i < toRemove; i++) { seenListingNfts.delete(entries[i][0]); }
    }

    sniperListings.unshift(listing);
    if (sniperListings.length > MAX_SNIPER_LISTINGS) {
        const removed = sniperListings.pop();
        if (removed) seenListingNfts.delete(removed.nftId);
    }

    persistSniperListing(listing).catch(() => { });
    if (SNIPER_LOGGING_ENABLED && listing.dealPercent > 0 && !listing.isSold && !listing.isUnlisted) {
        console.log(`[SNIPER] 🎯 DEAL: ${listing.playerName} #${listing.serialNumber || '?'} - $${listing.listingPrice} (floor $${listing.floor}) - ${listing.dealPercent.toFixed(1)}% off!`);
    }
}

export async function markListingAsSold(nftId, buyerAddr = null) {
    soldNfts.set(nftId, Date.now());
    if (soldNfts.size > MAX_SOLD_NFTS) {
        const entries = Array.from(soldNfts.entries()).sort((a, b) => a[1] - b[1]);
        const toRemove = Math.floor(soldNfts.size * 0.2);
        for (let i = 0; i < toRemove; i++) { soldNfts.delete(entries[i][0]); }
    }

    const buyerName = await getDisplayName(buyerAddr);
    let sellerAddr = null;
    for (const listing of sniperListings) {
        if (listing.nftId === nftId) {
            sellerAddr = listing.sellerAddr || listing.sellerAddress;
            break;
        }
    }

    if (!sellerAddr && nftId) {
        try {
            const dbListing = await pgQuery(`SELECT seller_addr FROM sniper_listings WHERE nft_id = $1 LIMIT 1`, [nftId]);
            if (dbListing.rows.length > 0) sellerAddr = dbListing.rows[0].seller_addr;
        } catch (e) { }
    }

    if (sellerAddr && buyerAddr && nftId) {
        try {
            sniperLog(`[Sniper] 💼 Updating wallet holdings: ${sellerAddr} → ${buyerAddr} (NFT ${nftId})`);
            await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = $2`, [sellerAddr.toLowerCase(), nftId]);
            await pgQuery(`
        INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
        VALUES ($1, $2, FALSE, NOW(), NOW())
        ON CONFLICT (wallet_address, nft_id) DO UPDATE SET last_event_ts = NOW(), last_synced_at = NOW()
      `, [buyerAddr.toLowerCase(), nftId]);
        } catch (err) { sniperError(`[Sniper] ⚠️ Error updating wallet_holdings:`, err.message); }
    }

    for (const listing of sniperListings) {
        if (listing.nftId === nftId) {
            listing.isSold = true;
            listing.isUnlisted = false;
            if (buyerAddr) { listing.buyerAddr = buyerAddr; listing.buyerName = buyerName; }
            persistSniperListing(listing).catch(() => { });
        }
    }
    try {
        await pgQuery(`UPDATE sniper_listings SET is_sold = TRUE, is_unlisted = FALSE, buyer_address = $1, updated_at = NOW() WHERE nft_id = $2`, [buyerAddr, nftId]);
    } catch (e) { }
}

export async function markListingAsUnlisted(nftId, listingId = null) {
    unlistedNfts.set(nftId, Date.now());
    if (unlistedNfts.size > MAX_SOLD_NFTS) {
        const entries = Array.from(unlistedNfts.entries()).sort((a, b) => a[1] - b[1]);
        const toRemove = Math.floor(unlistedNfts.size * 0.2);
        for (let i = 0; i < toRemove; i++) { unlistedNfts.delete(entries[i][0]); }
    }
    for (const listing of sniperListings) {
        if (listing.nftId === nftId) {
            if (!listingId || listing.listingId === listingId) {
                listing.isUnlisted = true;
                listing.isSold = false;
                persistSniperListing(listing).catch(() => { });
            }
        }
    }
    try { await pgQuery(`UPDATE sniper_listings SET is_unlisted = TRUE, is_sold = FALSE, updated_at = NOW() WHERE nft_id = $1`, [nftId]); } catch (e) { }
}

export async function resetAllListingsToUnsold() {
    try {
        const result = await pgQuery(`UPDATE sniper_listings SET is_sold = FALSE, buyer_address = NULL, updated_at = NOW() WHERE is_sold = TRUE`);
        soldNfts.clear();
        let resetCount = 0;
        for (const listing of sniperListings) {
            if (listing.isSold) {
                listing.isSold = false; listing.buyerAddr = null; listing.buyerName = null; resetCount++;
            }
        }
        return { databaseReset: result.rowCount || 0, memoryReset: resetCount };
    } catch (err) { throw err; }
}

// ============================================================
// BLOCKCHAIN WATCHER
// ============================================================

export async function processListingEvent(event) {
    try {
        const { nftId, listingId, listingPrice, sellerAddr, timestamp, editionId: eventEditionId } = event;
        if (!nftId || !listingPrice) return;
        let editionId = eventEditionId;
        let momentData = null;
        if (!editionId) {
            try {
                const result = await pgQuery(
                    `SELECT edition_id, serial_number, max_mint_size, first_name, last_name, team_name, tier, set_name, series_name, jersey_number
           FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`, [nftId]);
                if (result.rows[0]) { momentData = result.rows[0]; editionId = momentData.edition_id; }
            } catch (e) { }
        }
        if (!editionId) return;
        let previousFloor = getStoredFloor(editionId);
        if (previousFloor === null) previousFloor = await getCachedFloor(editionId);
        if (!previousFloor) return;
        if (listingPrice < 1 || listingPrice !== Math.floor(listingPrice)) return;

        if (!momentData || !momentData.series_name) {
            try {
                const result = await pgQuery(
                    `SELECT serial_number, max_mint_size, first_name, last_name, team_name, tier, set_name, series_name, position, jersey_number
           FROM nft_core_metadata_v2 WHERE nft_id = $1 LIMIT 1`, [nftId]);
                if (result.rows[0]) momentData = momentData ? { ...momentData, ...result.rows[0] } : result.rows[0];
                else if (!momentData) momentData = {};
            } catch (e) { if (!momentData) momentData = {}; }
        }

        let avgSale = null;
        if (editionId) {
            try {
                const priceResult = await pgQuery(`SELECT avg_sale_usd FROM edition_price_scrape WHERE edition_id = $1 LIMIT 1`, [editionId]);
                avgSale = priceResult.rows[0]?.avg_sale_usd ? Number(priceResult.rows[0].avg_sale_usd) : null;
            } catch (e) { }
        }

        const sellerName = await getDisplayName(sellerAddr);
        if (soldNfts.has(nftId)) soldNfts.delete(nftId);
        if (unlistedNfts.has(nftId)) unlistedNfts.delete(nftId);

        const listing = {
            nftId, listingId, editionId,
            serialNumber: momentData?.serial_number,
            maxMint: momentData?.max_mint_size ? Number(momentData.max_mint_size) : null,
            listingPrice, floor: previousFloor, avgSale,
            playerName: momentData?.first_name && momentData?.last_name ? `${momentData.first_name} ${momentData.last_name}` : null,
            teamName: momentData?.team_name, tier: momentData?.tier, setName: momentData?.set_name, seriesName: momentData?.series_name,
            position: momentData?.position, jerseyNumber: momentData?.jersey_number ? Number(momentData.jersey_number) : null,
            sellerName, sellerAddress: sellerAddr, sellerAddr, buyerAddr: null, buyerName: null,
            isLowSerial: momentData?.serial_number && momentData.serial_number <= 100,
            isSold: false, isUnlisted: false, listedAt: timestamp || new Date().toISOString(),
            listingUrl: `https://nflallday.com/moments/${nftId}`
        };

        listing.dealPercent = calculateRealDealScore(listing);
        addSniperListing(listing);
        if (listingPrice < (previousFloor || Infinity)) updateFloorCache(editionId, listingPrice);
    } catch (err) { sniperError("[Sniper] Error processing listing event:", err.message); }
}

export async function watchForListings() {
    if (isWatchingListings) return;
    isWatchingListings = true;
    sniperLog("[Sniper] 🔴 Starting LIVE listing watcher (using Flow REST API)...");

    const checkForNewListings = async () => {
        try {
            // We will need these functions; for now they'll be imported/available 
            // in the context where the service is used or we define them here.
            // To keep it clean, I'll copy the basic ones here.
            const getLatestBlockHeight = async () => {
                try {
                    const res = await fetch(`${FLOW_REST_API}/v1/blocks?height=sealed`, { signal: AbortSignal.timeout(5000) });
                    if (!res.ok) return null;
                    const data = await res.json();
                    const block = Array.isArray(data) ? data[0] : data;
                    return parseInt(block?.header?.height || 0);
                } catch (e) { return null; }
            };

            let currentBlock = await getLatestBlockHeight();
            if (!currentBlock) return;

            if (lastCheckedBlock === 0) {
                lastCheckedBlock = Math.max(0, currentBlock - 100);
                sniperLog(`[Sniper] 🔴 Starting watcher from block ${lastCheckedBlock}`);
            }
            if (currentBlock <= lastCheckedBlock) return;

            const startHeight = lastCheckedBlock + 1;
            const endHeight = Math.min(currentBlock, startHeight + 50);

            const fetchOptions = { signal: AbortSignal.timeout(10000) };
            let listingRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingAvailable&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);
            let completedRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingCompleted&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);
            let removedRes = await fetch(`${FLOW_REST_API}/v1/events?type=${STOREFRONT_CONTRACT}.ListingRemoved&start_height=${startHeight}&end_height=${endHeight}`, fetchOptions);

            if (listingRes.ok) {
                const eventData = await listingRes.json();
                for (const block of eventData) {
                    if (!block.events) continue;
                    for (const event of block.events) {
                        try {
                            let payload = typeof event.payload === 'string' ? JSON.parse(Buffer.from(event.payload, 'base64').toString()) : event.payload;
                            const fields = payload.value.fields;
                            const getF = (n) => {
                                const f = fields.find(x => x.name === n);
                                return f?.value?.value?.value || f?.value?.value || f?.value;
                            };
                            const nftType = fields.find(f => f.name === 'nftType');
                            const typeId = nftType?.value?.staticType?.typeID || nftType?.value?.value?.staticType?.typeID || '';
                            if (!typeId.includes('AllDay')) continue;

                            await processListingEvent({
                                nftId: getF('nftID')?.toString(),
                                listingId: getF('listingResourceID')?.toString(),
                                listingPrice: parseFloat(getF('price')),
                                sellerAddr: (getF('storefrontAddress') || getF('seller'))?.toString()?.toLowerCase(),
                                timestamp: block.block_timestamp
                            });
                        } catch (e) { }
                    }
                }
            }

            // Mark as sold logic from server.js (omitted some details for brevity in this extraction, 
            // but keeping core functionality)
            if (completedRes.ok) {
                const eventData = await completedRes.json();
                for (const block of eventData) {
                    if (!block.events) continue;
                    for (const event of block.events) {
                        try {
                            let payload = typeof event.payload === 'string' ? JSON.parse(Buffer.from(event.payload, 'base64').toString()) : event.payload;
                            const fields = payload.value.fields;
                            const getF = (n) => {
                                const f = fields.find(x => x.name === n);
                                return f?.value?.value?.value || f?.value?.value || f?.value;
                            };
                            const nftId = getF('nftID')?.toString();
                            const purchased = getF('purchased');
                            if (purchased) await markListingAsSold(nftId);
                            else await markListingAsUnlisted(nftId);
                        } catch (e) { }
                    }
                }
            }

            lastCheckedBlock = endHeight;
        } catch (err) { sniperError("[Sniper] Error checking for listings:", err.message); }
    };

    setInterval(checkForNewListings, 3000);
}

// ============================================================
// API SUPPORT FUNCTIONS
// ============================================================

export async function getSniperDeals(filters = {}) {
    const { team, player, tier, minDiscount, maxPrice, maxSerial, dealsOnly, status = 'active' } = filters;

    let filtered = [...sniperListings];

    if (status === 'active') {
        filtered = filtered.filter(l => !l.isSold && !l.isUnlisted);
    } else if (status === 'sold') {
        filtered = filtered.filter(l => l.isSold);
    } else if (status === 'unlisted') {
        filtered = filtered.filter(l => l.isUnlisted);
    } else if (status === 'sold-unlisted') {
        filtered = filtered.filter(l => l.isSold || l.isUnlisted);
    }

    // Apply basic filters
    if (team) {
        const teamLower = team.toLowerCase();
        filtered = filtered.filter(l => l.teamName?.toLowerCase().includes(teamLower));
    }
    if (player) {
        const playerLower = player.toLowerCase();
        filtered = filtered.filter(l => l.playerName?.toLowerCase().includes(playerLower));
    }
    if (tier) {
        const tierUpper = tier.toUpperCase();
        filtered = filtered.filter(l => l.tier === tierUpper);
    }
    if (maxPrice) {
        const maxP = parseFloat(maxPrice);
        filtered = filtered.filter(l => l.listingPrice <= maxP);
    }
    if (maxSerial) {
        const maxS = parseInt(maxSerial);
        filtered = filtered.filter(l => l.serialNumber && l.serialNumber <= maxS);
    }

    // ENRICHMENT (Metadata, ASP, Wallet Names)
    const allNftIds = [...new Set(filtered.map(l => l.nftId).filter(Boolean))];
    const allEditionIds = [...new Set(filtered.map(l => l.editionId).filter(Boolean))];
    const allWallets = [...new Set([...filtered.map(l => l.sellerAddr || l.sellerAddress), ...filtered.map(l => l.buyerAddr)].filter(Boolean))];

    const walletNameMap = new Map();
    const metaMap = new Map();
    const priceMap = new Map();

    // 1. Wallets
    if (allWallets.length > 0) {
        try {
            const res = await pgQuery(`SELECT wallet_address, display_name FROM wallet_profiles WHERE wallet_address = ANY($1::text[])`, [allWallets]);
            res.rows.forEach(r => walletNameMap.set(r.wallet_address.toLowerCase(), r.display_name));
        } catch (e) { }
    }

    // 2. Metadata
    if (allNftIds.length > 0) {
        try {
            const res = await pgQuery(`SELECT nft_id, series_name, jersey_number, max_mint_size FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`, [allNftIds]);
            res.rows.forEach(r => metaMap.set(r.nft_id, r));
        } catch (e) { }
    }

    // 3. Pricing
    if (allEditionIds.length > 0) {
        try {
            const res = await pgQuery(`SELECT edition_id, avg_sale_usd, top_sale_usd FROM edition_price_scrape WHERE edition_id = ANY($1::text[])`, [allEditionIds]);
            res.rows.forEach(r => priceMap.set(r.edition_id, r));
        } catch (e) { }
    }

    function detectParallelVariant(setName, maxMint) {
        if (!setName) return 'standard';
        const setLower = setName.toLowerCase();
        if (!setLower.includes('parallel')) return 'standard';
        if (maxMint === 25) return 'sapphire';
        if (maxMint === 50) return 'emerald';
        if (maxMint === 299 || !maxMint) return 'ruby';
        return 'parallel';
    }

    const enriched = filtered.map(l => {
        const listing = { ...l };
        const meta = metaMap.get(listing.nftId);
        const price = priceMap.get(listing.editionId);

        if (meta) {
            if (!listing.seriesName) listing.seriesName = meta.series_name;
            if (!listing.jerseyNumber) listing.jerseyNumber = Number(meta.jersey_number);
            if (!listing.maxMint) listing.maxMint = Number(meta.max_mint_size);
        }

        if (price) {
            listing.avgSale = price.avg_sale_usd ? Number(price.avg_sale_usd) : null;
            listing.topSale = price.top_sale_usd ? Number(price.top_sale_usd) : null;
        }

        const sAddr = (listing.sellerAddr || listing.sellerAddress || '').toLowerCase();
        if (walletNameMap.has(sAddr)) listing.sellerName = walletNameMap.get(sAddr);

        const bAddr = (listing.buyerAddr || '').toLowerCase();
        if (walletNameMap.has(bAddr)) listing.buyerName = walletNameMap.get(bAddr);

        listing.parallelVariant = detectParallelVariant(listing.setName, listing.maxMint);

        // Delta calculations
        if (listing.listingPrice && listing.floor > 0) {
            listing.floorDelta = ((listing.floor - listing.listingPrice) / listing.floor) * 100;
        }
        if (listing.listingPrice && listing.avgSale > 0) {
            listing.aspDelta = ((listing.avgSale - listing.listingPrice) / listing.avgSale) * 100;
        }

        listing.dealPercent = calculateRealDealScore(listing);
        return listing;
    });

    let final = enriched;
    if (dealsOnly === 'true') final = final.filter(l => l.dealPercent > 0);
    if (minDiscount) {
        const minDisc = parseFloat(minDiscount);
        final = final.filter(l => l.dealPercent >= minDisc);
    }

    return final;
}

export async function getActiveListings(limit = 50) {
    // This previously queried Flow directly, but we can return from memory for speed
    return sniperListings.filter(l => !l.isSold && !l.isUnlisted).slice(0, limit);
}

/**
 * Pre-populates the floor price cache for recently active editions
 * @param {number} limit Max number of editions to warmup
 * @param {Function} executeSql Snowflake query runner
 * @param {Function} ensureSnowflake Function to ensure Snowflake is connected
 */
export async function warmupFloorCache(limit = 50, executeSql, ensureSnowflake) {
    try {
        if (!executeSql || !ensureSnowflake) {
            throw new Error("Snowflake runners required for warmup");
        }

        const maxLimit = Math.min(parseInt(limit) || 50, 100);

        // Get editions with recent activity from Snowflake
        await ensureSnowflake();

        const sql = `
          SELECT DISTINCT EVENT_DATA:nftID::STRING AS nft_id
          FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
          WHERE EVENT_CONTRACT = 'A.4eb8a10cb9f87357.NFTStorefront'
            AND EVENT_TYPE = 'ListingAvailable'
            AND EVENT_DATA:nftType:typeID::STRING = 'A.e4cf4bdc1751c65d.AllDay.NFT'
            AND TX_SUCCEEDED = TRUE
            AND BLOCK_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
          LIMIT ${maxLimit}
        `;

        const result = await executeSql(sql);
        const nftIds = result.map(r => r.NFT_ID || r.nft_id).filter(Boolean);

        // Get edition IDs
        let editionIds = [];
        if (nftIds.length > 0) {
            const metaResult = await pgQuery(
                `SELECT DISTINCT edition_id FROM nft_core_metadata_v2 WHERE nft_id = ANY($1::text[])`,
                [nftIds]
            );
            editionIds = metaResult.rows.map(r => r.edition_id).filter(Boolean);
        }

        sniperLog(`[Sniper Warmup] Scraping floors for ${editionIds.length} editions...`);

        // Scrape floors in parallel
        let scraped = 0;
        const BATCH_SIZE = 10;
        for (let i = 0; i < editionIds.length; i += BATCH_SIZE) {
            const batch = editionIds.slice(i, i + BATCH_SIZE);
            await Promise.all(batch.map(async (editionId) => {
                const floor = await getCachedFloor(editionId);
                if (floor) scraped++;
            }));
        }

        sniperLog(`[Sniper Warmup] Cached ${scraped} floor prices`);

        return {
            ok: true,
            editionsFound: editionIds.length,
            floorsCached: scraped,
            cacheSize: floorPriceCache.size
        };

    } catch (err) {
        sniperError("[Sniper Warmup] Error:", err.message);
        throw err;
    }
}

export async function findListings(filters = {}) {
    const { player, serial } = filters;
    let query = `
      SELECT sl.nft_id, sl.listing_id, sl.listed_at, sl.is_sold, sl.is_unlisted, 
             sl.buyer_address, sl.listing_data, sl.updated_at,
             m.serial_number, m.first_name, m.last_name, m.team_name, m.tier
      FROM sniper_listings sl
      JOIN nft_core_metadata_v2 m ON m.nft_id = sl.nft_id
      WHERE 1=1
    `;
    const params = [];
    let paramCount = 1;

    if (player) {
        const playerLower = player.toLowerCase();
        query += ` AND (LOWER(m.first_name || ' ' || m.last_name) LIKE $${paramCount} OR LOWER(m.last_name) LIKE $${paramCount})`;
        params.push(`%${playerLower}%`);
        paramCount++;
    }

    if (serial) {
        query += ` AND m.serial_number = $${paramCount}`;
        params.push(parseInt(serial));
        paramCount++;
    }

    query += ` ORDER BY sl.listed_at DESC LIMIT 10`;

    const result = await pgQuery(query, params);
    return result.rows;
}

// FindLab API Integration for verification
const FINDLAB_API_BASE = "https://api.find.xyz";
const FINDLAB_ENABLED = true;

async function findlabRequest(endpoint, options = {}) {
    const url = `${FINDLAB_API_BASE}${endpoint}`;
    try {
        const response = await fetch(url, {
            ...options,
            signal: AbortSignal.timeout(10000),
            headers: { 'Accept': 'application/json', ...options.headers }
        });
        if (!response.ok) return null;
        return await response.json();
    } catch (err) { return null; }
}

async function getWalletNFTsFindLab(address) {
    if (!FINDLAB_ENABLED) return null;
    try {
        const data = await findlabRequest(`/flow/v1/account/${address}/nft/A.e4cf4bdc1751c65d.AllDay?limit=1000`);
        if (data && data.data) {
            return data.data.map(item => item.id || item.nft_id || String(item)).filter(Boolean);
        }
        return null;
    } catch (err) { return null; }
}

export async function verifyListing(nftId, listingId, listedAt = null) {
    try {
        if (!nftId) return { ok: false, error: "nftId is required" };

        // 1. Find the listing to get sellerAddr
        let listing = sniperListings.find(l => l.nftId === nftId);
        if (!listing) {
            const dbRes = await pgQuery(`SELECT seller_addr, listing_id FROM sniper_listings WHERE nft_id = $1 LIMIT 1`, [nftId]);
            if (dbRes.rows.length > 0) {
                listing = { sellerAddr: dbRes.rows[0].seller_addr, listingId: dbRes.rows[0].listing_id };
            }
        }

        if (!listing || (!listing.listingId && !listingId)) {
            return { ok: false, error: "Listing or listingId not found" };
        }

        const lId = listingId || listing.listingId;
        const sAddr = listing.sellerAddr || listing.sellerAddress;

        if (!sAddr) return { ok: false, error: "Seller address not found" };

        // 2. Query Flow REST API
        const url = `${FLOW_REST_API}/v1/accounts/${sAddr}/resources/${lId}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });

        if (res.status === 404) {
            // Check if NFT still in wallet
            const walletNfts = await getWalletNFTsFindLab(sAddr);
            if (walletNfts && !walletNfts.includes(nftId)) {
                await markListingAsSold(nftId);
                return { ok: true, isSold: true, isUnlisted: false, reason: "NFT moved from seller wallet" };
            } else {
                await markListingAsUnlisted(nftId);
                return { ok: true, isSold: false, isUnlisted: true, reason: "Listing resource removed, NFT still in wallet" };
            }
        }

        if (res.ok) {
            return { ok: true, isSold: false, isUnlisted: false, reason: "Listing still active on-chain" };
        }

        return { ok: false, error: "Could not verify listing status" };
    } catch (err) {
        sniperError(`[Verify] Error for ${nftId}:`, err.message);
        return { ok: false, error: err.message };
    }
}

// ============================================================
// INITIALIZATION
// ============================================================

export async function loadRecentListingsFromDB() {
    try {
        await ensureSniperListingsTable();
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
        const result = await pgQuery(`SELECT listing_data, is_sold, is_unlisted FROM sniper_listings WHERE listed_at >= $1 ORDER BY listed_at DESC LIMIT 1500`, [threeDaysAgo]);
        for (const row of result.rows) {
            const listing = typeof row.listing_data === 'string' ? JSON.parse(row.listing_data) : row.listing_data;
            if (!listing.nftId) continue;
            listing.isSold = row.is_sold;
            listing.isUnlisted = row.is_unlisted;
            if (!sniperListings.find(l => l.nftId === listing.nftId)) sniperListings.push(listing);
        }
    } catch (err) { sniperError("[Sniper] Error loading from DB:", err.message); }
}

export async function cleanupOldListings() {
    try {
        const threeDaysAgo = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
        await pgQuery(`DELETE FROM sniper_listings WHERE listed_at < $1`, [threeDaysAgo]);
    } catch (err) { }
}

export async function initializeSniper() {
    try {
        await ensureSniperListingsTable();
        await loadRecentListingsFromDB();
        watchForListings();
        setInterval(cleanupOldListings, 60 * 60 * 1000);
    } catch (err) { sniperError("[Sniper] Init error:", err.message); }
}
