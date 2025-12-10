// Discovery helper for FindLabs NFT collection shape (NFL All Day)
// Usage: FINDLABS_API_KEY=... node scripts/discover_findlabs_nft.js
import * as dotenv from "dotenv";
dotenv.config();

import { findlabsClient } from "../services/findlabs-client.js";

const NFLAD_CONTRACT = (process.env.NFLAD_CONTRACT || "0xe4cf4bdc1751c65d").toLowerCase();
const EXPLICIT_NFT_TYPE = process.env.FINDLABS_NFT_TYPE;

function icIncludes(haystack, needle) {
  return (haystack || "").toLowerCase().includes((needle || "").toLowerCase());
}

function pickBestCollection(collections) {
  if (!Array.isArray(collections)) return null;

  // Prefer explicit env override
  if (EXPLICIT_NFT_TYPE) {
    const exact = collections.find(
      (c) =>
        c.nft_type === EXPLICIT_NFT_TYPE ||
        c.identifier === EXPLICIT_NFT_TYPE ||
        c.id === EXPLICIT_NFT_TYPE
    );
    if (exact) return exact;
  }

  // Look for contract match
  const contractMatched = collections.find((c) => {
    const addr =
      (c.contract_address ||
        c.address ||
        (c.contract && c.contract.address) ||
        (c.contract && c.contract.contractAddress) ||
        "").toLowerCase();
    return addr === NFLAD_CONTRACT;
  });
  if (contractMatched) return contractMatched;

  // Look for name match
  const nameMatched = collections.find(
    (c) => icIncludes(c.name, "all day") || icIncludes(c.project_name, "all day") || icIncludes(c.slug, "allday") || icIncludes(c.symbol, "all day")
  );
  if (nameMatched) return nameMatched;

  // Fallback: any collection that mentions "nfl"
  const nfl = collections.find((c) => icIncludes(c.name, "nfl") || icIncludes(c.project_name, "nfl"));
  return nfl || null;
}

async function main() {
  console.log("[discover] fetching collections...");
  const collections = await findlabsClient.get("/flow/v1/nft");

  if (!collections || (Array.isArray(collections) && collections.length === 0)) {
    console.error("[discover] No collections returned; check auth/key.");
    return;
  }

  const list = Array.isArray(collections?.data) ? collections.data : Array.isArray(collections) ? collections : collections?.items || [];
  console.log(`[discover] collections returned: ${list.length}`);

  const collection = pickBestCollection(list);
  if (!collection) {
    console.error("[discover] Could not automatically identify NFL All Day collection. Please set FINDLABS_NFT_TYPE.");
    // Show a few candidates
    console.log("[discover] sample collections (first 5):", list.slice(0, 5));
    return;
  }

  const nftType = collection.nft_type || collection.identifier || collection.id || collection.slug;
  console.log("[discover] Selected collection:", {
    nftType,
    name: collection.name || collection.project_name,
    contract: collection.contract_address || collection.address || (collection.contract && collection.contract.address)
  });

  console.log("[discover] fetching first page of items for", nftType);
  const itemsResp = await findlabsClient.get(`/flow/v1/nft/${encodeURIComponent(nftType)}/item`, {
    query: { page: 1, page_size: 50 }
  });

  const items = Array.isArray(itemsResp?.data)
    ? itemsResp.data
    : Array.isArray(itemsResp)
      ? itemsResp
      : itemsResp?.items || [];

  console.log(`[discover] items returned: ${items.length}`);

  if (items.length) {
    const sample = items[0];
    console.log("[discover] sample item keys:", Object.keys(sample));
    console.log("[discover] sample item (truncated to common fields):", {
      id: sample.id || sample.nft_id || sample.item_id,
      name: sample.name || sample.display_name || sample.metadata?.name,
      set: sample.set || sample.set_name || sample.metadata?.set,
      team: sample.team || sample.team_name || sample.metadata?.team,
      price_fields: {
        floor: sample.floor || sample.floor_price || sample.lowest_ask_usd,
        avg: sample.avg_price || sample.avg_sale_usd,
        top: sample.top_sale_usd
      }
    });
  } else {
    console.log("[discover] no items returned on first page; the endpoint may use different pagination params.");
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

