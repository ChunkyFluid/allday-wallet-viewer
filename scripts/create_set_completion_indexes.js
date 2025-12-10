// Adds performance indexes for set completion endpoints without dropping tables
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function addIndexes() {
  console.log("Creating indexes for set completion...");
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_set ON nft_core_metadata(set_name);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_team ON nft_core_metadata(team_name);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_metadata_edition ON nft_core_metadata(edition_id);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_wallet ON wallet_holdings(wallet_address);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_nft ON wallet_holdings(nft_id);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_holdings_last_event ON wallet_holdings(last_event_ts);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_set_editions_set ON set_editions_snapshot(set_name);`);
  await pgQuery(`CREATE INDEX IF NOT EXISTS idx_set_editions_edition ON set_editions_snapshot(edition_id);`);
  console.log("✅ Indexes ensured.");
}

addIndexes()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

