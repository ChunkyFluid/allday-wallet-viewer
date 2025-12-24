import { pgQuery } from "./db.js";
import fs from "fs";

async function investigate() {
    try {
        const ids = ['9443518', '9443519', '938885'];
        const res = await pgQuery(`
      SELECT h.nft_id, h.wallet_address, m.first_name, m.last_name, m.team_name, m.set_name
      FROM wallet_holdings h
      LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
      WHERE h.nft_id IN ('9443518', '9443519', '938885')
    `);
        fs.writeFileSync("investigation_wallet_results.txt", JSON.stringify(res.rows, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

investigate();
