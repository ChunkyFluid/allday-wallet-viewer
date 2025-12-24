import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function fetchMarketContract() {
    console.log("Fetching TopShotMarketV3 contract code...");
    try {
        const account = await fcl.send([
            fcl.getAccount("0xc1e4f4f4c4257510")
        ]).then(fcl.decode);

        const code = account.contracts["TopShotMarketV3"];
        if (code) {
            console.log("Contract Code found! Writing to market_contract.cdc");
            fs.writeFileSync('market_contract.cdc', code, 'utf8');
        } else {
            console.log("No TopShotMarketV3 contract found on this account.");
        }
    } catch (err) {
        console.error("Error:", err.message);
    }
}

fetchMarketContract();
