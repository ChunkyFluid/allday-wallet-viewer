import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function fetchContractCode() {
    console.log("Fetching NFTLocker contract code...");
    try {
        const account = await fcl.send([
            fcl.getAccount("0xb6f2481eba4df97b")
        ]).then(fcl.decode);

        const code = account.contracts["NFTLocker"];
        if (code) {
            console.log("Contract Code found! Writing to contract_utf8.cdc");
            fs.writeFileSync('contract_utf8.cdc', code, 'utf8');
        } else {
            console.log("No NFTLocker contract found on this account.");
        }
    } catch (err) {
        console.error("Error:", err);
    }
}

fetchContractCode();
