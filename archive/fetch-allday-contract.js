import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function fetchAllDayContract() {
    console.log("Fetching AllDay contract code...");
    try {
        const account = await fcl.send([
            fcl.getAccount("0xe4cf4bdc1751c65d")
        ]).then(fcl.decode);

        const code = account.contracts["AllDay"];
        if (code) {
            console.log("Contract Code found! Writing to allday_contract.cdc");
            fs.writeFileSync('allday_contract.cdc', code, 'utf8');
        } else {
            console.log("No AllDay contract found on this account.");
        }
    } catch (err) {
        console.error("Error:", err.message);
    }
}

fetchAllDayContract();
