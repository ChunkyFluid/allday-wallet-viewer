import flowService from "./services/flow-blockchain.js";

async function testWalletIds() {
    try {
        const walletAddress = "0xddfbe848a81b2236";
        console.log(`Getting IDs for wallet ${walletAddress}`);
        const ids = await flowService.getWalletNFTIds(walletAddress);
        console.log("IDs:", ids.length);
        console.log("Contains 938885?", ids.includes(938885));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

testWalletIds();
