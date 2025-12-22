import flowService from "./services/flow-blockchain.js";

async function testSimpleFetch() {
    try {
        const nftId = "938885";
        const walletAddress = "0xddfbe848a81b2236";

        console.log(`Fetching SIMPLE metadata from blockchain: ID=${nftId} Wallet=${walletAddress}`);
        const details = await flowService.getNFTMetadata(walletAddress, parseInt(nftId));

        console.log("SIMPLE_BLOCKCHAIN_DETAILS:");
        console.log(JSON.stringify(details, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

testSimpleFetch();
