import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const IDS = ['6055517', '6057067'];

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkIds() {
    const script = `
    import AllDay from 0xe4cf4bdc1751c65d
    
    access(all) fun main(nftId: UInt64): {String: String}? {
        // We can't easily find the owner without a wallet, 
        // but we can try to see if the NFT exists in the contract if we have more info.
        // Actually, we need to know WHERE it is (which wallet).
        // Since we know it's in Junglerules' wallet according to our DB, let's check there.
        return nil
    }
  `;
    // I'll just use getNFTMetadata from the service
    const { getNFTMetadata } = await import("./services/flow-blockchain.js");
    const address = '0xcfd9bad75352b43b';

    for (const id of IDS) {
        console.log(`Checking ${id}...`);
        const meta = await getNFTMetadata(address, Number(id));
        console.log(`Result for ${id}:`, JSON.stringify(meta));
    }
    process.exit(0);
}

checkIds();
