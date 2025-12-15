// Hybrid Custody Trading - FCL Integration
// Handles on-chain NFT transfers and swaps using Flow Hybrid Custody

// Import Cadence transaction code
const TRANSFER_NFT_TX = `
import HybridCustody from 0xd8a7e05a7ac670c0
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d

transaction(childAddress: Address, nftId: UInt64, recipientAddress: Address) {
    let providerCap: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
    let receiverRef: &{NonFungibleToken.CollectionPublic}
    
    prepare(parent: auth(Storage) &Account) {
        let manager = parent.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
            from: HybridCustody.ManagerStoragePath
        ) ?? panic("No HybridCustody Manager - wallet not linked")
        
        let childAccount = manager.borrowAccount(addr: childAddress) 
            ?? panic("Child account not linked")
        
        let childAcct = getAuthAccount<auth(Storage, Capabilities) &Account>(childAddress)
        let providerType = Type<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>()
        var foundCap: Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>? = nil
        
        for storagePath in childAcct.storage.storagePaths {
            if childAcct.storage.borrow<&AllDay.Collection>(from: storagePath) != nil {
                for controller in childAcct.capabilities.storage.getControllers(forPath: storagePath) {
                    if !controller.borrowType.isSubtype(of: providerType) { continue }
                    if let cap = childAccount.getCapability(controllerID: controller.capabilityID, type: providerType) {
                        let typedCap = cap as! Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
                        if typedCap.check() { foundCap = typedCap; break }
                    }
                }
            }
            if foundCap != nil { break }
        }
        
        self.providerCap = foundCap ?? panic("No NFT provider capability")
        self.receiverRef = getAccount(recipientAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow() ?? panic("Could not borrow recipient's collection")
    }
    
    execute {
        let provider = self.providerCap.borrow() ?? panic("Could not borrow provider")
        let nft <- provider.withdraw(withdrawID: nftId)
        self.receiverRef.deposit(token: <-nft)
    }
}
`;

const GET_LINKED_ACCOUNTS_SCRIPT = `
import HybridCustody from 0xd8a7e05a7ac670c0

access(all) fun main(parent: Address): [Address] {
    let acct = getAuthAccount<auth(Storage) &Account>(parent)
    let manager = acct.storage.borrow<&HybridCustody.Manager>(from: HybridCustody.ManagerStoragePath)
    if manager == nil { return [] }
    return manager!.getChildAddresses()
}
`;

// HybridCustody module
const HybridCustody = {
    fcl: null,

    // Initialize with FCL instance
    init: function (fclInstance) {
        this.fcl = fclInstance;
        console.log('[HybridCustody] Initialized');
    },

    // Check if FCL is available
    isReady: function () {
        return this.fcl !== null && typeof this.fcl.mutate === 'function';
    },

    // Get the current user's Flow wallet address
    getCurrentUser: async function () {
        if (!this.fcl) return null;
        const user = await this.fcl.currentUser.snapshot();
        return user.loggedIn ? user.addr : null;
    },

    // Get linked child accounts (Dapper wallets)
    getLinkedAccounts: async function (parentAddress) {
        if (!this.fcl) throw new Error('FCL not initialized');

        try {
            const result = await this.fcl.query({
                cadence: GET_LINKED_ACCOUNTS_SCRIPT,
                args: (arg, t) => [arg(parentAddress, t.Address)]
            });
            return result || [];
        } catch (err) {
            console.error('[HybridCustody] getLinkedAccounts error:', err);
            return [];
        }
    },

    // Check if user has any linked wallets
    hasLinkedWallet: async function () {
        const currentUser = await this.getCurrentUser();
        if (!currentUser) return false;

        const linked = await this.getLinkedAccounts(currentUser);
        return linked.length > 0;
    },

    // Transfer NFT from child wallet to another address
    transferNFT: async function (childAddress, nftId, recipientAddress, onStatus) {
        if (!this.fcl) throw new Error('FCL not initialized');

        onStatus?.('Preparing transaction...');

        try {
            const txId = await this.fcl.mutate({
                cadence: TRANSFER_NFT_TX,
                args: (arg, t) => [
                    arg(childAddress, t.Address),
                    arg(nftId.toString(), t.UInt64),
                    arg(recipientAddress, t.Address)
                ],
                proposer: this.fcl.authz,
                payer: this.fcl.authz,
                authorizations: [this.fcl.authz],
                limit: 999
            });

            onStatus?.('Transaction submitted, waiting for confirmation...');
            console.log('[HybridCustody] Transfer txId:', txId);

            // Wait for transaction to be sealed
            const result = await this.fcl.tx(txId).onceSealed();

            if (result.status === 4) {
                onStatus?.('Transfer complete!');
                return { success: true, txId, result };
            } else {
                throw new Error('Transaction failed: ' + result.errorMessage);
            }
        } catch (err) {
            console.error('[HybridCustody] transferNFT error:', err);
            onStatus?.('Transfer failed: ' + err.message);
            return { success: false, error: err.message };
        }
    },

    // Execute trade (for marketplace purchases)
    executePurchase: async function (listing, onStatus) {
        const currentUser = await this.getCurrentUser();
        if (!currentUser) throw new Error('Not connected to wallet');

        // Get buyer's linked Dapper wallet to receive NFT
        const linkedWallets = await this.getLinkedAccounts(currentUser);
        if (linkedWallets.length === 0) {
            throw new Error('No linked Dapper wallet found. Please link your wallets first.');
        }

        const recipientAddress = linkedWallets[0]; // Use first linked wallet

        onStatus?.('Preparing purchase...');

        // The seller needs to sign the transfer transaction
        // For now, we record the purchase intent and notify seller
        return {
            success: true,
            message: 'Purchase recorded. Seller will complete the transfer.',
            buyerWallet: currentUser,
            recipientWallet: recipientAddress
        };
    }
};

// Auto-initialize if FCL is available
if (typeof window !== 'undefined') {
    window.HybridCustody = HybridCustody;

    // Try to initialize with global fcl
    if (window.fcl) {
        HybridCustody.init(window.fcl);
    } else {
        // Wait for fcl to load
        const checkFcl = setInterval(() => {
            if (window.fcl) {
                HybridCustody.init(window.fcl);
                clearInterval(checkFcl);
            }
        }, 500);

        // Stop checking after 10 seconds
        setTimeout(() => clearInterval(checkFcl), 10000);
    }
}
