// Hybrid Custody Trading - FCL Integration
// Handles on-chain NFT transfers and swaps using Flow Hybrid Custody

// Import Cadence transaction code
const TRANSFER_NFT_TX = `
import NonFungibleToken from 0x1d7e57aa55817448
import HybridCustody from 0xd8a7e05a7ac670c0
import AllDay from 0xe4cf4bdc1751c65d

transaction(childAddress: Address, nftId: UInt64, recipientAddress: Address) {
    let providerRef: auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}
    let receiverRef: &{NonFungibleToken.CollectionPublic}
    
    prepare(signer: auth(Storage) &Account) {
        // Get a reference to the signer's HybridCustody.Manager from storage
        let managerRef = signer.storage.borrow<auth(HybridCustody.Manage) &HybridCustody.Manager>(
            from: HybridCustody.ManagerStoragePath
        ) ?? panic("Could not borrow reference to HybridCustody.Manager - make sure wallet is linked")
        
        // Borrow a reference to the signer's specified child account
        let account = managerRef
            .borrowAccount(addr: childAddress)
            ?? panic("Signer does not have access to specified child account")
        
        // AllDay NFT collection storage path and type
        let storagePath = AllDay.CollectionStoragePath
        let collectionType = Type<auth(NonFungibleToken.Withdraw) &AllDay.Collection>()
        
        // Get the Capability Controller ID for the AllDay collection type
        let controllerID = account.getControllerIDForType(
            type: collectionType,
            forPath: storagePath
        ) ?? panic("Could not find Capability controller ID for AllDay collection")
        
        // Get a reference to the child NFT Provider
        let cap = account.getCapability(
            controllerID: controllerID,
            type: Type<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>()
        ) ?? panic("Cannot access NonFungibleToken.Provider from this child account")
        
        // Cast the Capability
        let providerCap = cap as! Capability<auth(NonFungibleToken.Withdraw) &{NonFungibleToken.Provider}>
        self.providerRef = providerCap.borrow() ?? panic("Provider capability is invalid - cannot borrow reference")
        
        // Get receiver reference for recipient
        self.receiverRef = getAccount(recipientAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow() ?? panic("Could not borrow recipient's AllDay collection")
    }
    
    execute {
        // Withdraw the NFT from the child account
        let nft <- self.providerRef.withdraw(withdrawID: nftId)
        // Deposit to recipient
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

// Direct transfer transaction - for when user controls their own wallet (no Hybrid Custody)
const DIRECT_TRANSFER_NFT_TX = `
import NonFungibleToken from 0x1d7e57aa55817448
import AllDay from 0xe4cf4bdc1751c65d

transaction(nftId: UInt64, recipientAddress: Address) {
    let collection: auth(NonFungibleToken.Withdraw) &AllDay.Collection
    let receiverRef: &{NonFungibleToken.CollectionPublic}
    
    prepare(signer: auth(Storage) &Account) {
        // Borrow the signer's AllDay collection
        self.collection = signer.storage.borrow<auth(NonFungibleToken.Withdraw) &AllDay.Collection>(
            from: AllDay.CollectionStoragePath
        ) ?? panic("Could not borrow AllDay Collection from signer")
        
        // Get recipient's collection
        self.receiverRef = getAccount(recipientAddress)
            .capabilities.get<&{NonFungibleToken.CollectionPublic}>(AllDay.CollectionPublicPath)
            .borrow() ?? panic("Could not borrow recipient's collection")
    }
    
    execute {
        let nft <- self.collection.withdraw(withdrawID: nftId)
        self.receiverRef.deposit(token: <-nft)
    }
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

    // Direct transfer NFT (for direct Dapper wallet login, no Hybrid Custody)
    directTransferNFT: async function (nftId, recipientAddress, onStatus) {
        if (!this.fcl) throw new Error('FCL not initialized');

        onStatus?.('Starting direct transfer...');

        try {
            const txId = await this.fcl.mutate({
                cadence: DIRECT_TRANSFER_NFT_TX,
                args: (arg, t) => [
                    arg(nftId.toString(), t.UInt64),
                    arg(recipientAddress, t.Address)
                ],
                proposer: this.fcl.authz,
                payer: this.fcl.authz,
                authorizations: [this.fcl.authz],
                limit: 999
            });

            onStatus?.('Transaction submitted, waiting for confirmation...');
            console.log('[HybridCustody] Direct transfer txId:', txId);

            // Wait for transaction to be sealed
            const result = await this.fcl.tx(txId).onceSealed();

            if (result.status === 4) {
                onStatus?.('Transfer complete!');
                return { success: true, txId, result };
            } else {
                throw new Error('Transaction failed: ' + result.errorMessage);
            }
        } catch (err) {
            console.error('[HybridCustody] directTransferNFT error:', err);
            onStatus?.('Transfer failed: ' + err.message);
            return { success: false, error: err.message };
        }
    },

    // Execute trade swap - transfer NFTs to trade partner
    // This is called by each party to send their NFTs
    executeTradeSwap: async function (tradeData, onStatus) {
        if (!this.fcl) throw new Error('FCL not initialized');

        const currentUser = await this.getCurrentUser();
        if (!currentUser) throw new Error('Not connected to Flow wallet');

        onStatus?.('Checking wallet setup...');

        // Get linked child accounts (Hybrid Custody model)
        // This is REQUIRED for NFL All Day - users must have a parent Flow wallet
        // with their Dapper wallet linked as a child account
        const linkedWallets = await this.getLinkedAccounts(currentUser);

        if (linkedWallets.length === 0) {
            throw new Error(
                `No linked Dapper wallet found for address ${currentUser.substring(0, 10)}...\n\n` +
                'This usually means you connected with Dapper instead of your parent wallet.\n\n' +
                'In Flow Wallet, switch to your parent account (the one that has Dapper linked BELOW it with a 🔗 icon), then try again.'
            );
        }

        const walletToUse = linkedWallets[0];
        onStatus?.(`Using linked wallet: ${walletToUse.substring(0, 10)}...`);

        const { nftIds, recipientAddress } = tradeData;

        if (!nftIds || nftIds.length === 0) {
            return { success: true, txIds: [], message: 'No NFTs to transfer' };
        }

        if (!recipientAddress) {
            throw new Error('Recipient address required');
        }

        onStatus?.(`Transferring ${nftIds.length} NFT(s)...`);

        const txIds = [];
        const errors = [];

        // Transfer each NFT using Hybrid Custody
        for (let i = 0; i < nftIds.length; i++) {
            const nftId = nftIds[i];
            onStatus?.(`Transferring NFT ${i + 1}/${nftIds.length} (ID: ${nftId})...`);

            try {
                const result = await this.transferNFT(
                    walletToUse,
                    nftId,
                    recipientAddress,
                    (status) => onStatus?.(`NFT ${i + 1}: ${status}`)
                );

                if (result.success) {
                    txIds.push(result.txId);
                } else {
                    errors.push({ nftId, error: result.error });
                }
            } catch (err) {
                errors.push({ nftId, error: err.message });
            }
        }

        if (errors.length > 0 && txIds.length === 0) {
            throw new Error(`All transfers failed: ${errors.map(e => e.error).join(', ')}`);
        }

        onStatus?.(`Completed ${txIds.length}/${nftIds.length} transfers`);

        return {
            success: txIds.length > 0,
            txIds,
            errors,
            message: errors.length > 0
                ? `${txIds.length} succeeded, ${errors.length} failed`
                : `All ${txIds.length} NFTs transferred successfully!`
        };
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
