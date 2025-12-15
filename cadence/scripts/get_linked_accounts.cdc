// Get all linked (child) accounts for a parent address
// Uses HybridCustody to discover Dapper wallets linked to a Flow wallet

import HybridCustody from 0xd8a7e05a7ac670c0

access(all) fun main(parent: Address): [Address] {
    let acct = getAuthAccount<auth(Storage) &Account>(parent)
    
    // Check if the account has a HybridCustody Manager
    let manager = acct.storage.borrow<&HybridCustody.Manager>(from: HybridCustody.ManagerStoragePath)
    
    if manager == nil {
        return [] // No linked accounts
    }
    
    // Return all child addresses linked to this parent
    return manager!.getChildAddresses()
}
