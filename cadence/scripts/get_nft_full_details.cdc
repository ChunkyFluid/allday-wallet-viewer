// Get full details for a specific NFT including edition, play, series, set
// Combines multiple metadata views for complete information

// Get full details for a specific NFT including edition, play, series, set
// Combines multiple metadata views for complete information
// Updated for Cadence 1.0+ syntax

import AllDay from 0xe4cf4bdc1751c65d
import MetadataViews from 0x1d7e57aa55817448
import NonFungibleToken from 0x1d7e57aa55817448

access(all) struct NFTDetails {
    access(all) let nftId: UInt64
    access(all) let editionId: String?
    access(all) let playId: UInt64?
    access(all) let seriesId: UInt64?
    access(all) let setId: UInt64?
    access(all) let tier: String?
    access(all) let serialNumber: UInt64?
    access(all) let maxMintSize: UInt64?
    access(all) let playerName: String?
    access(all) let teamName: String?
    access(all) let position: String?
    
    init(
        nftId: UInt64,
        editionId: String?,
        playId: UInt64?,
        seriesId: UInt64?,
        setId: UInt64?,
        tier: String?,
        serialNumber: UInt64?,
        maxMintSize: UInt64?,
        playerName: String?,
        teamName: String?,
        position: String?
    ) {
        self.nftId = nftId
        self.editionId = editionId
        self.playId = playId
        self.seriesId = seriesId
        self.setId = setId
        self.tier = tier
        self.serialNumber = serialNumber
        self.maxMintSize = maxMintSize
        self.playerName = playerName
        self.teamName = teamName
        self.position = position
    }
}

access(all) fun main(address: Address, nftId: UInt64): NFTDetails? {
    let account = getAccount(address)
    
    // In Cadence 1.0+, use capabilities.get<Type>(path)
    let collectionRef = account.capabilities.get<&{NonFungibleToken.CollectionPublic, MetadataViews.ResolverCollection}>(AllDay.CollectionPublicPath)
        .borrow()
        ?? return nil
    
    let nft = collectionRef.borrowNFT(id: nftId)
        ?? return nil
    
    let resolver = nft as! &{MetadataViews.Resolver}
    
    // Extract metadata from various views
    var editionId: String? = nil
    var serialNumber: UInt64? = nil
    var maxMintSize: UInt64? = nil
    var tier: String? = nil
    var playerName: String? = nil
    var teamName: String? = nil
    var position: String? = nil
    var playId: UInt64? = nil
    var seriesId: UInt64? = nil
    var setId: UInt64? = nil
    
    // Get Editions view
    let editions = MetadataViews.getEditions(resolver)
    if editions != nil && editions!.editions.length > 0 {
        let edition = editions!.editions[0]
        editionId = edition.name
        maxMintSize = edition.maxSize
        serialNumber = edition.serialNumber
    }
    
    // Get Serial view (fallback)
    if serialNumber == nil {
        let serial = MetadataViews.getSerial(resolver)
        if serial != nil {
            serialNumber = serial!.number
        }
    }
    
    // Get Traits view
    let traits = MetadataViews.getTraits(resolver)
    if traits != nil {
        for trait in traits! {
            if trait.name == "Tier" {
                tier = trait.value.toString()
            } else if trait.name == "Player" || trait.name == "PlayerName" {
                playerName = trait.value.toString()
            } else if trait.name == "Team" || trait.name == "TeamName" {
                teamName = trait.value.toString()
            } else if trait.name == "Position" {
                position = trait.value.toString()
            }
        }
    }
    
    // Get Display view for additional info
    let display = MetadataViews.getDisplay(resolver)
    if display != nil {
        if playerName == nil && display!.name != nil {
            playerName = display!.name
        }
    }
    
    return NFTDetails(
        nftId: nftId,
        editionId: editionId,
        playId: playId,
        seriesId: seriesId,
        setId: setId,
        tier: tier,
        serialNumber: serialNumber,
        maxMintSize: maxMintSize,
        playerName: playerName,
        teamName: teamName,
        position: position
    )
}

