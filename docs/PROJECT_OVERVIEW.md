# NFL All Day Wallet Viewer - Project Overview

> **Purpose**: This document provides AI assistants and developers with a comprehensive understanding of the project architecture, known issues, and development patterns.

## 🎯 Project Purpose

A full-stack web application for exploring **NFL ALL DAY** (Dapper Labs' NFL NFT platform) wallets, moments, and collections. Users can:
- View wallet contents by pasting a Flow wallet address
- Browse top holders, leaderboards, and collection insights
- Search moments by player, team, tier, set, series
- Track prices, set completion, and trading opportunities

**Live URL**: Hosted on Render.com

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├─────────────────────────────────────────────────────────────────┤
│  Flow Blockchain ──► Real-time Events ──► PostgreSQL            │
│      (WebSocket)      (Deposit/Withdraw)    (app database)      │
│                                                                  │
│  Dapper API ──► Display Names, Pricing                          │
│  FindLab API ──► Wallet NFT lookups (fallback)                  │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BACKEND                                  │
├─────────────────────────────────────────────────────────────────┤
│  server.js (Express, 410KB+)                                     │
│  ├── REST APIs: /api/query, /api/wallet-summary, /api/top-*     │
│  ├── Authentication: Sessions, Google OAuth                     │
│  ├── Flow blockchain queries via Cadence scripts                │
│  └── WebSocket for real-time transaction feed                   │
├─────────────────────────────────────────────────────────────────┤
│  services/                                                       │
│  ├── flow-blockchain.js - FCL integration, Cadence execution    │
│  ├── event-processor.js - Real-time blockchain event handler    │
│  └── findlabs-client.js - FindLab API fallback                  │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTEND                                  │
├─────────────────────────────────────────────────────────────────┤
│  public/                                                         │
│  ├── index.html - Main wallet viewer                            │
│  ├── explorer.html - Moment search/browse                       │
│  ├── sniper.html - Real-time listing tracker                    │
│  ├── trades.html - Peer-to-peer trading (WIP)                   │
│  ├── set-completion.html - Set progress tracker                 │
│  └── 20+ other pages for profiles, insights, etc.               │
│                                                                  │
│  Vanilla HTML/CSS/JS - No framework                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 💾 Database Schema (PostgreSQL on Render)

### Core Tables
| Table | Purpose |
|-------|---------|
| `nft_core_metadata` | NFT details: edition_id, player name, team, tier, serial, set, series |
| `wallet_holdings` | Who owns what: wallet_address, nft_id, is_locked, last_event_ts |
| `wallet_profiles` | Display names from Dapper API |
| `edition_price_scrape` | Pricing: lowest_ask, avg_sale, top_sale per edition |

### Normalized Tables (Snowflake-independent)
| Table | Purpose |
|-------|---------|
| `series` | Series lookup (id, name) |
| `sets` | Set lookup (id, name) |
| `plays` | Play/player info |
| `editions` | Edition details with FK to plays, sets, series |
| `nfts` | Individual NFTs with FK to editions |
| `holdings` | Current ownership with is_locked status |

### Snapshot Tables (for fast reads)
| Table | Purpose |
|-------|---------|
| `top_wallets_snapshot` | Leaderboard data |
| `editions_snapshot` | Aggregated edition stats |
| `explorer_filters_snapshot` | Filter options cache |
| `set_editions_snapshot` | Set/edition mapping |

---

## 🔄 Data Sync Pipeline

### Real-time Updates (Primary)
The app now uses **real-time blockchain events** via WebSocket:
- `Deposit` / `Withdraw` → Updates `holdings` and `wallet_holdings`
- `NFTLocked` / `NFTUnlocked` → Updates `is_locked` status
- `MomentNFTMinted` / `MomentNFTBurned` → Updates `nfts` table

### Manual Sync Scripts (in `scripts/`)
```bash
# Sync display names from Dapper API
node scripts/sync_wallet_profiles_from_dapper.js

# Rebuild leaderboards
node scripts/sync_leaderboards.js

# Master sync (single wallet or all)
node scripts/master_sync.js --wallet=0x...   # Single wallet
```

> **Note**: Snowflake sync scripts have been deprecated and moved to `scripts/deprecated/snowflake/`. The app no longer requires Snowflake access for normal operation.

---

## 📁 Key Files Reference

### Backend
| File | Purpose |
|------|---------|
| `server.js` | Main Express server (very large, 410KB+) |
| `db.js` | PostgreSQL connection pool |
| `services/flow-blockchain.js` | FCL integration, Cadence script execution |

### Cadence Scripts (`cadence/scripts/`)
| File | Purpose |
|------|---------|
| `get_wallet_nft_ids.cdc` | Get all NFT IDs in a wallet |
| `get_nft_full_details.cdc` | Get metadata for a specific NFT |
| `get_locked_nft_ids.cdc` | Get locked NFT IDs from NFTLocker |
| `fetch_nfts_from_linked.cdc` | Hybrid Custody - fetch from linked accounts |

### Frontend Pages
| Page | Purpose |
|------|---------|
| `index.html` | Main wallet viewer with filters |
| `explorer.html` | Browse/search all moments |
| `sniper.html` | Real-time listing monitor |
| `trades.html` | P2P trading interface (WIP, hidden from nav) |
| `set-completion.html` | Set progress tracker |
| `profiles.html` | User search by display name |
| `top-holders.html` | Leaderboards |

---

## ⚠️ Known Issues & Tech Debt

### Critical
1. **Missing NFT Metadata** - ~500 NFTs (6052xxx-6065xxx range) missing from `nft_core_metadata`. Fix: Rebuild Snowflake `ALLDAY_CORE_NFT_METADATA` table, then re-sync.

2. **Hybrid Custody Loop** - When executing trades via Dapper wallet with Hybrid Custody, the app loops back to "Connect Parent Wallet" instead of proceeding.

### Technical Debt
1. **server.js is 410KB+** - Should be split into route modules
2. **Cadence 1.0+ Syntax** - Some Cadence scripts may have outdated syntax (recently fixed `get_nft_full_details.cdc`)
3. **Price Data Staleness** - Prices from Dapper scraping can be outdated

---

## 🔧 Development Patterns

### Adding a New API Endpoint
1. Add route in `server.js`
2. Use `pgQuery()` for database access
3. Pattern: `app.get("/api/endpoint", async (req, res) => { ... })`

### Adding a New Frontend Page
1. Create `public/newpage.html`
2. Include `layout.js` for shared header/nav
3. Add to navigation in `public/header.html` if needed

### Syncing Data
1. For single wallet: `node scripts/master_sync.js --wallet=0x...`
2. For all data: `node scripts/master_sync.js`
3. For metadata refresh: Run Snowflake backfill SQL first

---

## 🚀 Running Locally

```bash
# 1. Install dependencies
npm install

# 2. Copy environment template
cp .env.example .env

# 3. Fill in .env with:
#    - DATABASE_URL (Render PostgreSQL)
#    - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#    - FLOW_ACCESS_NODE (optional, defaults to mainnet)

# 4. Start server
npm run dev
# Open http://localhost:3000
```

---

## 📊 Environment Variables

### Required
| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string (Render) |
| `SESSION_SECRET` | Express session secret |

### Optional
| Variable | Purpose |
|----------|---------|
| `FLOW_ACCESS_NODE` | Flow RPC endpoint (default: mainnet) |

### Deprecated (Snowflake)
| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT` | *(no longer required)* |
| `SNOWFLAKE_USER` | *(no longer required)* |
| `SNOWFLAKE_PASSWORD` | *(no longer required)* |
| `SNOWFLAKE_DATABASE` | *(no longer required)* |
| `SNOWFLAKE_SCHEMA` | *(no longer required)* |

---

## 🔗 External Dependencies

| Service | Purpose |
|---------|---------|
| **Flow Blockchain** | NFT ownership, real-time events |
| **Snowflake** | Historical data, analytics |
| **Render.com** | PostgreSQL + hosting |
| **Dapper API** | Display names, profile info |
| **NFL All Day GraphQL** | Pricing, listings (via cookies) |

---

## 📝 Git Workflow

- **main** - Production branch (deployed to Render)
- **dev** - Development branch for testing

Always push changes to `dev` first, then merge to `main` for deployment.

---

*Last Updated: December 17, 2025*
