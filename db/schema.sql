-- NOTE: Legacy schema file
-- This DDL was from an earlier iteration of the project and is not
-- used by the current app. The live schema is created/managed by
-- init_db.js and the various ETL scripts instead.

CREATE TABLE IF NOT EXISTS editions (
    edition_id          TEXT PRIMARY KEY,
    set_id              TEXT,
    set_name            TEXT,
    series_id           TEXT,
    series_name         TEXT,
    tier                TEXT,
    max_mint_size       INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS plays (
    play_id             TEXT PRIMARY KEY,
    player_name         TEXT,
    team_name           TEXT,
    position            TEXT,
    game_date           DATE,
    opponent            TEXT,
    game_type           TEXT,
    description         TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS moments (
    nft_id              TEXT PRIMARY KEY,
    edition_id          TEXT NOT NULL REFERENCES editions(edition_id),
    play_id             TEXT REFERENCES plays(play_id),
    serial_number       INTEGER,
    minted_at           TIMESTAMPTZ,
    burned_at           TIMESTAMPTZ,
    current_owner       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallets (
    wallet_address      TEXT PRIMARY KEY,
    username            TEXT,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_holdings (
    wallet_address      TEXT NOT NULL REFERENCES wallets(wallet_address),
    nft_id              TEXT NOT NULL REFERENCES moments(nft_id),
    acquired_at         TIMESTAMPTZ,
    disposition         TEXT,
    last_updated_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (wallet_address, nft_id)
);

CREATE INDEX IF NOT EXISTS idx_moments_edition_id ON moments(edition_id);
CREATE INDEX IF NOT EXISTS idx_moments_play_id ON moments(play_id);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet ON wallet_holdings(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallet_holdings_nft ON wallet_holdings(nft_id);
