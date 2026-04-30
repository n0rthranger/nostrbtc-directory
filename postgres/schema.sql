-- Directory schema
-- Open-source subset: directory profiles, trust graph, and related tables

-- Directory profiles (populated by directory indexer)
CREATE TABLE IF NOT EXISTS directory_profiles (
    hex_pubkey      TEXT PRIMARY KEY,
    display_name    TEXT,
    name            TEXT,
    about           TEXT,
    picture         TEXT,
    banner          TEXT,
    nip05           TEXT,
    lud16           TEXT,
    lud06           TEXT,
    website         TEXT,
    follower_count  INTEGER DEFAULT 0,
    following_count INTEGER DEFAULT 0,
    note_count      INTEGER DEFAULT 0,
    eigentrust_score REAL DEFAULT 0.0,
    lightning_reachable BOOLEAN,
    nip05_verified  BOOLEAN,
    badges          JSONB DEFAULT '[]'::JSONB,
    first_event_at  TIMESTAMPTZ,
    first_event_block INTEGER,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Directory API columns (pre-computed by directory indexer)
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS npub TEXT;
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS nip05_display TEXT DEFAULT '';
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS event_count INTEGER DEFAULT 0;
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS last_active BIGINT DEFAULT 0;
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS trust_count INTEGER DEFAULT 0;
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS subscription_created TEXT DEFAULT '';
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS directory_tags TEXT DEFAULT '[]';
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS card_url TEXT DEFAULT '';
ALTER TABLE directory_profiles ADD COLUMN IF NOT EXISTS reputation_score INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_directory_eigentrust ON directory_profiles(eigentrust_score DESC);
CREATE INDEX IF NOT EXISTS idx_directory_updated ON directory_profiles(updated_at);
CREATE INDEX IF NOT EXISTS idx_dirprof_last_active ON directory_profiles(last_active);
CREATE INDEX IF NOT EXISTS idx_dirprof_reputation ON directory_profiles(reputation_score DESC);
CREATE INDEX IF NOT EXISTS idx_dirprof_subscription_created ON directory_profiles(subscription_created);

-- Trust graph edges
CREATE TABLE IF NOT EXISTS trust_edges (
    source_pubkey   TEXT NOT NULL,
    target_pubkey   TEXT NOT NULL,
    edge_type       TEXT NOT NULL CHECK (edge_type IN ('follow', 'mute', 'report', 'reply', 'zap', 'reaction', 'repost')),
    weight          REAL NOT NULL DEFAULT 1.0,
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_pubkey, target_pubkey, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_trust_edges_target ON trust_edges(target_pubkey);
CREATE INDEX IF NOT EXISTS idx_trust_edges_updated ON trust_edges(last_seen_at);

-- Activity heatmap (pre-computed daily event counts per member)
CREATE TABLE IF NOT EXISTS activity_heatmap (
    pubkey          TEXT NOT NULL,
    day             TEXT NOT NULL,
    event_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pubkey, day)
);

-- Directory zaps (zap flow tracking between members)
CREATE TABLE IF NOT EXISTS directory_zaps (
    event_id        TEXT PRIMARY KEY,
    sender          TEXT NOT NULL,
    receiver        TEXT NOT NULL,
    amount_msats    BIGINT NOT NULL,
    created_at      BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dir_zaps_sender ON directory_zaps(sender);
CREATE INDEX IF NOT EXISTS idx_dir_zaps_receiver ON directory_zaps(receiver);
CREATE INDEX IF NOT EXISTS idx_dir_zaps_created ON directory_zaps(created_at);

-- Card links (custom links on user profile cards)
CREATE TABLE IF NOT EXISTS card_links (
    id              SERIAL PRIMARY KEY,
    pubkey          TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT NOT NULL,
    sort_order      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_card_links_pubkey ON card_links(pubkey);

-- Personalized trust scores (per-observer GrapeRank via Java algorithm)
CREATE TABLE IF NOT EXISTS personalized_scores (
    observer_pubkey TEXT NOT NULL,
    target_pubkey TEXT NOT NULL,
    score REAL NOT NULL,
    tier TEXT NOT NULL,
    hops INTEGER DEFAULT 3,
    computed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (observer_pubkey, target_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_personalized_observer ON personalized_scores(observer_pubkey);
CREATE INDEX IF NOT EXISTS idx_personalized_target ON personalized_scores(target_pubkey);

CREATE TABLE IF NOT EXISTS global_consensus_scores (
    target_pubkey TEXT PRIMARY KEY,
    score REAL NOT NULL DEFAULT 0,
    tier TEXT NOT NULL DEFAULT 'unverified',
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS house_graperank_scores (
    target_pubkey TEXT PRIMARY KEY,
    score REAL NOT NULL DEFAULT 0,
    tier TEXT NOT NULL DEFAULT 'unverified',
    hops INTEGER,
    average_score REAL,
    confidence REAL,
    total_input REAL,
    verified BOOLEAN DEFAULT FALSE,
    verified_followers INTEGER DEFAULT 0,
    source_observer TEXT NOT NULL,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_house_graperank_score
    ON house_graperank_scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_house_graperank_source
    ON house_graperank_scores(source_observer);
