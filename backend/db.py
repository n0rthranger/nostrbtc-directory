"""Database layer for the directory relay.

All data stored in PostgreSQL + Redis (directory member cache).
"""

import contextlib
import os
import time
import logging
import threading
import hashlib
import hmac
import json

_logger = logging.getLogger("directory.db")

# --- Postgres connection pool ---

_pg_pool = None
_pg_pool_lock = threading.Lock()
_pg_thread_local = threading.local()
_redis_conn = None


def _read_secret(name: str) -> str:
    """Read secret from /run/secrets/ file, falling back to env var."""
    try:
        with open(f"/run/secrets/{name}") as f:
            val = f.read().strip()
            if val:
                return val
    except FileNotFoundError:
        pass
    return os.environ.get(name, "")


def _sign_queue_message(payload: str) -> str:
    secret = _read_secret("GRAPERANK_QUEUE_SECRET") or _read_secret("AUTH_SECRET")
    if not secret:
        raise RuntimeError("GRAPERANK_QUEUE_SECRET or AUTH_SECRET must be set for Redis job signing")
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return json.dumps({"payload": payload, "hmac": sig}, separators=(",", ":"))


def _init_pool():
    """Initialize the connection pool (called once)."""
    global _pg_pool
    if _pg_pool is not None:
        return
    with _pg_pool_lock:
        if _pg_pool is not None:
            return
        import psycopg2.pool
        dsn = os.environ.get("DATABASE_URL", "")
        _pg_pool = psycopg2.pool.ThreadedConnectionPool(2, 20, dsn)
        _logger.info("db: Connection pool created (min=2, max=20)")


@contextlib.contextmanager
def get_pg_conn():
    """Context manager: get a connection from the pool, return it when done.

    Usage:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    global _pg_pool
    if _pg_pool is None:
        _init_pool()
    if _pg_pool is None:
        raise RuntimeError("Postgres pool not available")

    conn = _pg_pool.getconn()
    try:
        conn.autocommit = False
        yield conn
    finally:
        try:
            _pg_pool.putconn(conn)
        except Exception:
            pass


def _get_pg():
    """Get a Postgres connection from the pool (thread-local reuse).

    Returns the same connection for the same thread to avoid pool exhaustion.
    The connection is checked out once per thread and reused until the thread
    ends or the connection becomes unusable.
    """
    global _pg_pool
    if _pg_pool is None:
        _init_pool()
    if _pg_pool is None:
        _logger.error("Postgres pool not available")
        return None

    conn = getattr(_pg_thread_local, "conn", None)
    if conn is not None and not conn.closed:
        try:
            conn.rollback()  # clear any aborted transaction state
            return conn
        except Exception:
            # connection is broken, drop it
            try:
                _pg_pool.putconn(conn, close=True)
            except Exception:
                pass
            _pg_thread_local.conn = None

    try:
        conn = _pg_pool.getconn()
        conn.autocommit = False
        _pg_thread_local.conn = conn
        return conn
    except Exception as e:
        _logger.error(f"Postgres connection failed: {e}")
        return None

def _get_redis():
    """Get or create Redis connection."""
    global _redis_conn
    if _redis_conn is not None:
        return _redis_conn
    try:
        import redis
        _redis_conn = redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379"), decode_responses=True)
        _redis_conn.ping()
        return _redis_conn
    except Exception as e:
        _logger.error(f"Redis connection failed: {e}")
        return None

def init_pg():
    """Initialize Postgres + Redis connections. Call once at startup."""
    pg = _get_pg()
    r = _get_redis()
    if pg:
        _logger.info("db: Connected to PostgreSQL")
        _ensure_local_tables(pg)
    else:
        _logger.error("db: PostgreSQL not available - all operations will fail")
    if r:
        _logger.info("db: Connected to Redis")
    else:
        _logger.error("db: Redis not available - directory member cache unavailable")


def _ensure_local_tables(pg):
    """Create tables and add columns if they don't exist (idempotent)."""
    try:
        with pg.cursor() as cur:
            cur.execute("""
                -- Subscriptions table (tracks directory membership)
                CREATE TABLE IF NOT EXISTS subscriptions (
                    hex_pubkey TEXT PRIMARY KEY,
                    npub TEXT,
                    plan TEXT DEFAULT 'directory',
                    status TEXT DEFAULT 'active',
                    directory_listed BOOLEAN DEFAULT TRUE,
                    directory_tags TEXT DEFAULT '[]',
                    nip05_name TEXT,
                    nip05_expires_at TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '100 years',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Directory profiles (pre-computed by background indexer)
                CREATE TABLE IF NOT EXISTS directory_profiles (
                    hex_pubkey TEXT PRIMARY KEY,
                    npub TEXT,
                    name TEXT DEFAULT '',
                    picture TEXT DEFAULT '',
                    banner TEXT DEFAULT '',
                    website TEXT DEFAULT '',
                    about TEXT DEFAULT '',
                    lud16 TEXT DEFAULT '',
                    badges JSONB DEFAULT '[]',
                    nip05_display TEXT DEFAULT '',
                    event_count INTEGER DEFAULT 0,
                    last_active BIGINT DEFAULT 0,
                    trust_count INTEGER DEFAULT 0,
                    subscription_created TEXT DEFAULT '',
                    directory_tags TEXT DEFAULT '[]',
                    card_url TEXT DEFAULT '',
                    reputation_score INTEGER DEFAULT 0,
                    activity_score REAL DEFAULT 0,
                    identity_score REAL DEFAULT 0,
                    list_event_id TEXT,
                    self_signed BOOLEAN DEFAULT FALSE,
                    last_directory_visit TIMESTAMPTZ,
                    indexed_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_dirprof_last_active ON directory_profiles(last_active);
                CREATE INDEX IF NOT EXISTS idx_dirprof_reputation ON directory_profiles(reputation_score DESC);
                CREATE INDEX IF NOT EXISTS idx_dirprof_subscription_created ON directory_profiles(subscription_created);

                -- Trust edges (follow/mute/report relationships)
                CREATE TABLE IF NOT EXISTS trust_edges (
                    source_pubkey TEXT NOT NULL,
                    target_pubkey TEXT NOT NULL,
                    edge_type TEXT NOT NULL DEFAULT 'follow',
                    weight REAL DEFAULT 1.0,
                    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (source_pubkey, target_pubkey, edge_type)
                );
                ALTER TABLE trust_edges ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ DEFAULT NOW();
                CREATE INDEX IF NOT EXISTS idx_trust_edges_target ON trust_edges(target_pubkey, edge_type);
                CREATE INDEX IF NOT EXISTS idx_trust_edges_updated ON trust_edges(last_seen_at);

                -- Personalized scores (GrapeRank output)
                CREATE TABLE IF NOT EXISTS personalized_scores (
                    observer_pubkey TEXT NOT NULL,
                    target_pubkey TEXT NOT NULL,
                    score REAL NOT NULL DEFAULT 0,
                    tier TEXT NOT NULL DEFAULT 'unverified',
                    hops INTEGER,
                    computed_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (observer_pubkey, target_pubkey)
                );

                -- Activity heatmap
                CREATE TABLE IF NOT EXISTS activity_heatmap (
                    pubkey TEXT NOT NULL, day TEXT NOT NULL, event_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (pubkey, day)
                );

                -- Zap tracking
                CREATE TABLE IF NOT EXISTS directory_zaps (
                    event_id TEXT PRIMARY KEY, sender TEXT NOT NULL, receiver TEXT NOT NULL,
                    amount_msats BIGINT NOT NULL, created_at BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_dir_zaps_sender ON directory_zaps(sender);
                CREATE INDEX IF NOT EXISTS idx_dir_zaps_receiver ON directory_zaps(receiver);
                CREATE INDEX IF NOT EXISTS idx_dir_zaps_created ON directory_zaps(created_at);

                -- Card links
                CREATE TABLE IF NOT EXISTS card_links (
                    id SERIAL PRIMARY KEY, pubkey TEXT NOT NULL, title TEXT NOT NULL,
                    url TEXT NOT NULL, sort_order INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_card_links_pubkey ON card_links(pubkey);

                -- Trust clusters
                CREATE TABLE IF NOT EXISTS trust_clusters (
                    pubkey TEXT PRIMARY KEY,
                    cluster_id INTEGER NOT NULL DEFAULT -1,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
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
                CREATE TABLE IF NOT EXISTS trust_cluster_meta (
                    cluster_id INTEGER PRIMARY KEY,
                    label TEXT NOT NULL DEFAULT '',
                    color TEXT NOT NULL DEFAULT '#888888',
                    member_count INTEGER DEFAULT 0,
                    override_label TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Trust history snapshots
                CREATE TABLE IF NOT EXISTS trust_snapshots (
                    pubkey TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    reputation_score INTEGER NOT NULL DEFAULT 0,
                    trust_score REAL DEFAULT 0,
                    followers_count INTEGER DEFAULT 0,
                    PRIMARY KEY (pubkey, snapshot_date)
                );
                CREATE INDEX IF NOT EXISTS idx_trust_snap_pubkey ON trust_snapshots(pubkey);

                -- First-seen timestamp cache
                CREATE TABLE IF NOT EXISTS first_seen_cache (
                    pubkey TEXT PRIMARY KEY,
                    first_seen BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Relay state key-value store
                CREATE TABLE IF NOT EXISTS relay_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Activation audit log
                CREATE TABLE IF NOT EXISTS activation_log (
                    id SERIAL PRIMARY KEY,
                    hex_pubkey TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    metadata TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );

                -- Weekly digest tracking
                CREATE TABLE IF NOT EXISTS digest_history (
                    digest_date TEXT PRIMARY KEY,
                    event_id TEXT,
                    published_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        pg.commit()
    except Exception as e:
        _logger.error(f"_ensure_local_tables failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Redis helpers for directory_members set
# ---------------------------------------------------------------------------

def populate_directory_members_redis(pubkeys: set):
    """Replace the directory_members Redis set with the given pubkeys."""
    r = _get_redis()
    if not r:
        return
    try:
        pipe = r.pipeline()
        pipe.delete("directory:members")
        if pubkeys:
            pipe.sadd("directory:members", *pubkeys)
        pipe.execute()
    except Exception as e:
        _logger.error(f"populate_directory_members_redis failed: {e}")


def add_directory_member_redis(pubkey: str):
    """Add a single pubkey to the directory_members Redis set."""
    r = _get_redis()
    if not r:
        return
    try:
        r.sadd("directory:members", pubkey)
    except Exception as e:
        _logger.error(f"add_directory_member_redis failed: {e}")


def remove_directory_member_redis(pubkey: str):
    """Remove a single pubkey from the directory_members Redis set."""
    r = _get_redis()
    if not r:
        return
    try:
        r.srem("directory:members", pubkey)
    except Exception as e:
        _logger.error(f"remove_directory_member_redis failed: {e}")


# ---------------------------------------------------------------------------
# Directory listing activation
# ---------------------------------------------------------------------------

def activate_directory_listing(pubkey, npub, nip05_name=None):
    """Activate directory-only listing in Postgres."""
    pg = _get_pg()
    if not pg:
        _logger.error("activate_directory_listing: Postgres unavailable")
        return
    try:
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO subscriptions (hex_pubkey, npub, plan, status, directory_listed, nip05_name,
                                           expires_at, nip05_expires_at)
                VALUES (%s, %s, 'directory', 'active', TRUE, %s,
                        NOW() + INTERVAL '100 years',
                        CASE WHEN %s IS NOT NULL THEN NOW() + INTERVAL '1 year' ELSE NULL END)
                ON CONFLICT (hex_pubkey) DO UPDATE SET
                    directory_listed = TRUE,
                    plan = CASE WHEN subscriptions.status = 'expired' THEN 'directory' ELSE subscriptions.plan END,
                    status = CASE WHEN subscriptions.status = 'expired' THEN 'active' ELSE subscriptions.status END,
                    expires_at = CASE WHEN subscriptions.status = 'expired' THEN NOW() + INTERVAL '100 years' ELSE subscriptions.expires_at END,
                    updated_at = NOW(),
                    nip05_name = COALESCE(EXCLUDED.nip05_name, subscriptions.nip05_name),
                    nip05_expires_at = CASE
                        WHEN EXCLUDED.nip05_name IS NOT NULL AND subscriptions.plan = 'directory'
                        THEN NOW() + INTERVAL '1 year'
                        WHEN EXCLUDED.nip05_name IS NOT NULL
                        THEN subscriptions.expires_at
                        ELSE subscriptions.nip05_expires_at
                    END
            """, (pubkey, npub, nip05_name, nip05_name))
        pg.commit()
        _logger.info(f"activate_directory_listing: {pubkey[:16]}... nip05={nip05_name}")
    except Exception as e:
        _logger.error(f"activate_directory_listing failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass

    # Add to directory_members Redis set
    r = _get_redis()
    if r:
        try:
            r.sadd("directory:members", pubkey)
        except Exception as e:
            _logger.error(f"activate_directory_listing Redis failed: {e}")


def log_activation(pubkey, event_type, metadata=None):
    """Write to activation_log in Postgres (audit trail)."""
    pg = _get_pg()
    if pg:
        try:
            with pg.cursor() as cur:
                cur.execute("INSERT INTO activation_log (hex_pubkey, event_type, metadata) VALUES (%s, %s, %s)",
                           (pubkey, event_type, json.dumps(metadata) if metadata else None))
            pg.commit()
        except Exception as e:
            _logger.error(f"log_activation failed: {e}")
            try:
                pg.rollback()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Directory preferences (Postgres)
# ---------------------------------------------------------------------------

def get_directory_listed(pubkey):
    """Check if a subscriber is listed in the directory."""
    pg = _get_pg()
    if not pg:
        return False
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT directory_listed FROM subscriptions WHERE hex_pubkey = %s", (pubkey,))
            row = cur.fetchone()
        return bool(row and row[0])
    except Exception as e:
        _logger.error(f"get_directory_listed failed: {e}")
        return False


def set_directory_listed(pubkey, enabled):
    """Enable or disable directory listing for a subscriber."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("UPDATE subscriptions SET directory_listed = %s, updated_at = NOW() WHERE hex_pubkey = %s",
                       (enabled, pubkey))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_directory_listed failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def is_self_signed(pubkey):
    """Check if a member's directory listing is self-signed."""
    pg = _get_pg()
    if not pg:
        return False
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT self_signed FROM directory_profiles WHERE hex_pubkey = %s", (pubkey,))
            row = cur.fetchone()
        return bool(row and row[0])
    except Exception as e:
        _logger.error(f"is_self_signed failed: {e}")
        return False


def get_directory_tags(pubkey):
    """Get directory tags for a subscriber."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT directory_tags FROM subscriptions WHERE hex_pubkey = %s", (pubkey,))
            row = cur.fetchone()
        if row and row[0]:
            return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        return []
    except Exception as e:
        _logger.error(f"get_directory_tags failed: {e}")
        return []


def set_directory_tags(pubkey, tags):
    """Set directory tags for a subscriber. Max 10 tags, 30 chars each, alphanumeric+hyphen."""
    import re
    if not isinstance(tags, list):
        tags = []
    clean = []
    for t in tags[:10]:
        if isinstance(t, str) and re.match(r'^[a-zA-Z0-9_-]{1,30}$', t):
            clean.append(t.lower())
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("UPDATE subscriptions SET directory_tags = %s, updated_at = NOW() WHERE hex_pubkey = %s",
                       (json.dumps(clean), pubkey))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_directory_tags failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_directory_subscribers(full=False):
    """Get all active subscribers who opted into the directory.

    When *full=True*, also returns ``plan`` and ``created_at`` fields
    (used by the indexer for badge computation).
    """
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        if full:
            cols = "hex_pubkey AS pubkey, npub, nip05_name, directory_tags, plan, created_at::text AS created_at"
        else:
            cols = "hex_pubkey AS pubkey, npub, nip05_name, directory_tags"
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT {cols} FROM subscriptions WHERE directory_listed = TRUE AND expires_at > NOW()")
            rows = cur.fetchall()
        result = []
        for r in rows:
            tags = []
            if r["directory_tags"]:
                try:
                    tags = json.loads(r["directory_tags"]) if isinstance(r["directory_tags"], str) else r["directory_tags"]
                except (json.JSONDecodeError, TypeError):
                    pass
            entry = {
                "pubkey": r["pubkey"],
                "npub": r["npub"],
                "nip05_name": r["nip05_name"],
                "tags": tags,
            }
            if full:
                entry["plan"] = r["plan"]
                entry["created_at"] = r["created_at"]
            result.append(entry)
        return result
    except Exception as e:
        _logger.error(f"get_directory_subscribers failed: {e}")
        return []


def get_directory_subscribers_full():
    """Get all active directory-listed subscribers with created_at for badge computation.

    Convenience wrapper around ``get_directory_subscribers(full=True)``.
    """
    return get_directory_subscribers(full=True)


# ---------------------------------------------------------------------------
# Card links (Postgres)
# ---------------------------------------------------------------------------

def get_card_links(pubkey):
    """Get custom card links for a directory member."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT title, url FROM card_links WHERE pubkey = %s ORDER BY sort_order, id", (pubkey,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_card_links failed: {e}")
        return []


def save_card_links(pubkey, links):
    """Replace all card links for a directory member. links = [{"title": ..., "url": ...}, ...]"""
    import re
    MAX_LINKS = 20
    MAX_TITLE_LEN = 200
    MAX_URL_LEN = 2000
    links = links[:MAX_LINKS]
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM card_links WHERE pubkey = %s", (pubkey,))
            for i, link in enumerate(links):
                title = str(link.get("title", ""))[:MAX_TITLE_LEN]
                url = str(link.get("url", ""))[:MAX_URL_LEN]
                if not re.match(r'^https://', url, re.IGNORECASE):
                    continue
                cur.execute(
                    "INSERT INTO card_links (pubkey, title, url, sort_order) VALUES (%s, %s, %s, %s)",
                    (pubkey, title, url, i))
        pg.commit()
    except Exception as e:
        _logger.error(f"save_card_links failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Relay state (key-value store)
# ---------------------------------------------------------------------------

def get_relay_state(key):
    """Get a value from the relay_state key-value table."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT value FROM relay_state WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        _logger.error(f"get_relay_state failed: {e}")
        return None


def set_relay_state(key, value):
    """Set a value in the relay_state key-value table (upsert)."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO relay_state (key, value, updated_at) VALUES (%s, %s, NOW()) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
                (key, value))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_relay_state failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# First-seen cache
# ---------------------------------------------------------------------------

def get_first_seen(pubkey):
    """Get cached first-seen timestamp for a pubkey."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT first_seen FROM first_seen_cache WHERE pubkey = %s", (pubkey,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        _logger.error(f"get_first_seen failed: {e}")
        return None


def set_first_seen(pubkey, timestamp):
    """Cache first-seen timestamp for a pubkey. Only updates if new value is older."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO first_seen_cache (pubkey, first_seen, updated_at) VALUES (%s, %s, NOW()) "
                "ON CONFLICT (pubkey) DO UPDATE SET first_seen = LEAST(first_seen_cache.first_seen, EXCLUDED.first_seen), updated_at = NOW()",
                (pubkey, timestamp))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_first_seen failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# List event tracking
# ---------------------------------------------------------------------------

def get_list_event_id(pubkey):
    """Get the kind 9999 list event id for a directory member."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT list_event_id FROM directory_profiles WHERE hex_pubkey = %s", (pubkey,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        _logger.error(f"get_list_event_id failed: {e}")
        return None


def set_list_event_id(pubkey, event_id):
    """Set the kind 9999 list event id for a directory member."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE directory_profiles SET list_event_id = %s WHERE hex_pubkey = %s",
                (event_id, pubkey))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_list_event_id failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def set_self_signed(pubkey, value=True):
    """Mark a directory member's listing as self-signed (or not)."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE directory_profiles SET self_signed = %s WHERE hex_pubkey = %s",
                (value, pubkey))
        pg.commit()
    except Exception as e:
        _logger.error(f"set_self_signed failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Directory profiles (Postgres - pre-computed by background indexer)
# ---------------------------------------------------------------------------

def bulk_upsert_directory_profiles(rows):
    """Batch upsert directory profiles."""
    if not rows:
        return
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            for r in rows:
                cur.execute("SAVEPOINT prof_sp")
                try:
                    cur.execute("""
                        INSERT INTO directory_profiles
                            (hex_pubkey, npub, name, picture, nip05_display, about, lud16,
                             badges, event_count, last_active, trust_count,
                             subscription_created, directory_tags, card_url, indexed_at,
                             reputation_score, activity_score, identity_score, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s,
                                %s::jsonb, %s, %s, %s,
                                %s, %s, %s, NOW(),
                                %s, %s, %s, NOW())
                        ON CONFLICT (hex_pubkey) DO UPDATE SET
                            npub = EXCLUDED.npub,
                            name = EXCLUDED.name,
                            picture = EXCLUDED.picture,
                            nip05_display = EXCLUDED.nip05_display,
                            about = EXCLUDED.about,
                            lud16 = EXCLUDED.lud16,
                            badges = EXCLUDED.badges,
                            event_count = EXCLUDED.event_count,
                            last_active = EXCLUDED.last_active,
                            trust_count = EXCLUDED.trust_count,
                            subscription_created = EXCLUDED.subscription_created,
                            directory_tags = EXCLUDED.directory_tags,
                            card_url = EXCLUDED.card_url,
                            indexed_at = NOW(),
                            reputation_score = EXCLUDED.reputation_score,
                            activity_score = EXCLUDED.activity_score,
                            identity_score = EXCLUDED.identity_score,
                            updated_at = NOW()
                    """, (
                        r["pubkey"], r.get("npub", ""), r.get("name", ""), r.get("picture", ""),
                        r.get("nip05_display", ""), r.get("about", ""), r.get("lud16", ""),
                        r.get("badges", "[]"), r.get("event_count", 0), r.get("last_active", 0),
                        r.get("trust_count", 0), r.get("subscription_created", ""),
                        r.get("directory_tags", "[]"), r.get("card_url", ""),
                        r.get("reputation_score", 0),
                        r.get("activity_score", 0.0), r.get("identity_score", 0.0),
                    ))
                    cur.execute("RELEASE SAVEPOINT prof_sp")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT prof_sp")
        pg.commit()
    except Exception as e:
        _logger.error(f"bulk_upsert_directory_profiles failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def remove_stale_directory_profiles(active_pubkeys):
    """Delete profiles for pubkeys no longer in the directory."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            if not active_pubkeys:
                cur.execute("DELETE FROM directory_profiles")
            else:
                placeholders = ",".join(["%s"] * len(active_pubkeys))
                cur.execute(
                    f"DELETE FROM directory_profiles WHERE hex_pubkey NOT IN ({placeholders})",
                    list(active_pubkeys))
        pg.commit()
    except Exception as e:
        _logger.error(f"remove_stale_directory_profiles failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def _build_directory_query_parts(search=None, badge_filter=None, tag_filter=None, cluster_filter=None):
    """Build shared WHERE clauses and params for directory page queries.

    Returns (where_clauses: list[str], params: list).
    """
    where_clauses = []
    params = []

    if search:
        where_clauses.append("(dp.name ILIKE %s OR dp.nip05_display ILIKE %s OR dp.about ILIKE %s OR dp.directory_tags ILIKE %s)")
        escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        term = f"%{escaped}%"
        params.extend([term, term, term, term])

    if badge_filter:
        where_clauses.append("dp.badges::text LIKE %s")
        params.append(f'%"{badge_filter}"%')

    if tag_filter:
        tags = [t.strip() for t in tag_filter.split(",") if t.strip()]
        for t in tags:
            escaped_tag = t.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            where_clauses.append("dp.directory_tags ILIKE %s")
            params.append(f'%"{escaped_tag}"%')

    if cluster_filter is not None and cluster_filter >= 0:
        where_clauses.append("tc.cluster_id = %s")
        params.append(cluster_filter)

    return where_clauses, params


def _format_directory_rows(rows):
    """Convert raw directory_profiles rows to API-ready dicts (shared post-processing)."""
    members = []
    for r in rows:
        m = dict(r)
        m["badges"] = json.loads(m.get("badges", "[]"))
        m["tags"] = json.loads(m.get("directory_tags", "[]"))
        del m["directory_tags"]
        m["self_signed"] = bool(m.get("self_signed"))
        m["cluster_id"] = m.get("cluster_id")
        # Normalise optional trust fields when present
        if "trust_score" in m:
            m["trust_score"] = round(float(m["trust_score"] or 0), 4)
        if "trust_tier" in m:
            m["trust_tier"] = m["trust_tier"] or "unverified"
        members.append(m)
    return members


def get_directory_page(page=1, limit=24, sort="newest", badge_filter=None, search=None, tag_filter=None, cluster_filter=None):
    """Paginated directory query. Returns (members_list, total_count)."""
    pg = _get_pg()
    if not pg:
        return [], 0
    try:
        import psycopg2.extras
        from psycopg2 import sql as psql

        where_clauses, params = _build_directory_query_parts(search, badge_filter, tag_filter, cluster_filter)

        sort_map = {
            "newest": psql.SQL("dp.subscription_created DESC"),
            "active": psql.SQL("dp.last_active DESC"),
            "name": psql.SQL("dp.name ASC"),
        }
        order = sort_map.get(sort, psql.SQL("dp.subscription_created DESC"))

        from_clause = psql.SQL("directory_profiles dp LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey")
        where_clause = psql.SQL(" WHERE " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(psql.SQL("SELECT COUNT(*) as c FROM {}{}").format(from_clause, where_clause), params)
            total = cur.fetchone()["c"]

            offset = (page - 1) * limit
            cur.execute(
                psql.SQL(
                    "SELECT dp.hex_pubkey AS pubkey, dp.npub, dp.name, dp.picture, dp.nip05_display, dp.about, dp.lud16, "
                    "dp.badges::text AS badges, dp.event_count, dp.last_active, dp.trust_count, "
                    "dp.directory_tags, dp.card_url, dp.reputation_score, dp.self_signed, "
                    "tc.cluster_id "
                    "FROM {} {} ORDER BY {} LIMIT %s OFFSET %s"
                ).format(from_clause, where_clause, order),
                params + [limit, offset])
            rows = cur.fetchall()

        return _format_directory_rows(rows), total
    except Exception as e:
        _logger.error(f"get_directory_page failed: {e}")
        return [], 0


def get_directory_page_personalized(observer_pubkey, page=1, limit=24, sort="trust",
                                     badge_filter=None, search=None, tag_filter=None, max_hops=5, cluster_filter=None):
    """Personalized directory query: joins personalized_scores for observer's trust view.

    When sort='reputation', sorts by personalized score instead of global reputation_score.
    Adds trust_score, trust_tier, trust_hops fields to each member.
    """
    pg = _get_pg()
    if not pg:
        return [], 0
    try:
        import psycopg2.extras
        from psycopg2 import sql as psql

        where_clauses, params = _build_directory_query_parts(search, badge_filter, tag_filter, cluster_filter)

        sort_map = {
            "newest": psql.SQL("dp.subscription_created DESC"),
            "active": psql.SQL("dp.last_active DESC"),
            "name": psql.SQL("dp.name ASC"),
            "trust": psql.SQL("COALESCE(ps.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
            "top": psql.SQL("COALESCE(ps.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
        }
        order = sort_map.get(sort, psql.SQL("COALESCE(ps.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"))
        extra_where = psql.SQL(" AND " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                psql.SQL(
                    "SELECT COUNT(*) as c FROM directory_profiles dp "
                    "LEFT JOIN personalized_scores ps ON dp.hex_pubkey = ps.target_pubkey AND ps.observer_pubkey = %s "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{}"
                ).format(extra_where),
                [observer_pubkey] + params
            )
            total = cur.fetchone()["c"]

            offset = (page - 1) * limit
            cur.execute(
                psql.SQL(
                    "SELECT dp.hex_pubkey AS pubkey, dp.npub, dp.name, dp.picture, dp.nip05_display, dp.about, dp.lud16, "
                    "dp.badges::text AS badges, dp.event_count, dp.last_active, dp.trust_count, "
                    "dp.directory_tags, dp.card_url, dp.reputation_score, dp.self_signed, "
                    "ps.score AS trust_score, ps.tier AS trust_tier, ps.hops AS trust_hops, "
                    "tc.cluster_id "
                    "FROM directory_profiles dp "
                    "LEFT JOIN personalized_scores ps ON dp.hex_pubkey = ps.target_pubkey AND ps.observer_pubkey = %s "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{} "
                    "ORDER BY {} LIMIT %s OFFSET %s"
                ).format(extra_where, order),
                [observer_pubkey] + params + [limit, offset]
            )
            rows = cur.fetchall()

        members = _format_directory_rows(rows)
        # Add trust_hops (personalized-specific field)
        for m in members:
            m.setdefault("trust_hops", None)

        return members, total
    except Exception as e:
        _logger.error(f"get_directory_page_personalized failed: {e}")
        return [], 0


def get_directory_profile(pubkey):
    """Get a single directory profile by pubkey, or None."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT hex_pubkey AS pubkey, npub, name, picture, nip05_display, about, lud16, "
                "banner, website, badges::text AS badges, event_count, last_active, trust_count, "
                "subscription_created, directory_tags, card_url, reputation_score "
                "FROM directory_profiles WHERE hex_pubkey = %s", (pubkey,))
            row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        _logger.error(f"get_directory_profile failed: {e}")
        return None


def get_directory_profiles_batch(pubkeys):
    """Get directory profiles for multiple pubkeys in one query. Returns {pubkey: profile_dict}."""
    if not pubkeys:
        return {}
    pg = _get_pg()
    if not pg:
        return {}
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            ph = ",".join(["%s"] * len(pubkeys))
            cur.execute(
                f"SELECT hex_pubkey AS pubkey, npub, name, picture, nip05_display, about, lud16, "
                f"banner, website, badges::text AS badges, event_count, last_active, trust_count, "
                f"subscription_created, directory_tags, card_url, reputation_score "
                f"FROM directory_profiles WHERE hex_pubkey IN ({ph})", list(pubkeys))
            rows = cur.fetchall()
        return {row["pubkey"]: dict(row) for row in rows}
    except Exception as e:
        _logger.error(f"get_directory_profiles_batch failed: {e}")
        return {}


def get_directory_stats():
    """Aggregate stats for the directory overview."""
    week_ago = int(time.time()) - 604800
    pg = _get_pg()
    if not pg:
        return {"total_members": 0, "active_this_week": 0, "total_events": 0, "top_tags": []}
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT COUNT(*) as total_members,
                          SUM(CASE WHEN last_active > %s THEN 1 ELSE 0 END) as active_this_week,
                          SUM(event_count) as total_events
                   FROM directory_profiles""",
                (week_ago,))
            row = cur.fetchone()

            cur.execute("SELECT directory_tags FROM directory_profiles WHERE directory_tags != '[]'")
            tag_rows = cur.fetchall()

        tag_count = {}
        for r in tag_rows:
            try:
                for t in json.loads(r["directory_tags"]):
                    tag_count[t] = tag_count.get(t, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass
        top_tags = sorted(tag_count.items(), key=lambda x: x[1], reverse=True)[:10]

        return {
            "total_members": row["total_members"] or 0,
            "active_this_week": int(row["active_this_week"] or 0),
            "total_events": int(row["total_events"] or 0),
            "top_tags": [{"name": t[0], "count": t[1]} for t in top_tags],
        }
    except Exception as e:
        _logger.error(f"get_directory_stats failed: {e}")
        return {"total_members": 0, "active_this_week": 0, "total_events": 0, "top_tags": []}


def get_all_directory_tags():
    """Return all tags with member counts, sorted by frequency."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT directory_tags FROM directory_profiles WHERE directory_tags != '[]'")
            rows = cur.fetchall()
        tag_count = {}
        for r in rows:
            try:
                for t in json.loads(r[0]):
                    tag_count[t] = tag_count.get(t, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass
        return sorted(tag_count.items(), key=lambda x: x[1], reverse=True)
    except Exception as e:
        _logger.error(f"get_all_directory_tags failed: {e}")
        return []


def get_all_directory_members():
    """Get all directory members for recommendation scoring and attestation."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT hex_pubkey AS pubkey, npub, name, picture, nip05_display, card_url, "
                "reputation_score, badges::text AS badges, event_count, last_active, "
                "subscription_created "
                "FROM directory_profiles")
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_all_directory_members failed: {e}")
        return []


def get_directory_member(pubkey):
    """Get a single directory member by pubkey. Returns dict or None."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT hex_pubkey AS pubkey, npub, name, picture, nip05_display, card_url, "
                "reputation_score, badges::text AS badges, event_count, last_active, "
                "subscription_created "
                "FROM directory_profiles WHERE hex_pubkey = %s",
                (pubkey,))
            row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        _logger.error(f"get_directory_member failed: {e}")
        return None


def update_directory_visit(observer_pubkey):
    """Update last_directory_visit timestamp for an observer."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                """UPDATE directory_profiles SET last_directory_visit = NOW()
                   WHERE hex_pubkey = %s""",
                (observer_pubkey,)
            )
        pg.commit()
    except Exception:
        try:
            pg.rollback()
        except Exception:
            pass


def get_recently_active_observers(days=7):
    """Get observer pubkeys who visited the directory in the last N days."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute(
                """SELECT hex_pubkey FROM directory_profiles
                   WHERE last_directory_visit > NOW() - INTERVAL '%s days'
                   ORDER BY last_directory_visit DESC""",
                (days,)
            )
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Trust edges (Postgres)
# ---------------------------------------------------------------------------

def bulk_replace_trust_edges(edges):
    """Replace directory follow edges. edges = [(follower, followed), ...]

    Writes to the shared trust_edges table with edge_type='follow'.
    Only deletes/replaces edges where source is in the provided set.
    """
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            # Collect all source pubkeys
            sources = set(e[0] for e in edges)
            if sources:
                placeholders = ",".join(["%s"] * len(sources))
                cur.execute(
                    f"DELETE FROM trust_edges WHERE edge_type = 'follow' AND source_pubkey IN ({placeholders})",
                    list(sources))
            # Insert new follow edges
            for f, t in edges:
                cur.execute(
                    "INSERT INTO trust_edges (source_pubkey, target_pubkey, edge_type, weight) "
                    "VALUES (%s, %s, 'follow', 1.0) ON CONFLICT DO NOTHING",
                    (f, t))
        pg.commit()
    except Exception as e:
        _logger.error(f"bulk_replace_trust_edges failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_trust_edges():
    """Get all follow edges for graph visualization."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT source_pubkey, target_pubkey FROM trust_edges WHERE edge_type = 'follow'")
            return [(r[0], r[1]) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_trust_edges failed: {e}")
        return []


def get_trust_edges_for_sources(source_pubkeys):
    """Get follow edges only for specific source pubkeys. Returns [(follower, followed), ...]."""
    pg = _get_pg()
    if not pg or not source_pubkeys:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT source_pubkey, target_pubkey FROM trust_edges "
                "WHERE edge_type = 'follow' AND source_pubkey = ANY(%s)",
                (list(source_pubkeys),)
            )
            return [(r[0], r[1]) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_trust_edges_for_sources failed: {e}")
        return []


def get_trust_subgraph(pubkey, max_depth=2):
    """BFS from pubkey through follow edges up to max_depth.

    Returns (node_pubkeys_set, edges_list) where edges_list has
    (source, target) tuples.
    """
    max_depth = min(max_depth, 3)
    pg = _get_pg()
    if not pg:
        return set(), []
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT source_pubkey, target_pubkey FROM trust_edges WHERE edge_type = 'follow' LIMIT 50000")
            rows = cur.fetchall()
    except Exception as e:
        _logger.error(f"get_trust_subgraph failed: {e}")
        return set(), []

    outgoing = {}
    incoming = {}
    for r in rows:
        f, t = r[0], r[1]
        outgoing.setdefault(f, set()).add(t)
        incoming.setdefault(t, set()).add(f)

    visited = {pubkey}
    frontier = {pubkey}
    all_edges = []

    MAX_VISITED = 5000
    for _depth in range(max_depth):
        next_frontier = set()
        for node in frontier:
            if len(visited) >= MAX_VISITED:
                break
            for target in outgoing.get(node, set()):
                all_edges.append((node, target))
                if target not in visited:
                    visited.add(target)
                    next_frontier.add(target)
            for source in incoming.get(node, set()):
                all_edges.append((source, node))
                if source not in visited:
                    visited.add(source)
                    next_frontier.add(source)
        frontier = next_frontier
        if not frontier or len(visited) >= MAX_VISITED:
            break

    unique_edges = list(set(all_edges))
    return visited, unique_edges


def get_trust_path(source_pubkey, target_pubkey, max_depth=6):
    """Find shortest follow path from source to target. Returns list of pubkeys or []."""
    pg = _get_pg()
    if not pg:
        return []
    if source_pubkey == target_pubkey:
        return [source_pubkey]
    try:
        with pg.cursor() as cur:
            cur.execute("""
                WITH RECURSIVE path AS (
                    -- Base: source follows these targets (hop 1)
                    SELECT te.target_pubkey AS node,
                           ARRAY[%s, te.target_pubkey] AS trail,
                           1 AS depth
                    FROM trust_edges te
                    WHERE te.source_pubkey = %s AND te.edge_type = 'follow'
                    UNION ALL
                    -- Recursive: follow the follow chain
                    SELECT te.target_pubkey,
                           p.trail || te.target_pubkey,
                           p.depth + 1
                    FROM path p
                    JOIN trust_edges te ON te.source_pubkey = p.node AND te.edge_type = 'follow'
                    WHERE te.target_pubkey <> ALL(p.trail)
                      AND p.depth < %s
                )
                SELECT trail FROM path
                WHERE node = %s
                ORDER BY depth
                LIMIT 1
            """, (source_pubkey, source_pubkey, max_depth, target_pubkey))
            row = cur.fetchone()
            return row[0] if row else []
    except Exception as e:
        _logger.error(f"get_trust_path failed: {e}")
        return []


def get_trust_path_profiles(pubkeys):
    """Get profiles for a list of pubkeys (for trust path display)."""
    if not pubkeys:
        return []
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            ph = ",".join(["%s"] * len(pubkeys))
            cur.execute(
                f"SELECT hex_pubkey AS pubkey, name, picture, npub, card_url, reputation_score "
                f"FROM directory_profiles WHERE hex_pubkey IN ({ph})", pubkeys
            )
            profile_map = {row["pubkey"]: dict(row) for row in cur.fetchall()}
        # Return in path order, filling in unknowns
        return [profile_map.get(pk, {"pubkey": pk, "name": "", "picture": "", "npub": "", "card_url": "", "reputation_score": 0}) for pk in pubkeys]
    except Exception as e:
        _logger.error(f"get_trust_path_profiles failed: {e}")
        return []


def get_trust_lookup(observer_pubkey, target_pubkey):
    """Look up trust relationship between observer and target.

    Returns trust score/tier/hops, target profile, and shared follow connections.
    """
    pg = _get_pg()
    if not pg:
        return None
    try:
        import psycopg2.extras
        result = {}

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1. Get personalized score for this observer -> target
            if observer_pubkey:
                cur.execute(
                    "SELECT score, tier, hops, computed_at FROM personalized_scores "
                    "WHERE observer_pubkey = %s AND target_pubkey = %s",
                    (observer_pubkey, target_pubkey)
                )
                score_row = cur.fetchone()
                if score_row:
                    result["trust_score"] = round(float(score_row["score"]), 4)
                    result["trust_tier"] = score_row["tier"]
                    result["trust_hops"] = score_row["hops"]
                    result["computed_at"] = score_row["computed_at"].isoformat() if score_row["computed_at"] else None
                else:
                    result["trust_score"] = 0
                    result["trust_tier"] = "unknown"
                    result["trust_hops"] = None
                    result["computed_at"] = None
            else:
                result["trust_score"] = None
                result["trust_tier"] = None
                result["trust_hops"] = None
                result["computed_at"] = None

            # 2. Get target's directory profile (if they're a member)
            cur.execute(
                "SELECT hex_pubkey AS pubkey, npub, name, picture, nip05_display, about, lud16, "
                "badges::text AS badges, event_count, last_active, trust_count, "
                "card_url, reputation_score, activity_score, identity_score, self_signed "
                "FROM directory_profiles WHERE hex_pubkey = %s", (target_pubkey,)
            )
            profile_row = cur.fetchone()
            if profile_row:
                profile = dict(profile_row)
                profile["badges"] = json.loads(profile.get("badges", "[]"))
                profile["self_signed"] = bool(profile.get("self_signed"))
                result["profile"] = profile
                result["is_member"] = True
            else:
                result["profile"] = None
                result["is_member"] = False

            # 2b. Mutual follow check
            if observer_pubkey:
                cur.execute(
                    """SELECT
                       EXISTS(SELECT 1 FROM trust_edges WHERE source_pubkey=%s AND target_pubkey=%s AND edge_type='follow') AS observer_follows,
                       EXISTS(SELECT 1 FROM trust_edges WHERE source_pubkey=%s AND target_pubkey=%s AND edge_type='follow') AS target_follows""",
                    (observer_pubkey, target_pubkey, target_pubkey, observer_pubkey)
                )
                mf_row = cur.fetchone()
                result["observer_follows_target"] = bool(mf_row["observer_follows"])
                result["target_follows_observer"] = bool(mf_row["target_follows"])
                result["mutual_follow"] = bool(mf_row["observer_follows"] and mf_row["target_follows"])
            else:
                result["observer_follows_target"] = None
                result["target_follows_observer"] = None
                result["mutual_follow"] = None

            # 3-5: Shared connections (require observer)
            shared_follow_pks = []
            mutual_follower_pks = []
            if observer_pubkey:
                # 3. Shared follows: people that BOTH observer and target follow
                cur.execute(
                    """SELECT te1.target_pubkey
                       FROM trust_edges te1
                       JOIN trust_edges te2 ON te1.target_pubkey = te2.target_pubkey
                       WHERE te1.source_pubkey = %s AND te2.source_pubkey = %s
                         AND te1.edge_type = 'follow' AND te2.edge_type = 'follow'""",
                    (observer_pubkey, target_pubkey)
                )
                shared_follow_pks = [row["target_pubkey"] for row in cur.fetchall()]

                # 4. Who follows the target that the observer also follows
                cur.execute(
                    """SELECT te1.source_pubkey
                       FROM trust_edges te1
                       JOIN trust_edges te2 ON te1.source_pubkey = te2.target_pubkey
                       WHERE te1.target_pubkey = %s AND te1.edge_type = 'follow'
                         AND te2.source_pubkey = %s AND te2.edge_type = 'follow'""",
                    (target_pubkey, observer_pubkey)
                )
                mutual_follower_pks = [row["source_pubkey"] for row in cur.fetchall()]

            # 5. Resolve names for shared connections (both types combined, limit 10)
            all_connection_pks = list(set(shared_follow_pks + mutual_follower_pks))
            if all_connection_pks:
                ph = ",".join(["%s"] * len(all_connection_pks))
                cur.execute(
                    f"SELECT hex_pubkey, name, picture, card_url FROM directory_profiles "
                    f"WHERE hex_pubkey IN ({ph})", all_connection_pks
                )
                conn_profiles = {row["hex_pubkey"]: dict(row) for row in cur.fetchall()}
            else:
                conn_profiles = {}

        # Build shared connections response
        shared_follows = []
        for pk in shared_follow_pks[:5]:
            p = conn_profiles.get(pk, {})
            shared_follows.append({"pubkey": pk, "name": p.get("name", ""), "picture": p.get("picture", ""), "card_url": p.get("card_url", "")})

        mutual_followers = []
        for pk in mutual_follower_pks[:5]:
            p = conn_profiles.get(pk, {})
            mutual_followers.append({"pubkey": pk, "name": p.get("name", ""), "picture": p.get("picture", ""), "card_url": p.get("card_url", "")})

        result["shared_follows"] = shared_follows
        result["shared_follows_count"] = len(shared_follow_pks)
        result["mutual_followers"] = mutual_followers
        result["mutual_followers_count"] = len(mutual_follower_pks)

        # 6. Social stats for the target
        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE target_pubkey = %s AND edge_type = 'follow'",
                (target_pubkey,))
            result["followers_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE source_pubkey = %s AND edge_type = 'follow'",
                (target_pubkey,))
            result["following_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE target_pubkey = %s AND edge_type = 'mute'",
                (target_pubkey,))
            result["muted_by_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE target_pubkey = %s AND edge_type = 'report'",
                (target_pubkey,))
            result["reported_by_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE source_pubkey = %s AND edge_type = 'mute'",
                (target_pubkey,))
            result["muting_count"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM trust_edges WHERE source_pubkey = %s AND edge_type = 'report'",
                (target_pubkey,))
            result["reporting_count"] = cur.fetchone()[0]

            # Verified followers/following + tier breakdown (require observer)
            if observer_pubkey:
                cur.execute(
                    """SELECT COUNT(*) FROM trust_edges te
                       JOIN personalized_scores ps ON te.source_pubkey = ps.target_pubkey
                       WHERE te.target_pubkey = %s AND te.edge_type = 'follow'
                         AND ps.observer_pubkey = %s AND ps.score >= 0.02""",
                    (target_pubkey, observer_pubkey))
                result["verified_followers"] = cur.fetchone()[0]

                cur.execute(
                    """SELECT COUNT(*) FROM trust_edges te
                       JOIN personalized_scores ps ON te.target_pubkey = ps.target_pubkey
                       WHERE te.source_pubkey = %s AND te.edge_type = 'follow'
                         AND ps.observer_pubkey = %s AND ps.score >= 0.02""",
                    (target_pubkey, observer_pubkey))
                result["verified_following"] = cur.fetchone()[0]

                # Follower audience quality - tier breakdown
                cur.execute(
                    """SELECT ps.tier, COUNT(*) FROM trust_edges te
                       JOIN personalized_scores ps ON te.source_pubkey = ps.target_pubkey
                       WHERE te.target_pubkey = %s AND te.edge_type = 'follow'
                         AND ps.observer_pubkey = %s
                       GROUP BY ps.tier""",
                    (target_pubkey, observer_pubkey))
                tier_counts = {"highly_trusted": 0, "trusted": 0, "neutral": 0, "low_trust": 0, "unverified": 0}
                for row in cur.fetchall():
                    if row[0] in tier_counts:
                        tier_counts[row[0]] = row[1]
                result["follower_tiers"] = tier_counts

                # Following quality - tier breakdown
                cur.execute(
                    """SELECT ps.tier, COUNT(*) FROM trust_edges te
                       JOIN personalized_scores ps ON te.target_pubkey = ps.target_pubkey
                       WHERE te.source_pubkey = %s AND te.edge_type = 'follow'
                         AND ps.observer_pubkey = %s
                       GROUP BY ps.tier""",
                    (target_pubkey, observer_pubkey))
                ftier_counts = {"highly_trusted": 0, "trusted": 0, "neutral": 0, "low_trust": 0, "unverified": 0}
                for row in cur.fetchall():
                    if row[0] in ftier_counts:
                        ftier_counts[row[0]] = row[1]
                result["following_tiers"] = ftier_counts
            else:
                result["verified_followers"] = 0
                result["verified_following"] = 0
                result["follower_tiers"] = None
                result["following_tiers"] = None

        return result
    except Exception as e:
        _logger.error(f"get_trust_lookup failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Cluster management
# ---------------------------------------------------------------------------

def save_cluster_results(assignments, meta):
    """Save cluster assignments and metadata. Preserves manual label overrides."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            # Read existing overrides before truncating
            cur.execute("SELECT cluster_id, override_label FROM trust_cluster_meta WHERE override_label IS NOT NULL")
            overrides = {r[0]: r[1] for r in cur.fetchall()}

            cur.execute("TRUNCATE trust_clusters, trust_cluster_meta")
            if assignments:
                from psycopg2.extras import execute_values
                rows = [(pk, cid) for pk, cid in assignments.items() if cid >= 0]
                if rows:
                    execute_values(cur, "INSERT INTO trust_clusters (pubkey, cluster_id) VALUES %s", rows)
            if meta:
                from psycopg2.extras import execute_values
                meta_rows = []
                for cid, m in meta.items():
                    ovr = overrides.get(cid)
                    label = ovr if ovr else m["label"]
                    meta_rows.append((cid, label, m["color"], m["member_count"], ovr))
                if meta_rows:
                    execute_values(cur, "INSERT INTO trust_cluster_meta (cluster_id, label, color, member_count, override_label) VALUES %s", meta_rows)
        pg.commit()
    except Exception as e:
        _logger.error(f"save_cluster_results failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_cluster_meta():
    """Get cluster metadata (label, color, count)."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT cluster_id, label, color, member_count FROM trust_cluster_meta ORDER BY cluster_id")
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_cluster_meta failed: {e}")
        return []


def get_cluster_assignments():
    """Get cluster assignments as {pubkey: cluster_id}."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT pubkey, cluster_id FROM trust_clusters")
            return {r[0]: r[1] for r in cur.fetchall()}
    except Exception as e:
        _logger.error(f"get_cluster_assignments failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Personalized scores (GrapeRank output)
# ---------------------------------------------------------------------------

def _score_to_tier(score):
    if score >= 0.50:
        return "highly_trusted"
    elif score >= 0.20:
        return "trusted"
    elif score >= 0.07:
        return "neutral"
    elif score >= 0.02:
        return "low_trust"
    return "unverified"


_graperank_job_counter = 0


def trigger_graperank_computation(observer_pubkey):
    """Trigger on-demand GrapeRank computation for an observer via Redis queue.

    Pushes job to message_queue, waits up to 120s for result, stores in personalized_scores.
    Returns number of scores stored, or -1 on failure.
    """
    global _graperank_job_counter

    r = _get_redis()
    pg = _get_pg()
    if not r or not pg:
        return -1

    _graperank_job_counter += 1
    job_id = _graperank_job_counter + 900000  # offset to avoid collision with indexer jobs

    result_key = f"graperank:result:{job_id}"

    # Push job
    message = json.dumps({"private_id": job_id, "parameters": observer_pubkey})
    r.rpush("message_queue", _sign_queue_message(message))
    _logger.info(f"GrapeRank on-demand: triggered job {job_id} for {observer_pubkey[:16]}...")

    # Wait for result on the per-job key
    timeout = 120
    result = r.blpop(result_key, timeout=timeout)
    if result is None:
        _logger.error(f"GrapeRank on-demand: timeout for job {job_id}")
        return -1

    try:
        data = json.loads(result[1])
        gr = data.get("result", {})
        if not gr.get("success"):
            _logger.warning(f"GrapeRank on-demand: job {job_id} failed")
            return -1
        scorecards = gr.get("scorecards", {})
        _logger.info(f"GrapeRank on-demand: job {job_id} got {len(scorecards)} scorecards")

        # Only store scores for directory members (not all reachable nodes)
        member_pubkeys = set()
        try:
            raw = r.smembers("directory:members")
            if raw:
                member_pubkeys = {m.decode() if isinstance(m, bytes) else m for m in raw}
        except Exception as e:
            _logger.warning(f"GrapeRank on-demand: could not read directory_members: {e}")

        import psycopg2.extras
        values = []
        for pk, sc in scorecards.items():
            if pk == observer_pubkey:
                continue
            if member_pubkeys and pk not in member_pubkeys:
                continue
            influence = sc.get("influence", 0.0)
            hops = int(sc.get("hops", 999))
            tier = _score_to_tier(influence)
            values.append((observer_pubkey, pk, influence, tier, hops))

        if values:
            try:
                with pg.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO personalized_scores
                           (observer_pubkey, target_pubkey, score, tier, hops, computed_at)
                           VALUES %s
                           ON CONFLICT (observer_pubkey, target_pubkey) DO UPDATE SET
                               score = EXCLUDED.score, tier = EXCLUDED.tier,
                               hops = EXCLUDED.hops, computed_at = NOW()""",
                        values,
                        template="(%s, %s, %s, %s, %s, NOW())"
                    )
                pg.commit()
                _logger.info(f"GrapeRank on-demand: stored {len(values)} scores for {observer_pubkey[:16]}")
            except Exception as e:
                _logger.error(f"GrapeRank on-demand: DB write failed: {e}")
                try:
                    pg.rollback()
                except Exception:
                    pass
                return -1
        return len(values)
    except (ValueError, TypeError, KeyError) as e:
        _logger.error(f"GrapeRank on-demand: parse error: {e}")
        return -1


def has_personalized_scores(observer_pubkey):
    """Check if observer has any personalized scores computed."""
    pg = _get_pg()
    if not pg:
        return False
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM personalized_scores WHERE observer_pubkey = %s LIMIT 1",
                (observer_pubkey,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def get_personalized_scores(observer_pubkey):
    """Get all personalized scores for an observer. Returns {target_pubkey: score}."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT target_pubkey, score FROM personalized_scores WHERE observer_pubkey = %s",
                (observer_pubkey,)
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception as e:
        _logger.error(f"get_personalized_scores failed: {e}")
        return {}


def get_personalized_scores_batch(observer_pubkeys):
    """Get personalized scores for multiple observers in one query.

    Returns {observer_pubkey: {target_pubkey: score}}.
    """
    pg = _get_pg()
    if not pg or not observer_pubkeys:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT observer_pubkey, target_pubkey, score FROM personalized_scores "
                "WHERE observer_pubkey = ANY(%s)",
                (list(observer_pubkeys),)
            )
            result = {}
            for obs, tgt, score in cur.fetchall():
                if obs not in result:
                    result[obs] = {}
                result[obs][tgt] = score
            return result
    except Exception as e:
        _logger.error(f"get_personalized_scores_batch failed: {e}")
        return {}


def get_personalized_scores_with_hops(observer_pubkey):
    """Get all personalized scores+hops for an observer. Returns {target_pubkey: (score, hops)}."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT target_pubkey, score, hops FROM personalized_scores WHERE observer_pubkey = %s",
                (observer_pubkey,)
            )
            return {row[0]: (row[1], row[2]) for row in cur.fetchall()}
    except Exception as e:
        _logger.error(f"get_personalized_scores_with_hops failed: {e}")
        return {}


def get_scores_freshness(observer_pubkey):
    """Get the most recent computed_at for an observer's scores. Returns datetime or None."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT MAX(computed_at) FROM personalized_scores WHERE observer_pubkey = %s",
                (observer_pubkey,)
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Global consensus scores
# ---------------------------------------------------------------------------

def compute_and_save_global_consensus():
    """Compute global consensus scores: weighted average of all member personalized scores.

    Each member's opinion is weighted by their own global score (recursive -
    bootstrap from equal weight on first run).
    Stores one row per target in global_consensus_scores.
    Also caches in Redis for fast lookups.
    """
    pg = _get_pg()
    if not pg:
        return 0
    try:
        import psycopg2.extras
        # Get all member pubkeys (observers)
        with pg.cursor() as cur:
            cur.execute("SELECT hex_pubkey FROM directory_profiles")
            member_pks = {row[0] for row in cur.fetchall()}

        if not member_pks:
            return 0

        # Load all personalized scores for member observers
        all_scores = {}
        with pg.cursor() as cur:
            cur.execute(
                "SELECT observer_pubkey, target_pubkey, score FROM personalized_scores "
                "WHERE observer_pubkey = ANY(%s)",
                (list(member_pks),)
            )
            for obs, tgt, score in cur.fetchall():
                if obs not in all_scores:
                    all_scores[obs] = {}
                all_scores[obs][tgt] = score

        if not all_scores:
            return 0

        # Load existing global scores for observer weighting (bootstrap: equal weight if none)
        observer_weights = {}
        with pg.cursor() as cur:
            cur.execute("SELECT target_pubkey, score FROM global_consensus_scores")
            existing_global = {row[0]: row[1] for row in cur.fetchall()}

        for obs in all_scores:
            observer_weights[obs] = existing_global.get(obs, 1.0 / len(all_scores))

        # Compute weighted average per target
        target_numerator = {}
        target_denominator = {}
        for observer, scores in all_scores.items():
            w = max(observer_weights[observer], 0.001)
            for target, score in scores.items():
                target_numerator[target] = target_numerator.get(target, 0.0) + score * w
                target_denominator[target] = target_denominator.get(target, 0.0) + w

        consensus = {}
        for target in target_numerator:
            if target_denominator[target] > 0:
                consensus[target] = target_numerator[target] / target_denominator[target]

        if not consensus:
            return 0

        # Write to Postgres atomically
        with get_pg_conn() as gcs_conn:
            gcs_conn.autocommit = True
            with gcs_conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("""
                    CREATE TEMP TABLE _gcs_new (
                        target_pubkey TEXT PRIMARY KEY,
                        score REAL NOT NULL,
                        tier TEXT NOT NULL,
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    ) ON COMMIT DROP
                """)
                values = [
                    (pk, score, _score_to_tier(score))
                    for pk, score in consensus.items()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO _gcs_new (target_pubkey, score, tier, computed_at) VALUES %s",
                    values,
                    template="(%s, %s, %s, NOW())"
                )
                cur.execute("TRUNCATE global_consensus_scores")
                cur.execute("INSERT INTO global_consensus_scores SELECT * FROM _gcs_new")
                cur.execute("COMMIT")

        # Cache in Redis
        r = _get_redis()
        if r:
            try:
                pipe = r.pipeline()
                pipe.delete("directory:global_scores")
                score_map = {pk: str(round(s, 6)) for pk, s in consensus.items()}
                if score_map:
                    pipe.hset("directory:global_scores", mapping=score_map)
                pipe.execute()
            except Exception as e:
                _logger.error(f"Redis global_scores cache failed: {e}")

        _logger.info(f"Global consensus: computed {len(consensus)} scores from {len(all_scores)} observers")
        return len(consensus)
    except Exception as e:
        _logger.error(f"compute_and_save_global_consensus failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
        return 0


def get_global_consensus_scores():
    """Get global consensus scores. Returns {target_pubkey: score}.

    Tries Redis cache first, falls back to Postgres.
    """
    r = _get_redis()
    if r:
        try:
            scores = r.hgetall("directory:global_scores")
            if scores:
                return {pk: float(s) for pk, s in scores.items()}
        except Exception:
            pass
    # Fallback to Postgres
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT target_pubkey, score FROM global_consensus_scores")
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}


def get_public_graperank_scores():
    """Get public GrapeRank scores, preferring house scores with global fallback."""
    pg = _get_pg()
    if not pg:
        return get_global_consensus_scores()
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(hgs.target_pubkey, gs.target_pubkey) AS target_pubkey,
                       COALESCE(hgs.score, gs.score) AS score
                FROM house_graperank_scores hgs
                FULL OUTER JOIN global_consensus_scores gs
                  ON hgs.target_pubkey = gs.target_pubkey
            """)
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        try:
            pg.rollback()
        except Exception:
            pass
        return get_global_consensus_scores()


def get_directory_page_global(page=1, limit=24, sort="trust", badge_filter=None,
                               search=None, tag_filter=None, cluster_filter=None):
    """Directory listing with public house scores and global-consensus fallback.

    The house score is a fixed public point of view. Global consensus remains
    the fallback while the house refresh is warming up or missing a member.
    """
    pg = _get_pg()
    if not pg:
        return [], 0
    try:
        import psycopg2.extras
        from psycopg2 import sql as psql

        where_clauses, params = _build_directory_query_parts(search, badge_filter, tag_filter, cluster_filter)

        sort_map = {
            "newest": psql.SQL("dp.subscription_created DESC"),
            "active": psql.SQL("dp.last_active DESC"),
            "name": psql.SQL("dp.name ASC"),
            "trust": psql.SQL("COALESCE(hgs.score, gs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
            "top": psql.SQL("COALESCE(hgs.score, gs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
        }
        order = sort_map.get(sort, psql.SQL("COALESCE(hgs.score, gs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"))
        extra_where = psql.SQL(" AND " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                psql.SQL(
                    "SELECT COUNT(*) as c FROM directory_profiles dp "
                    "LEFT JOIN house_graperank_scores hgs ON dp.hex_pubkey = hgs.target_pubkey "
                    "LEFT JOIN global_consensus_scores gs ON dp.hex_pubkey = gs.target_pubkey "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{}"
                ).format(extra_where),
                params
            )
            total = cur.fetchone()["c"]

            offset = (page - 1) * limit
            cur.execute(
                psql.SQL(
                    "SELECT dp.hex_pubkey AS pubkey, dp.npub, dp.name, dp.picture, dp.nip05_display, dp.about, dp.lud16, "
                    "dp.badges::text AS badges, dp.event_count, dp.last_active, dp.trust_count, "
                    "dp.directory_tags, dp.card_url, dp.reputation_score, dp.self_signed, "
                    "COALESCE(hgs.score, gs.score) AS trust_score, "
                    "COALESCE(hgs.tier, gs.tier) AS trust_tier, "
                    "hgs.hops AS trust_hops, "
                    "CASE WHEN hgs.target_pubkey IS NOT NULL THEN 'house_graperank' "
                    "     WHEN gs.target_pubkey IS NOT NULL THEN 'global_consensus' "
                    "     ELSE 'none' END AS trust_score_source, "
                    "tc.cluster_id "
                    "FROM directory_profiles dp "
                    "LEFT JOIN house_graperank_scores hgs ON dp.hex_pubkey = hgs.target_pubkey "
                    "LEFT JOIN global_consensus_scores gs ON dp.hex_pubkey = gs.target_pubkey "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{} "
                    "ORDER BY {} LIMIT %s OFFSET %s"
                ).format(extra_where, order),
                params + [limit, offset]
            )
            rows = cur.fetchall()

        return _format_directory_rows(rows), total
    except Exception as e:
        _logger.error(f"get_directory_page_global failed: {e}")
        return [], 0


def approximate_visitor_scores(visitor_follows, member_followers_map, precomputed_scores, global_scores):
    """Fast approximate personalization without full GrapeRank.

    Args:
        visitor_follows: set of pubkeys the visitor follows
        member_followers_map: {member_pubkey: set of their followers}
        precomputed_scores: {observer_pubkey: {target_pubkey: score}} - member personalized scores
        global_scores: {target_pubkey: score} - global consensus

    Returns: {target_pubkey: score}
    """
    scores = {}
    for member_pk, member_follower_set in member_followers_map.items():
        # Direct follow = highest signal
        if member_pk in visitor_follows:
            scores[member_pk] = 0.9
            continue

        # Which of visitor's follows also follow this member?
        mutual_followers = visitor_follows & member_follower_set
        if not mutual_followers:
            continue

        total_weight = 0.0
        weighted_score = 0.0
        for follower in mutual_followers:
            if follower in precomputed_scores:
                score = precomputed_scores[follower].get(member_pk, 0.0)
                weight = 1.0
            else:
                score = global_scores.get(member_pk, 0.0)
                weight = 0.5
            weighted_score += score * weight
            total_weight += weight

        if total_weight > 0:
            scores[member_pk] = weighted_score / total_weight

    return scores


def get_directory_page_visitor(visitor_scores, page=1, limit=24, sort="trust",
                                badge_filter=None, search=None, tag_filter=None, cluster_filter=None):
    """Directory listing with approximate visitor scores.

    Loads visitor_scores into a temp table, then JOINs + sorts + paginates in SQL.
    """
    pg = _get_pg()
    if not pg:
        return [], 0
    try:
        import psycopg2.extras
        where_clauses = []
        params = []

        if search:
            where_clauses.append("(dp.name ILIKE %s OR dp.nip05_display ILIKE %s OR dp.about ILIKE %s OR dp.directory_tags ILIKE %s)")
            escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            term = f"%{escaped}%"
            params.extend([term, term, term, term])

        if badge_filter:
            where_clauses.append("dp.badges::text LIKE %s")
            params.append(f'%"{badge_filter}"%')

        if tag_filter:
            tags = [t.strip() for t in tag_filter.split(",") if t.strip()]
            for t in tags:
                escaped_tag = t.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                where_clauses.append("dp.directory_tags ILIKE %s")
                params.append(f'%"{escaped_tag}"%')

        if cluster_filter is not None and cluster_filter >= 0:
            where_clauses.append("tc.cluster_id = %s")
            params.append(cluster_filter)

        from psycopg2 import sql as psql
        sort_map = {
            "newest": psql.SQL("dp.subscription_created DESC"),
            "active": psql.SQL("dp.last_active DESC"),
            "name": psql.SQL("dp.name ASC"),
            "trust": psql.SQL("COALESCE(vs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
            "top": psql.SQL("COALESCE(vs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"),
        }
        order = sort_map.get(sort, psql.SQL("COALESCE(vs.score, 0) DESC, COALESCE(dp.reputation_score, 0) DESC, dp.last_active DESC"))
        extra_where = psql.SQL(" AND " + " AND ".join(where_clauses)) if where_clauses else psql.SQL("")

        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Load visitor scores into a temp table for SQL-side JOIN + sort
            cur.execute("""
                CREATE TEMP TABLE _visitor_scores (
                    pubkey TEXT PRIMARY KEY,
                    score REAL NOT NULL
                ) ON COMMIT DROP
            """)
            if visitor_scores:
                score_values = [(pk, s) for pk, s in visitor_scores.items()]
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO _visitor_scores (pubkey, score) VALUES %s",
                    score_values,
                    template="(%s, %s)"
                )

            cur.execute(
                psql.SQL(
                    "SELECT COUNT(*) as c FROM directory_profiles dp "
                    "LEFT JOIN _visitor_scores vs ON dp.hex_pubkey = vs.pubkey "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{}"
                ).format(extra_where),
                params
            )
            total = cur.fetchone()["c"]

            offset = (page - 1) * limit
            cur.execute(
                psql.SQL(
                    "SELECT dp.hex_pubkey AS pubkey, dp.npub, dp.name, dp.picture, dp.nip05_display, dp.about, dp.lud16, "
                    "dp.badges::text AS badges, dp.event_count, dp.last_active, dp.trust_count, "
                    "dp.directory_tags, dp.card_url, dp.reputation_score, dp.self_signed, "
                    "vs.score AS trust_score, "
                    "tc.cluster_id "
                    "FROM directory_profiles dp "
                    "LEFT JOIN _visitor_scores vs ON dp.hex_pubkey = vs.pubkey "
                    "LEFT JOIN trust_clusters tc ON dp.hex_pubkey = tc.pubkey "
                    "WHERE 1=1{} "
                    "ORDER BY {} LIMIT %s OFFSET %s"
                ).format(extra_where, order),
                params + [limit, offset]
            )
            rows = cur.fetchall()

        # Rollback to drop the temp table (no real changes to commit)
        try:
            pg.rollback()
        except Exception:
            pass

        members = []
        for r in rows:
            m = dict(r)
            m["badges"] = json.loads(m.get("badges", "[]"))
            m["tags"] = json.loads(m.get("directory_tags", "[]"))
            del m["directory_tags"]
            m["trust_score"] = round(float(m.get("trust_score") or 0), 4)
            m["trust_tier"] = _score_to_tier(float(m.get("trust_score") or 0))
            m["self_signed"] = bool(m.get("self_signed"))
            m["cluster_id"] = m.get("cluster_id")
            members.append(m)

        return members, total
    except Exception as e:
        _logger.error(f"get_directory_page_visitor failed: {e}")
        return [], 0


# ---------------------------------------------------------------------------
# Activity heatmap (Postgres)
# ---------------------------------------------------------------------------

def bulk_upsert_activity_heatmap(rows):
    """Replace heatmap data for given pubkeys. rows = [{"pubkey", "day", "event_count"}, ...]"""
    if not rows:
        return
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            for r in rows:
                cur.execute(
                    "INSERT INTO activity_heatmap (pubkey, day, event_count) VALUES (%s, %s, %s) "
                    "ON CONFLICT (pubkey, day) DO UPDATE SET event_count = EXCLUDED.event_count",
                    (r["pubkey"], r["day"], r["event_count"]))
        pg.commit()
    except Exception as e:
        _logger.error(f"bulk_upsert_activity_heatmap failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def cleanup_old_heatmap(max_days=400):
    """Remove heatmap entries older than max_days."""
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - max_days * 86400))
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM activity_heatmap WHERE day < %s", (cutoff,))
        pg.commit()
    except Exception as e:
        _logger.error(f"cleanup_old_heatmap failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_activity_heatmap(pubkey):
    """Get daily activity counts for a pubkey over the last 365 days."""
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 365 * 86400))
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT day, event_count FROM activity_heatmap WHERE pubkey = %s AND day >= %s ORDER BY day",
                (pubkey, cutoff))
            return [{"date": r[0], "count": r[1]} for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_activity_heatmap failed: {e}")
        return []


def get_all_heatmap_data(pubkeys, days=90):
    """Get heatmap data for multiple pubkeys at once. Returns {pubkey: {day: count}}."""
    if not pubkeys:
        return {}
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - days * 86400))
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            placeholders = ",".join(["%s"] * len(pubkeys))
            cur.execute(
                f"SELECT pubkey, day, event_count FROM activity_heatmap "
                f"WHERE pubkey IN ({placeholders}) AND day >= %s",
                pubkeys + [cutoff])
            rows = cur.fetchall()
        result = {}
        for r in rows:
            pk = r[0]
            if pk not in result:
                result[pk] = {}
            result[pk][r[1]] = r[2]
        return result
    except Exception as e:
        _logger.error(f"get_all_heatmap_data failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Zap tracking (Postgres)
# ---------------------------------------------------------------------------

def bulk_upsert_zaps(rows):
    """Insert zap records, ignoring duplicates. rows = [{"event_id", "sender", "receiver", "amount_msats", "created_at"}, ...]"""
    if not rows:
        return
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            for r in rows:
                cur.execute(
                    "INSERT INTO directory_zaps (event_id, sender, receiver, amount_msats, created_at) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (r["event_id"], r["sender"], r["receiver"], r["amount_msats"], r["created_at"]))
        pg.commit()
    except Exception as e:
        _logger.error(f"bulk_upsert_zaps failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def cleanup_old_zaps(max_age_seconds=7776000):
    """Remove zaps older than max_age_seconds (default 90 days)."""
    cutoff = int(time.time()) - max_age_seconds
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM directory_zaps WHERE created_at < %s", (cutoff,))
        pg.commit()
    except Exception as e:
        _logger.error(f"cleanup_old_zaps failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_zap_edges(period_seconds=7776000):
    """Get raw zap edges. Returns [(sender, receiver, amount_msats, created_at), ...]"""
    cutoff = int(time.time()) - period_seconds
    pg = _get_pg()
    if not pg:
        return []
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT sender, receiver, amount_msats, created_at FROM directory_zaps WHERE created_at > %s",
                (cutoff,))
            return [(r[0], r[1], r[2], r[3]) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_zap_edges failed: {e}")
        return []


def get_zap_flows(limit=50):
    """Get top zap flows between directory members. Returns list of {sender, receiver, total_msats, count}."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT z.sender, z.receiver,
                       SUM(z.amount_msats) AS total_msats,
                       COUNT(*) AS zap_count,
                       sp.name AS sender_name, sp.picture AS sender_picture,
                       rp.name AS receiver_name, rp.picture AS receiver_picture
                FROM directory_zaps z
                JOIN directory_profiles sp ON sp.hex_pubkey = z.sender
                JOIN directory_profiles rp ON rp.hex_pubkey = z.receiver
                GROUP BY z.sender, z.receiver, sp.name, sp.picture, rp.name, rp.picture
                ORDER BY total_msats DESC
                LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        _logger.error(f"get_zap_flows failed: {e}")
        return []


def get_zap_summary():
    """Get aggregate zap stats for the community."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total_zaps,
                       COALESCE(SUM(amount_msats), 0) AS total_msats,
                       COUNT(DISTINCT sender) AS unique_senders,
                       COUNT(DISTINCT receiver) AS unique_receivers
                FROM directory_zaps
            """)
            row = cur.fetchone()
            return {
                "total_zaps": row[0],
                "total_sats": row[1] // 1000,
                "unique_senders": row[2],
                "unique_receivers": row[3],
            }
    except Exception as e:
        _logger.error(f"get_zap_summary failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Community trust stats
# ---------------------------------------------------------------------------

def get_community_trust_stats():
    """Get community-wide trust statistics."""
    pg = _get_pg()
    if not pg:
        return {}
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM directory_profiles")
            total_members = cur.fetchone()[0]

            cur.execute("SELECT AVG(reputation_score), MAX(reputation_score) FROM directory_profiles")
            row = cur.fetchone()
            avg_rep = round(float(row[0] or 0), 1)
            max_rep = row[1] or 0

            cur.execute("SELECT COUNT(*) FROM trust_edges WHERE edge_type = 'follow'")
            total_follows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM trust_edges WHERE edge_type = 'mute'")
            total_mutes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM trust_edges WHERE edge_type = 'report'")
            total_reports = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM directory_profiles dp
                WHERE NOT EXISTS (
                    SELECT 1 FROM trust_edges te
                    WHERE te.target_pubkey = dp.hex_pubkey AND te.edge_type = 'mute'
                )
            """)
            clean_members = cur.fetchone()[0]

            cur.execute("SELECT AVG(hops) FROM personalized_scores WHERE hops IS NOT NULL AND hops > 0")
            avg_hops = round(float(cur.fetchone()[0] or 0), 1)

            cur.execute("""
                SELECT tier, COUNT(*) FROM personalized_scores
                GROUP BY tier ORDER BY COUNT(*) DESC
            """)
            tier_dist = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute("""
                SELECT COUNT(*) FROM directory_profiles
                WHERE badges::text LIKE '%%nip05-live%%'
            """)
            nip05_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM directory_profiles
                WHERE badges::text LIKE '%%lightning-reachable%%'
            """)
            lightning_count = cur.fetchone()[0]

            return {
                "total_members": total_members,
                "avg_reputation": avg_rep,
                "max_reputation": max_rep,
                "total_follow_edges": total_follows,
                "total_mute_edges": total_mutes,
                "total_report_edges": total_reports,
                "clean_members": clean_members,
                "clean_pct": round(clean_members / total_members * 100, 1) if total_members > 0 else 0,
                "avg_hops": avg_hops,
                "tier_distribution": tier_dist,
                "nip05_verified": nip05_count,
                "lightning_reachable": lightning_count,
            }
    except Exception as e:
        _logger.error(f"get_community_trust_stats failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Trust history snapshots
# ---------------------------------------------------------------------------

def save_trust_snapshots(rows):
    """Save daily trust snapshots. rows = [{pubkey, snapshot_date, reputation_score, trust_score, followers_count}]."""
    if not rows:
        return
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO trust_snapshots (pubkey, snapshot_date, reputation_score, trust_score, followers_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (pubkey, snapshot_date) DO UPDATE SET
                        reputation_score = EXCLUDED.reputation_score,
                        trust_score = EXCLUDED.trust_score,
                        followers_count = EXCLUDED.followers_count
                """, (r["pubkey"], r["snapshot_date"], r["reputation_score"],
                      r.get("trust_score", 0), r.get("followers_count", 0)))
        pg.commit()
    except Exception as e:
        _logger.error(f"save_trust_snapshots failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass


def get_trust_history(pubkey, days=90):
    """Get trust history for a pubkey over the last N days."""
    pg = _get_pg()
    if not pg:
        return []
    try:
        import psycopg2.extras
        with pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT snapshot_date, reputation_score, trust_score, followers_count
                FROM trust_snapshots
                WHERE pubkey = %s
                ORDER BY snapshot_date DESC
                LIMIT %s
            """, (pubkey, days))
            return [dict(r) for r in cur.fetchall()][::-1]  # oldest first
    except Exception as e:
        _logger.error(f"get_trust_history failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Digest tracking
# ---------------------------------------------------------------------------

def get_digest_last_date():
    """Get the date of the last published digest."""
    pg = _get_pg()
    if not pg:
        return None
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT digest_date FROM digest_history ORDER BY digest_date DESC LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        _logger.error(f"get_digest_last_date failed: {e}")
        return None


def save_digest(digest_date, event_id):
    """Record a published digest."""
    pg = _get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO digest_history (digest_date, event_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (digest_date, event_id))
        pg.commit()
    except Exception as e:
        _logger.error(f"save_digest failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
