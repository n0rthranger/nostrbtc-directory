# Nostr Directory — Personalized Web of Trust
#
# Trust computation powered by GrapeRank
# Copyright (c) Pretty-Good-Freedom-Tech / NosFabrica
# Algorithm design: David Strayhorn (straycat)
#
# GrapeRank source:
#   https://github.com/NosFabrica/brainstorm_graperank_algorithm
#   https://github.com/Pretty-Good-Freedom-Tech/graperank-nodejs
#
# Licensed under AGPL-3.0

"""Event-driven directory indexer: tails strfry for real-time profile and trust updates.

Connects to strfry WebSocket, processes incoming events:
- kind 0: upsert profile into directory_profiles (Postgres)
- kind 1: increment note_count, update last_active
- kind 3: update trust_edges (Postgres) + sync follow graph to Neo4j
- kind 6/7: track interactions for trust graph
- kind 9735: track zaps for trust graph
- kind 10000: sync mute edges to Neo4j
- kind 1984: sync report edges to Neo4j

Trust computation:
- Neo4j stores the full social graph (follows, mutes, reports)
- Java GrapeRank service reads from Neo4j, computes per-observer scores
- This indexer triggers GrapeRank via Redis message queue
- Results are read from Redis, stored in personalized_scores (Postgres)
- NIP-85 kind 30382 events published per observer

Schedule:
- Continuous: tail strfry, sync edges to Neo4j as they arrive
- Every 6 hours: per-observer GrapeRank for all paid members
- Every 6 hours: public house GrapeRank scores imported for anonymous ranking
- Sunday 3am: full graph rebuild + recomputation
- NIP-05 and Lightning reachability re-checked every 24 hours
"""

import asyncio
import concurrent.futures
import hashlib
import hmac
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2
import psycopg2.extras
import psycopg2.pool
import redis as redis_lib
import secp256k1
import websockets
from nostr_crypto_shared import extract_p_tag_pubkeys, is_valid_hex_pubkey, verify_event
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("indexer")

# --- Config ---

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STRFRY_URL = os.environ.get("STRFRY_URL", "ws://strfry:7777")
NEO4J_URL = os.environ.get("NEO4J_URL", "neo4j://neo4j:7687")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
if not NEO4J_PASSWORD:
    logger.warning("NEO4J_PASSWORD not set — Neo4j operations will fail")
    NEO4J_PASSWORD = ""

GRAPERANK_INTERVAL = 21600  # 6 hours
HOUSE_GRAPERANK_INTERVAL = int(os.environ.get("HOUSE_GRAPERANK_REFRESH_INTERVAL", str(GRAPERANK_INTERVAL)))
NIP05_CHECK_INTERVAL = 86400  # 24 hours
FULL_RECOMPUTE_DAY = 6  # Sunday (0=Monday in weekday())
GRAPERANK_TIMEOUT = 300  # 5 min timeout waiting for Java GrapeRank result
GRAPERANK_VALID_THRESHOLD = float(os.environ.get("GRAPERANK_CUTOFF_VALID_USER", "0.02"))
def _read_secret(name):
    try:
        with open(f"/run/secrets/{name}") as f:
            val = f.read().strip()
            if val:
                return val
    except FileNotFoundError:
        pass
    return os.environ.get(name, "")

GRAPERANK_QUEUE_SECRET = _read_secret("GRAPERANK_QUEUE_SECRET") or _read_secret("AUTH_SECRET")

NEO4J_REBUILD_LIMIT = int(os.environ.get("NEO4J_REBUILD_LIMIT", "500000"))

TRACKED_KINDS = {0, 1, 3, 6, 7, 9735, 10000, 1984}

# NIP-85 config.
# Security note: use a dedicated NIP85_SIGNING_KEY in production. Falling back
# to RELAY_PRIVATE_KEY preserves existing deployments but reuses one key across
# relay/admin/publishing roles and broadens blast radius if any role is exposed.
NIP85_SIGNING_KEY = os.environ.get("NIP85_SIGNING_KEY", "")
RELAY_PRIVATE_KEY = os.environ.get("RELAY_PRIVATE_KEY", "")
try:
    _relay_secret_path = "/run/secrets/NIP85_SIGNING_KEY" if NIP85_SIGNING_KEY else "/run/secrets/RELAY_PRIVATE_KEY"
    with open(_relay_secret_path) as _f:
        _val = _f.read().strip()
        if _val:
            NIP85_SIGNING_KEY = _val
except FileNotFoundError:
    pass
if not NIP85_SIGNING_KEY:
    NIP85_SIGNING_KEY = RELAY_PRIVATE_KEY

NIP85_SCORE_THRESHOLD = int(os.environ.get("NIP85_SCORE_THRESHOLD", "1"))  # Only republish when score changes by more than this

HOUSE_OBSERVER_ENV_KEYS = (
    "NOSTRBTC_HOUSE_POV_PUBKEY",
    "HOUSE_GRAPERANK_OBSERVER",
    "BRAINSTORM_HOUSE_POV_PUBKEY",
)
BRAINSTORM_HOUSE_SCORE_API_URL = (
    os.environ.get("BRAINSTORM_HOUSE_SCORE_API_URL")
    or "https://brainstorm.world/api/search/profiles/meili/document/{pubkey}"
).strip()
BRAINSTORM_HOUSE_SCORE_TIMEOUT = float(os.environ.get("BRAINSTORM_HOUSE_SCORE_TIMEOUT", "4"))
BRAINSTORM_HOUSE_SCORE_CONCURRENCY = max(1, int(os.environ.get("BRAINSTORM_HOUSE_SCORE_CONCURRENCY", "8")))
BRAINSTORM_HOUSE_SCORE_ENABLED = (
    os.environ.get("BRAINSTORM_HOUSE_SCORE_ENABLED", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)


# --- Trust tiers ---

def score_to_tier(score, verified_threshold=GRAPERANK_VALID_THRESHOLD):
    if score >= 0.50:
        return "highly_trusted"
    elif score >= 0.20:
        return "trusted"
    elif score >= 0.07:
        return "neutral"
    elif score >= verified_threshold:
        return "low_trust"
    else:
        return "unverified"


def configured_house_observer_pubkey() -> str:
    for key in HOUSE_OBSERVER_ENV_KEYS:
        value = os.environ.get(key, "").strip().lower()
        if not value:
            continue
        if len(value) == 64 and all(c in "0123456789abcdef" for c in value):
            return value
        logger.warning("%s is not a 64-char hex pubkey; house GrapeRank disabled", key)
        return ""
    return ""


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_brainstorm_rank(value):
    rank = _safe_float(value)
    if rank is None:
        return None
    score = rank / 100.0 if rank > 1.0 else rank
    return max(0.0, min(score, 1.0))


def _score_from_brainstorm_document(doc: dict, observer_pubkey: str):
    if not isinstance(doc, dict):
        return None
    score = _normalize_brainstorm_rank(doc.get("wot_rank"))
    if score is None:
        return None
    document_pov = str(doc.get("wot_pov") or "").strip().lower()
    if document_pov and observer_pubkey and document_pov != observer_pubkey:
        logger.debug(
            "Brainstorm house score POV mismatch for %s: document=%s configured=%s",
            str(doc.get("pubkey") or doc.get("id") or "")[:16],
            document_pov[:16],
            observer_pubkey[:16],
        )
    return {
        "score": score,
        "tier": score_to_tier(score),
        "hops": _safe_int(doc.get("wot_hops")),
        "average_score": None,
        "confidence": score,
        "total_input": None,
        "verified": score >= GRAPERANK_VALID_THRESHOLD,
        "verified_followers": _safe_int(doc.get("wot_followers")) or 0,
    }


def _brainstorm_house_score_url(pubkey: str) -> str:
    import urllib.parse
    encoded = urllib.parse.quote(pubkey, safe="")
    if "{pubkey}" in BRAINSTORM_HOUSE_SCORE_API_URL:
        return BRAINSTORM_HOUSE_SCORE_API_URL.format(pubkey=encoded)
    return BRAINSTORM_HOUSE_SCORE_API_URL.rstrip("/") + "/" + encoded


def _fetch_brainstorm_house_score(pubkey: str, observer_pubkey: str):
    if not BRAINSTORM_HOUSE_SCORE_ENABLED or not BRAINSTORM_HOUSE_SCORE_API_URL:
        return pubkey, None
    try:
        with httpx.Client(timeout=BRAINSTORM_HOUSE_SCORE_TIMEOUT, follow_redirects=True) as client:
            response = client.get(_brainstorm_house_score_url(pubkey), headers={"Accept": "application/json"})
            response.raise_for_status()
            payload = response.json()
    except Exception as e:
        logger.debug("Brainstorm house score fetch failed for %s: %s", pubkey[:16], e)
        return pubkey, None
    doc = payload.get("document") if isinstance(payload, dict) else None
    if not doc and isinstance(payload, dict) and isinstance(payload.get("hits"), list) and payload["hits"]:
        doc = payload["hits"][0]
    return pubkey, _score_from_brainstorm_document(doc, observer_pubkey)


def _fetch_brainstorm_house_scores(member_pubkeys: set, observer_pubkey: str) -> dict:
    if not BRAINSTORM_HOUSE_SCORE_ENABLED or not member_pubkeys:
        return {}
    scores = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=BRAINSTORM_HOUSE_SCORE_CONCURRENCY) as executor:
        futures = [
            executor.submit(_fetch_brainstorm_house_score, pubkey, observer_pubkey)
            for pubkey in sorted(member_pubkeys)
        ]
        for future in concurrent.futures.as_completed(futures):
            pubkey, score = future.result()
            if score is not None:
                scores[pubkey] = score
    logger.info("Brainstorm house import: loaded %d/%d public wot_rank scores", len(scores), len(member_pubkeys))
    return scores


# --- Nostr Signing (minimal, for NIP-85 event publishing) ---


def _privkey_to_pubkey(privkey_hex: str) -> str:
    sk = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
    return sk.pubkey.serialize(compressed=True).hex()[2:]


def _is_valid_hex_pubkey(s: str) -> bool:
    """Thin wrapper kept for call-sites that pass non-str values."""
    return isinstance(s, str) and is_valid_hex_pubkey(s)


def _make_event(privkey_hex: str, kind: int, content: str, tags: list) -> dict:
    pubkey = _privkey_to_pubkey(privkey_hex)
    event = {
        "pubkey": pubkey,
        "created_at": int(time.time()),
        "kind": kind,
        "tags": tags,
        "content": content,
    }
    serialized = json.dumps(
        [0, event["pubkey"], event["created_at"], event["kind"], event["tags"], event["content"]],
        separators=(",", ":"), ensure_ascii=False,
    ).encode()
    event["id"] = hashlib.sha256(serialized).hexdigest()
    sk = secp256k1.PrivateKey(bytes.fromhex(privkey_hex))
    sig = sk.schnorr_sign(bytes.fromhex(event["id"]), bip340tag=None, raw=True)
    event["sig"] = sig.hex()
    return event


# --- Globals ---

_redis = None
_pg_pool = None
_neo4j_driver = None


def _init_pg_pool():
    """Initialise the Postgres connection pool (min=1, max=4)."""
    global _pg_pool
    _pg_pool = psycopg2.pool.ThreadedConnectionPool(1, 4, DATABASE_URL)


class _pg_conn_ctx:
    """Context manager that borrows a connection from the pool and returns it."""

    def __init__(self, autocommit=False):
        self._autocommit = autocommit
        self._conn = None

    def __enter__(self):
        self._conn = _pg_pool.getconn()
        self._conn.autocommit = self._autocommit
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            if not self._autocommit:
                if exc_type is not None:
                    try:
                        self._conn.rollback()
                    except Exception:
                        pass
            try:
                _pg_pool.putconn(self._conn)
            except Exception:
                pass
            self._conn = None
        return False


def get_pg():
    """Get a Postgres connection from the pool.

    Returns a connection. Callers that only need a short-lived connection should
    prefer ``_pg_conn_ctx()`` as a context manager, but many existing call-sites
    use ``get_pg()`` directly so we keep this helper for compatibility.
    """
    try:
        conn = _pg_pool.getconn()
        conn.autocommit = False
        # Quick health check
        try:
            with conn.cursor() as c:
                c.execute("SELECT 1")
        except Exception:
            # Connection is stale — discard and get a fresh one
            try:
                _pg_pool.putconn(conn, close=True)
            except Exception:
                pass
            conn = _pg_pool.getconn()
            conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Postgres pool.getconn failed: {e}")
        return None


def put_pg(conn):
    """Return a connection to the pool. Must be called after get_pg()."""
    if conn is not None:
        try:
            _pg_pool.putconn(conn)
        except Exception:
            pass


def get_neo4j():
    """Get Neo4j driver (singleton)."""
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    return _neo4j_driver


def get_subscriber_pubkeys():
    """Get all subscriber pubkeys from Redis."""
    members = _redis.smembers("directory:authorized_pubkeys")
    return {m.decode() if isinstance(m, bytes) else m for m in members}


def get_directory_member_pubkeys():
    """Get directory members from Redis (populated from kind 9999 events by indexer)."""
    try:
        members = _redis.smembers("directory:directory_members")
        if members:
            return {m.decode() if isinstance(m, bytes) else m for m in members}
    except Exception as e:
        logger.error(f"Redis directory_members lookup failed: {e}")
    # Fallback to Postgres (cold start before first indexer cycle)
    pg = get_pg()
    if not pg:
        return get_subscriber_pubkeys()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT hex_pubkey FROM subscriptions WHERE directory_listed = TRUE AND expires_at > NOW()")
            result = {row[0] for row in cur.fetchall()}
        return result
    except Exception as e:
        logger.error(f"Failed to fetch directory members: {e}")
        return get_subscriber_pubkeys()
    finally:
        put_pg(pg)


def mark_dirty(pubkey: str):
    """Mark a pubkey as needing GrapeRank recomputation."""
    try:
        _redis.sadd("directory:dirty_pubkeys", pubkey)
    except Exception as e:
        logger.debug(f"mark_dirty Redis error for {pubkey[:16]}: {e}")


# --- Neo4j Graph Sync ---

def _neo4j_sync_edges(driver, pubkey: str, tag_list: set, rel_type: str):
    """Sync a pubkey's edge list to Neo4j. Replaces all existing edges of rel_type.

    Works for FOLLOWS, MUTES, and REPORTS — batches in groups of 500.
    """
    try:
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(
                "MERGE (u:NostrUser {pubkey: $pubkey})",
                pubkey=pubkey
            ))
            # Remove old edges of this type
            session.execute_write(lambda tx: tx.run(
                f"MATCH (u:NostrUser {{pubkey: $pubkey}})-[r:{rel_type}]->() DELETE r",
                pubkey=pubkey
            ))
            # Create new edges in batches
            targets_list = list(tag_list)
            for i in range(0, len(targets_list), 500):
                batch = targets_list[i:i + 500]
                session.execute_write(lambda tx, b=batch: tx.run(
                    f"""
                    UNWIND $targets AS target
                    MERGE (t:NostrUser {{pubkey: target}})
                    WITH t
                    MATCH (u:NostrUser {{pubkey: $pubkey}})
                    MERGE (u)-[:{rel_type}]->(t)
                    """,
                    pubkey=pubkey, targets=b
                ))
    except Exception as e:
        logger.error(f"Neo4j {rel_type} sync failed for {pubkey[:16]}: {e}")


def neo4j_sync_follows(pubkey: str, followed: set):
    """Sync a pubkey's follow list to Neo4j. Replaces all existing FOLLOWS edges."""
    _neo4j_sync_edges(get_neo4j(), pubkey, followed, "FOLLOWS")


def neo4j_sync_mutes(pubkey: str, muted: set):
    """Sync mute list to Neo4j."""
    _neo4j_sync_edges(get_neo4j(), pubkey, muted, "MUTES")


def neo4j_sync_reports(pubkey: str, reported: set):
    """Sync report targets to Neo4j."""
    _neo4j_sync_edges(get_neo4j(), pubkey, reported, "REPORTS")


def neo4j_ensure_index():
    """Create index on NostrUser.pubkey for fast lookups."""
    driver = get_neo4j()
    try:
        with driver.session() as session:
            session.run(
                "CREATE INDEX nostr_user_pubkey IF NOT EXISTS FOR (n:NostrUser) ON (n.pubkey)"
            )
        logger.info("Neo4j: index ensured on NostrUser.pubkey")
    except Exception as e:
        logger.warning(f"Neo4j index creation: {e}")


def neo4j_full_rebuild():
    """Full graph rebuild from strfry: fetch all kind 3/10000/1984 events and sync to Neo4j."""
    logger.info("Neo4j: starting full graph rebuild from strfry...")
    started = time.time()

    driver = get_neo4j()

    # Clear existing graph
    try:
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(
                "MATCH (n:NostrUser) DETACH DELETE n"
            ))
        logger.info("Neo4j: cleared existing graph")
    except Exception as e:
        logger.error(f"Neo4j: failed to clear graph: {e}")
        return

    neo4j_ensure_index()

    # Fetch all kind 3 events from strfry
    # WARNING: This loads up to NEO4J_REBUILD_LIMIT events into memory at once.
    # For very large relays this can consume significant RAM. The limit is
    # configurable via the NEO4J_REBUILD_LIMIT env var (default 500000).

    async def _fetch_all_kind3():
        events = []
        try:
            async with websockets.connect(STRFRY_URL, close_timeout=30, open_timeout=10) as ws:
                await ws.send(json.dumps(["REQ", "rebuild-k3", {
                    "kinds": [3],
                    "limit": NEO4J_REBUILD_LIMIT,
                }]))
                async for msg in ws:
                    data = json.loads(msg)
                    if data[0] == "EVENT" and data[1] == "rebuild-k3":
                        events.append(data[2])
                    elif data[0] == "EOSE":
                        break
                await ws.send(json.dumps(["CLOSE", "rebuild-k3"]))
        except Exception as e:
            logger.error(f"Neo4j rebuild: kind 3 fetch failed: {e}")
        return events

    async def _fetch_all(kind, sid):
        events = []
        try:
            async with websockets.connect(STRFRY_URL, close_timeout=30, open_timeout=10) as ws:
                await ws.send(json.dumps(["REQ", sid, {
                    "kinds": [kind],
                    "limit": 100000,
                }]))
                async for msg in ws:
                    data = json.loads(msg)
                    if data[0] == "EVENT" and data[1] == sid:
                        events.append(data[2])
                    elif data[0] == "EOSE":
                        break
                await ws.send(json.dumps(["CLOSE", sid]))
        except Exception as e:
            logger.error(f"Neo4j rebuild: kind {kind} fetch failed: {e}")
        return events

    loop = asyncio.new_event_loop()
    k3_events = loop.run_until_complete(_fetch_all_kind3())
    k10000_events = loop.run_until_complete(_fetch_all(10000, "rebuild-mutes"))
    k1984_events = loop.run_until_complete(_fetch_all(1984, "rebuild-reports"))
    loop.close()

    logger.info(f"Neo4j rebuild: fetched {len(k3_events)} kind 3, {len(k10000_events)} kind 10000, {len(k1984_events)} kind 1984")

    # Process kind 3 events — batch into Neo4j
    # Deduplicate: keep latest kind 3 per pubkey
    latest_k3 = {}
    for ev in k3_events:
        pk = ev.get("pubkey", "")
        ts = ev.get("created_at", 0)
        if pk and (pk not in latest_k3 or ts > latest_k3[pk].get("created_at", 0)):
            latest_k3[pk] = ev

    follows_synced = 0
    for pk, ev in latest_k3.items():
        followed = set(extract_p_tag_pubkeys(ev.get("tags", [])))
        if followed:
            neo4j_sync_follows(pk, followed)
            follows_synced += 1

    # Process kind 10000 mutes — deduplicate per pubkey
    latest_mutes = {}
    for ev in k10000_events:
        pk = ev.get("pubkey", "")
        ts = ev.get("created_at", 0)
        if pk and (pk not in latest_mutes or ts > latest_mutes[pk].get("created_at", 0)):
            latest_mutes[pk] = ev

    mutes_synced = 0
    for pk, ev in latest_mutes.items():
        muted = set(extract_p_tag_pubkeys(ev.get("tags", [])))
        if muted:
            neo4j_sync_mutes(pk, muted)
            mutes_synced += 1

    # Process kind 1984 reports
    reports_synced = 0
    for ev in k1984_events:
        pk = ev.get("pubkey", "")
        reported = set(extract_p_tag_pubkeys(ev.get("tags", [])))
        if reported:
            neo4j_sync_reports(pk, reported)
            reports_synced += 1

    elapsed = time.time() - started
    logger.info(f"Neo4j rebuild complete: {follows_synced} follow lists, {mutes_synced} mute lists, {reports_synced} reports in {elapsed:.1f}s")


# --- Event Processing ---

def process_kind0(event: dict):
    """Update profile in directory_profiles from kind 0 metadata."""
    pubkey = event.get("pubkey", "")
    try:
        content = json.loads(event.get("content", "{}"))
    except (json.JSONDecodeError, TypeError):
        return

    name = (content.get("name") or content.get("display_name") or "")[:100]
    about = (content.get("about") or "")[:200]
    picture = (content.get("picture") or "")[:500]
    nip05 = (content.get("nip05") or "")[:200]
    lud16 = (content.get("lud16") or content.get("lud06") or "")[:200]

    pg = get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("""
                UPDATE directory_profiles SET
                    name = %s, about = %s, picture = %s,
                    nip05 = %s, lud16 = %s, updated_at = NOW()
                WHERE hex_pubkey = %s
            """, (name, about, picture, nip05, lud16, pubkey))
            updated = cur.rowcount
        pg.commit()
        if updated > 0:
            logger.debug(f"Updated profile for {pubkey[:12]}...")
    except Exception as e:
        logger.error(f"kind 0 update failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
    finally:
        put_pg(pg)


def process_kind1(event: dict):
    """Increment note_count and update last_active for kind 1 events."""
    pubkey = event.get("pubkey", "")

    pg = get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("""
                UPDATE directory_profiles SET
                    note_count = COALESCE(note_count, 0) + 1,
                    updated_at = NOW()
                WHERE hex_pubkey = %s
            """, (pubkey,))
        pg.commit()
    except Exception as e:
        logger.error(f"kind 1 update failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
    finally:
        put_pg(pg)


def process_kind3(event: dict):
    """Update follow graph in Postgres (trust_edges) AND Neo4j."""
    pubkey = event.get("pubkey", "")
    tags = event.get("tags", [])

    followed = set(extract_p_tag_pubkeys(tags))

    if not followed:
        return

    # Postgres trust_edges (existing behavior)
    pg = get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM trust_edges WHERE source_pubkey = %s AND edge_type = 'follow'",
                       (pubkey,))
            values = [(pubkey, target, "follow", 1.0) for target in followed]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO trust_edges (source_pubkey, target_pubkey, edge_type, weight, last_seen_at) VALUES %s "
                "ON CONFLICT (source_pubkey, target_pubkey, edge_type) DO UPDATE SET "
                "weight = EXCLUDED.weight, last_seen_at = EXCLUDED.last_seen_at",
                values,
                template="(%s, %s, %s, %s, NOW())"
            )
        pg.commit()
        mark_dirty(pubkey)
        for t in followed:
            mark_dirty(t)
    except Exception as e:
        logger.error(f"kind 3 Postgres update failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
    finally:
        put_pg(pg)

    # Neo4j graph sync
    neo4j_sync_follows(pubkey, followed)


def process_kind10000(event: dict):
    """Sync mute list (kind 10000) to Neo4j."""
    pubkey = event.get("pubkey", "")
    tags = event.get("tags", [])

    muted = set(extract_p_tag_pubkeys(tags))

    if muted:
        neo4j_sync_mutes(pubkey, muted)


def process_kind1984(event: dict):
    """Sync report (kind 1984) to Neo4j."""
    pubkey = event.get("pubkey", "")
    tags = event.get("tags", [])

    reported = set(extract_p_tag_pubkeys(tags))

    if reported:
        neo4j_sync_reports(pubkey, reported)


def process_interaction(event: dict):
    """Track interactions (kind 6 repost, kind 7 reaction) for trust graph."""
    pubkey = event.get("pubkey", "")
    kind = event.get("kind", 0)
    tags = event.get("tags", [])

    p_targets = extract_p_tag_pubkeys(tags)
    target = p_targets[0] if p_targets else None
    if not target or target == pubkey:
        return

    edge_type = "repost" if kind == 6 else "reaction"
    pg = get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO trust_edges (source_pubkey, target_pubkey, edge_type, weight)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_pubkey, target_pubkey, edge_type) DO UPDATE SET
                    weight = LEAST(trust_edges.weight + EXCLUDED.weight, 100.0),
                    last_seen_at = NOW()
            """, (pubkey, target, edge_type, 0.1))
        pg.commit()
        mark_dirty(pubkey)
        mark_dirty(target)
    except Exception as e:
        logger.error(f"Interaction update failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
    finally:
        put_pg(pg)


def process_zap(event: dict):
    """Track zap receipts (kind 9735) for trust graph."""
    tags = event.get("tags", [])

    sender = None
    receiver = None
    bolt11 = None

    for tag in tags:
        if tag[0] == "P" and len(tag) >= 2:
            receiver = tag[1]
        elif tag[0] == "p" and len(tag) >= 2:
            if receiver is None:
                receiver = tag[1]
        elif tag[0] == "bolt11" and len(tag) >= 2:
            bolt11 = tag[1]
        elif tag[0] == "description" and len(tag) >= 2:
            try:
                desc = json.loads(tag[1])
                sender = desc.get("pubkey")
            except (json.JSONDecodeError, TypeError):
                pass

    if not sender or not receiver or sender == receiver:
        return
    if not _is_valid_hex_pubkey(sender) or not _is_valid_hex_pubkey(receiver):
        return

    amount_msats = 0
    if bolt11:
        import re
        m = re.match(r"lnbc(\d+)([munp]?)", bolt11.lower())
        if m:
            amount, mult = int(m.group(1)), m.group(2)
            if mult == "m":
                amount_msats = amount * 100_000_000
            elif mult == "u":
                amount_msats = amount * 100_000
            elif mult == "n":
                amount_msats = amount * 100
            elif mult == "p":
                amount_msats = amount * 10
            else:
                amount_msats = amount * 100_000_000_000

    pg = get_pg()
    if not pg:
        return
    try:
        with pg.cursor() as cur:
            cur.execute("""
                INSERT INTO trust_edges (source_pubkey, target_pubkey, edge_type, weight)
                VALUES (%s, %s, 'zap', %s)
                ON CONFLICT (source_pubkey, target_pubkey, edge_type) DO UPDATE SET
                    weight = LEAST(trust_edges.weight + EXCLUDED.weight, 100.0),
                    last_seen_at = NOW()
            """, (sender, receiver, max(1, amount_msats // 1000)))
        pg.commit()
        mark_dirty(sender)
        mark_dirty(receiver)
    except Exception as e:
        logger.error(f"Zap update failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
    finally:
        put_pg(pg)


EVENT_PROCESSORS = {
    0: process_kind0,
    1: process_kind1,
    3: process_kind3,
    6: process_interaction,
    7: process_interaction,
    9735: process_zap,
    10000: process_kind10000,
    1984: process_kind1984,
}


# --- GrapeRank (Java service via Redis queue) ---

_job_counter = 0


def sign_queue_message(payload: str) -> str:
    if not GRAPERANK_QUEUE_SECRET:
        raise RuntimeError("GRAPERANK_QUEUE_SECRET or AUTH_SECRET must be set for Redis job signing")
    sig = hmac.new(GRAPERANK_QUEUE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return json.dumps({"payload": payload, "hmac": sig}, separators=(",", ":"))


def trigger_graperank(observer: str) -> dict:
    """Trigger Java GrapeRank for one observer via Redis message queue.

    Pushes a job to `message_queue`, waits for result on `results_message_queue`.
    Returns {pubkey: ScoreCard_dict} or empty dict on failure.
    """
    global _job_counter
    _job_counter += 1
    job_id = _job_counter

    # Push job to GrapeRank Java service
    message = json.dumps({"private_id": job_id, "parameters": observer})
    _redis.rpush("message_queue", sign_queue_message(message))
    logger.info(f"GrapeRank: triggered job {job_id} for observer {observer[:16]}...")

    # Wait for result — poll results_message_queue
    # The Java service pushes results to results_message_queue
    started = time.time()
    while time.time() - started < GRAPERANK_TIMEOUT:
        result = _redis.blpop("results_message_queue", timeout=5)
        if result is None:
            continue

        try:
            data = json.loads(result[1])
            if data.get("private_id") == job_id:
                gr_result = data.get("result", {})
                if gr_result.get("success"):
                    scorecards = gr_result.get("scorecards", {})
                    if not isinstance(scorecards, dict):
                        logger.warning(f"GrapeRank: job {job_id} returned non-dict scorecards")
                        return {}
                    rounds = gr_result.get("rounds", 0)
                    duration = gr_result.get("duration_seconds", 0)
                    logger.info(f"GrapeRank: job {job_id} complete — {len(scorecards)} scorecards, {rounds} rounds, {duration:.1f}s")
                    return scorecards
                else:
                    logger.warning(f"GrapeRank: job {job_id} returned success=false")
                    return {}
            else:
                # Not our job — put it back (other consumer might need it)
                _redis.rpush("results_message_queue", result[1])
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.error(f"GrapeRank: failed to parse result: {e}")

    logger.error(f"GrapeRank: timeout waiting for job {job_id}")
    return {}


def store_observer_scores(observer: str, scorecards: dict, member_pubkeys: set) -> dict:
    """Extract member-only scores from scorecards and store in personalized_scores.

    Returns {target_pubkey: score} for directory members only.
    """
    pg = get_pg()
    if not pg:
        return {}

    member_scores = {}
    for pk, sc in scorecards.items():
        if pk in member_pubkeys and pk != observer:
            influence = sc.get("influence", 0.0)
            hops = int(sc.get("hops", 999))
            tier = score_to_tier(influence)
            member_scores[pk] = {
                "score": influence,
                "tier": tier,
                "hops": hops,
            }

    if member_scores:
        try:
            with pg.cursor() as cur:
                values = [
                    (observer, pk, data["score"], data["tier"], data["hops"])
                    for pk, data in member_scores.items()
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO personalized_scores (observer_pubkey, target_pubkey, score, tier, hops, computed_at)
                       VALUES %s
                       ON CONFLICT (observer_pubkey, target_pubkey) DO UPDATE SET
                           score = EXCLUDED.score,
                           tier = EXCLUDED.tier,
                           hops = EXCLUDED.hops,
                           computed_at = NOW()""",
                    values,
                    template="(%s, %s, %s, %s, %s, NOW())"
                )
            pg.commit()
            logger.info(f"GrapeRank: stored {len(member_scores)} scores for observer {observer[:16]}")
        except Exception as e:
            logger.error(f"GrapeRank: failed to write scores for {observer[:16]}: {e}")
            try:
                pg.rollback()
            except Exception:
                pass

    put_pg(pg)
    return {pk: data["score"] for pk, data in member_scores.items()}


def run_graperank_for_observer(observer: str, member_pubkeys: set) -> dict:
    """Run GrapeRank for one observer (on-demand), store results.

    Returns {target_pubkey: score} for directory members only.
    """
    scorecards = trigger_graperank(observer)
    if not scorecards:
        return {}
    return store_observer_scores(observer, scorecards, member_pubkeys)


def trigger_graperank_batch(observers: list) -> dict:
    """Trigger batch GrapeRank for all observers in one graph load.

    Returns {observer_pubkey: {target_pubkey: ScoreCard_dict}} or empty on failure.
    """
    global _job_counter
    _job_counter += 1
    job_id = _job_counter

    message = json.dumps({
        "private_id": job_id,
        "type": "batch",
        "observers": observers
    })
    _redis.rpush("message_queue", sign_queue_message(message))
    logger.info(f"GrapeRank batch: job {job_id} for {len(observers)} observers")

    started = time.time()
    timeout = GRAPERANK_TIMEOUT * 2  # longer for batch
    while time.time() - started < timeout:
        result = _redis.blpop("results_message_queue", timeout=10)
        if result is None:
            continue

        try:
            data = json.loads(result[1])
            if data.get("private_id") == job_id:
                if data.get("type") == "batch":
                    results = data.get("results", {})
                    graph_nodes = data.get("graph_nodes", "?")
                    graph_edges = data.get("graph_edges", "?")
                    logger.info(f"GrapeRank batch: job {job_id} complete — "
                                f"{len(results)} observers, {graph_nodes} nodes, {graph_edges} edges")
                    return results
                else:
                    logger.warning(f"GrapeRank batch: unexpected result format for job {job_id}")
                    return {}
            else:
                _redis.rpush("results_message_queue", result[1])
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.error(f"GrapeRank batch: parse error: {e}")

    logger.error(f"GrapeRank batch: timeout for job {job_id}")
    return {}


# --- NIP-85 Trusted Assertions (per-observer) ---


def _graperank_to_reputation(gr_score: float) -> int:
    """Map raw GrapeRank 0-1 score to reputation 0-100."""
    if gr_score <= 0:
        return 0
    score = 100.0 * min(gr_score, 1.0) ** 0.5
    return min(100, max(0, int(round(score))))


async def publish_nip85_assertions(all_observer_scores: dict, full: bool = False):
    """Publish kind 30382 NIP-85 assertions per observer.

    Args:
        all_observer_scores: {observer_pubkey: {target_pubkey: score}}
        full: If True, publish all regardless of change threshold.
    """
    if not NIP85_SIGNING_KEY:
        logger.warning("NIP-85: no signing key configured, skipping assertions")
        return
    if not all_observer_scores:
        return

    pg = get_pg()
    if not pg:
        return

    # Collect all target pubkeys across all observers
    all_targets = set()
    for scores in all_observer_scores.values():
        all_targets.update(scores.keys())

    if not all_targets:
        put_pg(pg)
        return

    # Fetch member metadata
    pubkeys = list(all_targets)
    try:
        from psycopg2 import sql as psql
        pk_list = psql.SQL(",").join([psql.Placeholder()] * len(pubkeys))
        with pg.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(psql.SQL("""
                SELECT dp.hex_pubkey, dp.nip05_verified, dp.lightning_reachable,
                       dp.last_active,
                       dp.name, dp.about, dp.picture, dp.banner, dp.nip05, dp.lud16, dp.website,
                       COALESCE(s.created_at, dp.indexed_at) AS member_since
                FROM directory_profiles dp
                LEFT JOIN subscriptions s ON dp.hex_pubkey = s.hex_pubkey
                WHERE dp.hex_pubkey IN ({})
            """).format(pk_list), pubkeys)
            profiles = {row["hex_pubkey"]: dict(row) for row in cur.fetchall()}

            cur.execute(psql.SQL("""
                SELECT target_pubkey, COUNT(*) AS cnt
                FROM trust_edges
                WHERE edge_type = 'follow' AND target_pubkey IN ({})
                GROUP BY target_pubkey
            """).format(pk_list), pubkeys)
            follower_counts = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(psql.SQL("""
                SELECT source_pubkey, COUNT(*) AS cnt
                FROM trust_edges
                WHERE edge_type = 'follow' AND source_pubkey IN ({})
                GROUP BY source_pubkey
            """).format(pk_list), pubkeys)
            following_counts = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute(psql.SQL("""
                SELECT receiver, COALESCE(SUM(amount_msats), 0) / 1000 AS total_sats
                FROM directory_zaps
                WHERE receiver IN ({})
                GROUP BY receiver
            """).format(pk_list), pubkeys)
            zap_totals = {row[0]: int(row[1]) for row in cur.fetchall()}

            thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
            cur.execute(psql.SQL("""
                SELECT pubkey, COUNT(*) AS active_days
                FROM activity_heatmap
                WHERE pubkey IN ({})
                  AND day >= %s
                GROUP BY pubkey
            """).format(pk_list), pubkeys + [thirty_days_ago])
            active_days = {row[0]: row[1] for row in cur.fetchall()}

            # Get hops from personalized_scores
            all_observers = list(all_observer_scores.keys())
            obs_list = psql.SQL(",").join([psql.Placeholder()] * len(all_observers))
            cur.execute(psql.SQL("""
                SELECT observer_pubkey, target_pubkey, hops
                FROM personalized_scores
                WHERE observer_pubkey IN ({})
            """).format(obs_list), all_observers)
            hops_map = {}
            for row in cur.fetchall():
                hops_map.setdefault(row[0], {})[row[1]] = row[2]
    except Exception as e:
        logger.error(f"NIP-85: Failed to fetch member data: {e}")
        return
    finally:
        put_pg(pg)

    # Build per-observer events
    events_to_publish = []
    skipped = 0

    for observer, scores in all_observer_scores.items():
        for pk, raw_score in scores.items():
            rep_score = _graperank_to_reputation(raw_score)
            tier = score_to_tier(raw_score)
            hops = hops_map.get(observer, {}).get(pk, 999)

            if not full:
                # Redis client uses decode_responses=False so we must encode
                # keys/values to bytes explicitly.  The backend reads with
                # decode_responses=True and receives strings transparently.
                hash_key = f"directory:nip85_last:{observer}".encode()
                last_val = _redis.hget(hash_key, pk.encode())
                if last_val is not None:
                    last_score = int(last_val)
                    if abs(rep_score - last_score) <= NIP85_SCORE_THRESHOLD:
                        skipped += 1
                        continue

            profile = profiles.get(pk, {})
            followers = follower_counts.get(pk, 0)
            following = following_counts.get(pk, 0)
            zaps = zap_totals.get(pk, 0)
            days_active = active_days.get(pk, 0)
            member_since = profile.get("member_since")
            member_since_ts = int(member_since.timestamp()) if member_since else 0
            nip05_valid = "1" if profile.get("nip05_verified") else "0"
            lightning_valid = "1" if profile.get("lightning_reachable") else "0"

            follow_ratio = round(followers / max(following, 1), 2)
            follow_ratio = min(follow_ratio, 999.0)

            completeness_fields = ["name", "about", "picture", "banner", "nip05", "lud16", "website"]
            completeness = sum(14 for f in completeness_fields if profile.get(f))
            completeness = min(completeness, 100)

            tags = [
                ["d", pk],
                ["p", observer],
                ["rank", str(rep_score)],
                ["tier", tier],
                ["hops", str(hops)],
                ["algorithm", "graperank_v1"],
                ["followers", str(followers)],
                ["following", str(following)],
                ["follow_ratio", str(follow_ratio)],
                ["active_days_30", str(days_active)],
                ["first_seen", str(member_since_ts)],
                ["nip05_valid", nip05_valid],
                ["lightning_valid", lightning_valid],
                ["profile_completeness", str(completeness)],
            ]

            try:
                event = _make_event(NIP85_SIGNING_KEY, kind=30382, content="", tags=tags)
                events_to_publish.append((observer, pk, rep_score, event))
            except Exception as e:
                logger.error(f"NIP-85: Failed to sign event for {observer[:12]}→{pk[:12]}: {e}")

    if not events_to_publish:
        logger.info(f"NIP-85: nothing to publish (skipped {skipped})")
        return

    # Publish all events
    published = 0
    try:
        async with websockets.connect(STRFRY_URL, close_timeout=10, open_timeout=5) as ws:
            for observer, pk, rep_score, event in events_to_publish:
                try:
                    await ws.send(json.dumps(["EVENT", event]))
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(msg)
                    if isinstance(data, list) and len(data) >= 3 and data[0] == "OK" and data[2]:
                        published += 1
                        _redis.hset(f"directory:nip85_last:{observer}".encode(), pk.encode(), str(rep_score).encode())
                    else:
                        logger.debug(f"NIP-85: strfry rejected for {observer[:12]}→{pk[:12]}: {data}")
                except asyncio.TimeoutError:
                    logger.debug(f"NIP-85: strfry no OK for {observer[:12]}→{pk[:12]}")
                except Exception as e:
                    logger.debug(f"NIP-85: send failed for {observer[:12]}→{pk[:12]}: {e}")
    except Exception as e:
        logger.error(f"NIP-85: websocket connection failed: {e}")

    mode = "full" if full else "scheduled"
    logger.info(f"NIP-85 ({mode}): published {published}, skipped {skipped}, total {len(events_to_publish) + skipped}")


async def lazy_nip85_consumer():
    """Consume NIP-85 publish requests queued by the backend after on-demand GrapeRank."""
    await asyncio.sleep(30)  # Wait for startup
    while True:
        try:
            result = await asyncio.to_thread(_redis.blpop, "directory:nip85_publish_queue", 30)
            if result is None:
                continue
            _, observer = result
            if isinstance(observer, bytes):
                observer = observer.decode()

            # Load scores from Postgres
            pg = get_pg()
            if not pg:
                continue
            try:
                with pg.cursor() as cur:
                    cur.execute(
                        "SELECT target_pubkey, score FROM personalized_scores WHERE observer_pubkey = %s",
                        (observer,)
                    )
                    scores = {row[0]: row[1] for row in cur.fetchall()}
            except Exception as e:
                logger.error(f"NIP-85 lazy: failed to read scores for {observer[:16]}: {e}")
                continue
            finally:
                put_pg(pg)

            if scores:
                await publish_nip85_assertions({observer: scores}, full=False)
                logger.info(f"NIP-85 lazy: published for observer {observer[:16]}")
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"NIP-85 lazy consumer error: {e}")
            await asyncio.sleep(5)


# --- NIP-05 Verification ---

def _is_public_ip(ip_str: str) -> bool:
    """Check if an IP address is public (not private/loopback/link-local)."""
    import ipaddress
    try:
        ip = ipaddress.ip_address(ip_str)
        return ip.is_global and not ip.is_reserved
    except ValueError:
        return False


async def _resolve_safe(domain: str) -> str | None:
    """Resolve domain and return first public IP, or None if unsafe."""
    import socket
    try:
        results = await asyncio.get_event_loop().run_in_executor(
            None, socket.getaddrinfo, domain, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
        for family, _, _, _, sockaddr in results:
            ip = sockaddr[0]
            if _is_public_ip(ip):
                return ip
        return None
    except Exception:
        return None


async def check_nip05_batch():
    """Re-verify NIP-05 identifiers for all directory profiles."""
    pg = get_pg()
    if not pg:
        return

    try:
        with pg.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT hex_pubkey, nip05 FROM directory_profiles WHERE nip05 IS NOT NULL AND nip05 != ''")
            profiles = cur.fetchall()
    except Exception as e:
        logger.error(f"NIP-05 fetch failed: {e}")
        put_pg(pg)
        return

    if not profiles:
        put_pg(pg)
        return

    verified = 0
    failed = 0
    results = {}  # {hex_pubkey: bool}
    async with httpx.AsyncClient(timeout=8, follow_redirects=False) as client:
        for row in profiles:
            pk = row["hex_pubkey"]
            nip05 = row["nip05"]
            if "@" not in nip05:
                continue
            name, domain = nip05.rsplit("@", 1)
            try:
                # SSRF protection: resolve DNS and verify public IP
                safe_ip = await _resolve_safe(domain)
                if not safe_ip:
                    results[pk] = False
                    failed += 1
                    continue
                url = f"https://{safe_ip}/.well-known/nostr.json?name={name}"
                resp = await client.get(url, headers={"Host": domain})
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("names", {}).get(name) == pk:
                        verified += 1
                        results[pk] = True
                    else:
                        results[pk] = False
                        failed += 1
                else:
                    results[pk] = False
                    failed += 1
            except Exception as e:
                logger.debug(f"NIP-05 check failed for {pk[:12]} ({nip05}): {e}")
                results[pk] = False
                failed += 1
            await asyncio.sleep(0.5)

    # Persist NIP-05 verification results back to directory_profiles
    if results:
        pg2 = get_pg()
        if pg2:
            try:
                with pg2.cursor() as cur:
                    for pk, is_verified in results.items():
                        cur.execute(
                            "UPDATE directory_profiles SET nip05_verified = %s WHERE hex_pubkey = %s",
                            (is_verified, pk),
                        )
                pg2.commit()
            except Exception as e:
                logger.error(f"NIP-05: failed to persist verification results: {e}")
                try:
                    pg2.rollback()
                except Exception:
                    pass
            finally:
                put_pg(pg2)

    put_pg(pg)
    logger.info(f"NIP-05 verification: {verified}/{len(profiles)} verified, {failed} failed")


# --- Strfry Event Stream ---

async def tail_strfry():
    """Connect to strfry WebSocket and process incoming events."""
    subscribers = get_subscriber_pubkeys()
    reconnect_delay = 1.0

    while True:
        try:
            async with websockets.connect(STRFRY_URL, open_timeout=10, ping_interval=30) as ws:
                reconnect_delay = 1.0
                logger.info(f"Connected to strfry, streaming events for {len(subscribers)} subscribers")

                sub_list = list(subscribers)
                for i in range(0, len(sub_list), 150):
                    batch = sub_list[i:i + 150]
                    sid = f"idx-{i}"
                    await ws.send(json.dumps(["REQ", sid,
                        {"authors": batch, "kinds": list(TRACKED_KINDS), "since": int(time.time()) - 300},
                        {"#p": batch, "kinds": [6, 7, 9735], "since": int(time.time()) - 300},
                    ]))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    if not isinstance(msg, list) or len(msg) < 2:
                        continue

                    if msg[0] == "EVENT" and len(msg) >= 3:
                        event = msg[2]
                        if isinstance(event, dict):
                            if not verify_event(event):
                                logger.warning(f"Rejected event with invalid signature: {event.get('id', '?')[:16]}")
                                continue
                            kind = event.get("kind", -1)
                            processor = EVENT_PROCESSORS.get(kind)
                            if processor:
                                try:
                                    processor(event)
                                except Exception as e:
                                    logger.error(f"Event processing failed (kind {kind}): {e}")

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.warning(f"Strfry connection error: {e}")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 60)

        try:
            subscribers = get_subscriber_pubkeys()
        except Exception:
            pass


def _compute_global_consensus():
    """Compute global consensus scores from all member personalized scores.

    Weighted average of all member personalized scores. Each member's opinion
    is weighted by their own global score (bootstrap: equal weight on first run).
    Uses atomic temp-table swap so readers never see an empty table.
    """
    pg = get_pg()
    if not pg:
        return 0
    try:
        # Get all member pubkeys
        with pg.cursor() as cur:
            cur.execute("SELECT hex_pubkey FROM directory_profiles")
            member_pks = {row[0] for row in cur.fetchall()}
        if not member_pks:
            return 0

        # Load all personalized scores for member observers
        all_scores = {}  # {observer: {target: score}}
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

        # Load existing global scores for observer weighting (bootstrap: equal weight)
        observer_weights = {}
        with pg.cursor() as cur:
            cur.execute("SELECT target_pubkey, score FROM global_consensus_scores")
            existing = {row[0]: row[1] for row in cur.fetchall()}
        for obs in all_scores:
            observer_weights[obs] = existing.get(obs, 1.0 / len(all_scores))

        # Compute weighted average per target
        target_num = {}
        target_den = {}
        for observer, scores in all_scores.items():
            w = max(observer_weights[observer], 0.001)
            for target, score in scores.items():
                target_num[target] = target_num.get(target, 0.0) + score * w
                target_den[target] = target_den.get(target, 0.0) + w

        consensus = {}
        for target in target_num:
            if target_den[target] > 0:
                consensus[target] = target_num[target] / target_den[target]
        if not consensus:
            return 0
    except Exception as e:
        logger.error(f"Global consensus computation failed (read phase): {e}")
        try:
            pg.rollback()
        except Exception:
            pass
        return 0
    finally:
        put_pg(pg)

    # Write to Postgres atomically using a fresh autocommit connection so we
    # control the transaction boundary explicitly (no nested BEGIN inside an
    # implicit transaction).
    try:
        with _pg_conn_ctx(autocommit=True) as wconn:
            with wconn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("""
                    CREATE TEMP TABLE _gcs_new (
                        target_pubkey TEXT PRIMARY KEY,
                        score REAL NOT NULL,
                        tier TEXT NOT NULL,
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    ) ON COMMIT DROP
                """)
                values = [(pk, s, score_to_tier(s)) for pk, s in consensus.items()]
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO _gcs_new (target_pubkey, score, tier, computed_at) VALUES %s",
                    values,
                    template="(%s, %s, %s, NOW())"
                )
                cur.execute("TRUNCATE global_consensus_scores")
                cur.execute("INSERT INTO global_consensus_scores SELECT * FROM _gcs_new")
                cur.execute("COMMIT")
    except Exception as e:
        logger.error(f"Global consensus computation failed (write phase): {e}")
        return 0

    # Cache in Redis (worker uses decode_responses=False, so encode keys/values
    # explicitly — backend reads with decode_responses=True and gets strings)
    try:
        pipe = _redis.pipeline()
        pipe.delete(b"directory:global_scores")
        score_map = {pk.encode(): str(round(s, 6)).encode() for pk, s in consensus.items()}
        if score_map:
            pipe.hset(b"directory:global_scores", mapping=score_map)
        pipe.execute()
    except Exception as e:
        logger.error(f"Redis global_scores cache failed: {e}")

    logger.info(f"Global consensus: {len(consensus)} scores from {len(all_scores)} observers")
    return len(consensus)


def store_house_graperank_scores(observer: str, scores: dict, member_pubkeys: set | None = None) -> int:
    """Store public house GrapeRank scores for anonymous directory ranking."""
    if not observer or not scores:
        return 0

    pg = get_pg()
    if not pg:
        return 0

    try:
        with pg.cursor() as cur:
            cur.execute("DELETE FROM house_graperank_scores WHERE source_observer <> %s", (observer,))
            if member_pubkeys:
                cur.execute(
                    "DELETE FROM house_graperank_scores WHERE NOT (target_pubkey = ANY(%s))",
                    (list(member_pubkeys),),
                )

            values = [
                (
                    pk,
                    float(data["score"]),
                    data.get("tier") or score_to_tier(float(data["score"])),
                    data.get("hops"),
                    data.get("average_score"),
                    data.get("confidence"),
                    data.get("total_input"),
                    bool(data.get("verified")),
                    int(data.get("verified_followers") or 0),
                    observer,
                )
                for pk, data in scores.items()
            ]
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO house_graperank_scores (
                    target_pubkey, score, tier, hops, average_score, confidence,
                    total_input, verified, verified_followers, source_observer, computed_at
                ) VALUES %s
                ON CONFLICT (target_pubkey) DO UPDATE SET
                    score = EXCLUDED.score,
                    tier = EXCLUDED.tier,
                    hops = EXCLUDED.hops,
                    average_score = EXCLUDED.average_score,
                    confidence = EXCLUDED.confidence,
                    total_input = EXCLUDED.total_input,
                    verified = EXCLUDED.verified,
                    verified_followers = EXCLUDED.verified_followers,
                    source_observer = EXCLUDED.source_observer,
                    computed_at = NOW()
                """,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
            )
        pg.commit()
        logger.info("House GrapeRank: stored %d public scores", len(values))
        return len(values)
    except Exception as e:
        logger.error(f"House GrapeRank write failed: {e}")
        try:
            pg.rollback()
        except Exception:
            pass
        return 0
    finally:
        put_pg(pg)


def refresh_house_graperank() -> int:
    """Import public Brainstorm wot_rank scores for current directory members."""
    if not BRAINSTORM_HOUSE_SCORE_ENABLED:
        return 0

    observer = configured_house_observer_pubkey()
    if not observer:
        logger.info("House GrapeRank: no house observer configured")
        return 0

    member_pks = get_directory_member_pubkeys()
    if not member_pks:
        logger.info("House GrapeRank: no directory members to score")
        return 0

    scores = _fetch_brainstorm_house_scores(member_pks, observer)
    if not scores:
        logger.warning("House GrapeRank: Brainstorm import returned no scores")
        return 0

    return store_house_graperank_scores(observer, scores, member_pks)


async def periodic_graperank():
    """Neo4j graph maintenance + batch GrapeRank + global consensus."""
    await asyncio.sleep(120)  # Initial delay — wait for Neo4j + GrapeRank Java to start
    last_full = 0
    last_graperank = 0
    last_consensus = 0
    last_house_graperank = 0

    # Check if Neo4j graph is empty — if so, do a full rebuild before first computation
    try:
        driver = get_neo4j()
        with driver.session() as session:
            result = session.run("MATCH (n:NostrUser) RETURN count(n) AS c")
            node_count = result.single()["c"]
        if node_count == 0:
            logger.info("GrapeRank: Neo4j graph is empty — triggering initial full rebuild")
            await asyncio.to_thread(neo4j_full_rebuild)
            last_full = time.time()
    except Exception as e:
        logger.error(f"GrapeRank: initial graph check failed: {e}")

    # Run initial batch GrapeRank + consensus on startup
    try:
        all_scores = await asyncio.to_thread(_run_batch_graperank_cycle)
        if all_scores:
            last_graperank = time.time()
            await publish_nip85_assertions(all_scores)
    except Exception as e:
        logger.error(f"GrapeRank: initial batch cycle failed: {e}")

    try:
        count = await asyncio.to_thread(_compute_global_consensus)
        if count > 0:
            last_consensus = time.time()
            logger.info(f"GrapeRank: initial global consensus computed ({count} scores)")
    except Exception as e:
        logger.error(f"GrapeRank: initial consensus failed: {e}")

    try:
        count = await asyncio.to_thread(refresh_house_graperank)
        if count > 0:
            last_house_graperank = time.time()
            logger.info(f"House GrapeRank: initial import complete ({count} scores)")
    except Exception as e:
        logger.error(f"House GrapeRank: initial import failed: {e}")

    while True:
        try:
            now = datetime.now(timezone.utc)

            # Full graph rebuild: Sunday 3am UTC
            if now.weekday() == FULL_RECOMPUTE_DAY and now.hour == 3 and time.time() - last_full > 82800:
                await asyncio.to_thread(neo4j_full_rebuild)
                last_full = time.time()
                _redis.delete("directory:dirty_pubkeys")
                logger.info("GrapeRank: Sunday full Neo4j rebuild complete")

            # Batch GrapeRank for all directory members every cycle
            if time.time() - last_graperank > GRAPERANK_INTERVAL - 60:
                all_scores = await asyncio.to_thread(_run_batch_graperank_cycle)
                if all_scores:
                    last_graperank = time.time()
                    await publish_nip85_assertions(all_scores)

            # Recompute global consensus every cycle (after GrapeRank)
            if time.time() - last_consensus > GRAPERANK_INTERVAL - 60:
                count = await asyncio.to_thread(_compute_global_consensus)
                if count > 0:
                    last_consensus = time.time()

            if time.time() - last_house_graperank > HOUSE_GRAPERANK_INTERVAL - 60:
                count = await asyncio.to_thread(refresh_house_graperank)
                if count > 0:
                    last_house_graperank = time.time()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"GrapeRank cycle failed: {e}")

        await asyncio.sleep(GRAPERANK_INTERVAL)


def _run_batch_graperank_cycle() -> dict:
    """Run batch GrapeRank for all directory members. Graph loads once.

    Returns {observer_pubkey: {target_pubkey: score}} for NIP-85 publishing.
    """
    member_pks = get_directory_member_pubkeys()
    if not member_pks:
        logger.info("GrapeRank batch cycle: no directory members")
        return {}

    observers = list(member_pks)
    logger.info(f"GrapeRank batch cycle: computing for {len(observers)} observers")

    batch_results = trigger_graperank_batch(observers)
    if not batch_results:
        return {}

    # Store scores for each observer
    all_scores = {}
    for observer, result_data in batch_results.items():
        scorecards = result_data.get("scorecards", {}) if isinstance(result_data, dict) else {}
        if not scorecards:
            continue
        scores = store_observer_scores(observer, scorecards, member_pks)
        if scores:
            all_scores[observer] = scores

    logger.info(f"GrapeRank batch cycle: stored scores for {len(all_scores)}/{len(observers)} observers")
    return all_scores


async def periodic_nip05():
    """Re-verify NIP-05 identifiers every 24 hours."""
    await asyncio.sleep(300)
    while True:
        try:
            await check_nip05_batch()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"NIP-05 check failed: {e}")
        await asyncio.sleep(NIP05_CHECK_INTERVAL)


async def main():
    global _redis, _neo4j_driver

    _redis = redis_lib.from_url(REDIS_URL, decode_responses=False)
    _redis.ping()
    logger.info("Connected to Redis")

    _init_pg_pool()
    logger.info("Connected to PostgreSQL (pool: min=1, max=4)")

    _neo4j_driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    _neo4j_driver.verify_connectivity()
    logger.info("Connected to Neo4j")

    # Ensure Neo4j index exists
    neo4j_ensure_index()

    # Create personalized_scores table if not exists
    pg = get_pg()
    if pg:
        try:
            with pg.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS personalized_scores (
                        observer_pubkey TEXT NOT NULL,
                        target_pubkey TEXT NOT NULL,
                        score REAL NOT NULL,
                        tier TEXT NOT NULL,
                        hops INTEGER DEFAULT 3,
                        computed_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (observer_pubkey, target_pubkey)
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_personalized_observer ON personalized_scores(observer_pubkey)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_personalized_target ON personalized_scores(target_pubkey)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS global_consensus_scores (
                        target_pubkey TEXT PRIMARY KEY,
                        score REAL NOT NULL DEFAULT 0,
                        tier TEXT NOT NULL DEFAULT 'unverified',
                        computed_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
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
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_house_graperank_score ON house_graperank_scores(score DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_house_graperank_source ON house_graperank_scores(source_observer)")
            pg.commit()
        except Exception as e:
            logger.warning(f"personalized_scores table setup: {e}")
            try:
                pg.rollback()
            except Exception:
                pass
        finally:
            put_pg(pg)

    tasks = [
        asyncio.create_task(tail_strfry(), name="strfry-tail"),
        asyncio.create_task(periodic_graperank(), name="graperank"),
        asyncio.create_task(periodic_nip05(), name="nip05-check"),
        asyncio.create_task(lazy_nip85_consumer(), name="nip85-lazy"),
    ]

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def handle_signal():
        stop_event.set()
        for t in tasks:
            t.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    if _neo4j_driver:
        _neo4j_driver.close()
    if _pg_pool:
        _pg_pool.closeall()
    logger.info("Indexer stopped")


if __name__ == "__main__":
    asyncio.run(main())
