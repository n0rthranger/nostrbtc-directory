"""Background directory indexer — pre-computes profiles, badges, and trust data.

Runs every INDEX_INTERVAL seconds. Fetches profiles from strfry, checks
NIP-05 resolution, Lightning reachability, computes trust graph, and writes
everything to the directory_profiles table for fast API reads.
"""

import asyncio
import ipaddress
import json
import logging
import math
import os
import re
import socket
import time
from datetime import datetime, timezone

import httpx
import websockets

import db
import discovery
import nostr_crypto
from url_safety import is_safe_domain as _is_safe_domain, resolve_domain_to_safe_ip as _resolve_domain_to_safe_ip

logger = logging.getLogger("nostrbtc.directory_indexer")

RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

INDEX_INTERVAL = 900  # 15 minutes
ACTIVE_WRITER_DAYS = 7
HTTP_TIMEOUT = 8
MAX_CONCURRENT_HTTP = 50
_http_sem = asyncio.Semaphore(MAX_CONCURRENT_HTTP)


class _CycleMetrics:
    """Accumulates metrics during a single index cycle."""
    __slots__ = ("relay_timeouts", "relay_errors", "nip05_checked",
                 "nip05_failed", "lightning_checked", "lightning_failed",
                 "profiles_refreshed", "external_relay_fetches")

    def __init__(self):
        self.relay_timeouts = 0
        self.relay_errors = 0
        self.nip05_checked = 0
        self.nip05_failed = 0
        self.lightning_checked = 0
        self.lightning_failed = 0
        self.profiles_refreshed = 0
        self.external_relay_fetches = 0

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}


# Per-cycle metrics instance, reset at start of each run_full_index
_metrics: _CycleMetrics | None = None


async def index_loop():
    """Main loop — runs forever, re-indexes every INDEX_INTERVAL seconds."""
    await asyncio.sleep(30)  # let strfry warm up
    while True:
        try:
            await run_full_index()
        except Exception:
            logger.exception("Directory index cycle failed")
        await asyncio.sleep(INDEX_INTERVAL)


async def index_single_pubkey(pubkey: str):
    """Quick index for a single pubkey — called on new subscription activation."""
    try:
        subscribers = db.get_directory_subscribers_full()
        sub = next((s for s in subscribers if s["pubkey"] == pubkey), None)
        if not sub:
            return

        profiles = await discovery.fetch_profiles([pubkey])
        profile = profiles.get(pubkey, {})

        nip05_name = sub.get("nip05_name")
        about = profile.get("about", "")
        if len(about) > 200:
            about = about[:197] + "..."

        # Fetch activity from strfry + external relays
        event_count = 0
        last_active = 0
        try:
            act = await _fetch_activity_batch([pubkey])
            act_data = act.get(pubkey, {})
            event_count = act_data.get("count", 0)
            last_active = act_data.get("last_ts", 0)
            if event_count == 0 or last_active == 0:
                ext = await _fetch_external_activity([pubkey])
                ext_data = ext.get(pubkey, {})
                if ext_data.get("last_ts", 0) > last_active:
                    last_active = ext_data["last_ts"]
                if ext_data.get("count", 0) > event_count:
                    event_count = ext_data["count"]
        except Exception:
            logger.debug(f"Activity fetch failed for quick-index {pubkey[:12]}...")

        row = {
            "pubkey": pubkey,
            "npub": sub["npub"],
            "name": profile.get("name", ""),
            "picture": profile.get("picture", ""),
            "nip05_display": "",
            "about": about,
            "lud16": profile.get("lud16", ""),
            "badges": json.dumps([]),
            "event_count": event_count,
            "last_active": last_active,
            "trust_count": 0,
            "subscription_created": sub.get("created_at", ""),
            "directory_tags": json.dumps(sub.get("tags", [])),
            "card_url": f"/p/{nip05_name}" if nip05_name else f"/p/{sub['npub']}",
            "indexed_at": int(time.time()),
            "reputation_score": 0,
        }
        db.bulk_upsert_directory_profiles([row])
        logger.info(f"Quick-indexed new subscriber: {profile.get('name', 'anon')} ({pubkey[:12]}...)")
    except Exception:
        logger.exception(f"Quick-index failed for {pubkey[:12]}...")


async def _fetch_kind9999_events():
    """Fetch raw kind 9999 events from strfry referencing our list header.

    Returns (list_header_id, events_list). Shared by _get_kind9999_members()
    and _sync_list_events() to avoid duplicate relay fetches.
    """
    list_header_id = db.get_relay_state("list_header_event_id")
    if not list_header_id:
        return "", []
    filt = {"kinds": [9999], "#z": [list_header_id], "limit": 500}
    events = await discovery._fetch_from_relay(discovery.STRFRY_URL, filt, timeout=10)
    return list_header_id, events


def _parse_kind9999_events(events, bot_pubkey=""):
    """Parse kind 9999 events into a members dict.

    Returns dict mapping pubkey -> {"event_id", "self_signed", "created_at"}.
    """
    members = {}
    for ev in events:
        p_tags = [t[1] for t in ev.get("tags", []) if len(t) >= 2 and t[0] == "p"]
        if len(p_tags) != 1:
            continue
        pk = p_tags[0]
        ts = ev.get("created_at", 0)
        if pk not in members or ts > members[pk]["created_at"]:
            members[pk] = {
                "event_id": ev["id"],
                "self_signed": ev.get("pubkey", "") != bot_pubkey,
                "created_at": ts,
            }
    return members


def _get_bot_pubkey():
    """Get the relay bot public key (safe import)."""
    try:
        import nostr_auth
        return nostr_auth.RELAY_PUBLIC_KEY
    except Exception:
        return ""


async def _get_kind9999_members(*, _prefetched=None):
    """Scan strfry for kind 9999 events referencing our list header.

    Returns dict mapping pubkey -> {"event_id", "self_signed", "created_at"}.
    Pass *_prefetched=(list_header_id, events)* to reuse an earlier fetch.
    """
    if _prefetched:
        list_header_id, events = _prefetched
    else:
        list_header_id, events = await _fetch_kind9999_events()
    if not list_header_id:
        return {}

    bot_pubkey = _get_bot_pubkey()
    members = _parse_kind9999_events(events, bot_pubkey)
    logger.info(f"Kind 9999 scan: {len(members)} members from strfry")
    return members


async def _sync_list_events(pubkeys: list[str], rows: list[dict], *, _prefetched=None):
    """Sync kind 9999 decentralized list events with Postgres.

    - Checks which members have kind 9999 events in strfry
    - Self-heals by publishing missing events
    - Detects user-signed listings and marks them in Postgres

    Pass *_prefetched=(list_header_id, events)* to reuse an earlier fetch.
    """
    if _prefetched:
        list_header_id, events = _prefetched
    else:
        list_header_id, events = await _fetch_kind9999_events()
    if not list_header_id:
        return

    import decentralized_list

    bot_pubkey = _get_bot_pubkey()
    parsed = _parse_kind9999_events(events, bot_pubkey)

    # Build maps from parsed data
    event_map = {pk: v["event_id"] for pk, v in parsed.items()}
    self_signed = {pk: v["self_signed"] for pk, v in parsed.items()}

    # Update Postgres with event ids and self_signed flags
    pubkey_set = set(pubkeys)
    synced = 0
    healed = 0
    for pk in pubkeys:
        if pk in event_map:
            db.set_list_event_id(pk, event_map[pk])
            if self_signed.get(pk, False):
                db.set_self_signed(pk, True)
            synced += 1
        else:
            # Self-heal: publish missing kind 9999
            row = next((r for r in rows if r["pubkey"] == pk), None)
            if row:
                try:
                    event_id = await decentralized_list.publish_member_item(
                        pk, row.get("name", ""), row.get("nip05_display", ""),
                        list_header_id)
                    if event_id:
                        db.set_list_event_id(pk, event_id)
                        healed += 1
                except Exception:
                    logger.debug(f"Self-heal failed for {pk[:12]}...")

    logger.info(f"List sync: {synced} synced, {healed} self-healed, "
                f"{sum(1 for v in self_signed.values() if v)} user-signed")


async def run_full_index():
    """Single index cycle — fetches profiles, badges, and computes final reputation.

    This indexer handles profile fetching, badge verification, activity scoring,
    identity scoring, and combining them into the final 0-100 reputation_score.
    Trust ranking is handled separately via personalized_scores (per-observer GrapeRank).
    """
    global _metrics
    _metrics = _CycleMetrics()
    started = time.time()

    # Postgres active subscriptions are authoritative for membership. Kind 9999
    # events are public attestations only; conflicts are logged and re-published
    # from Postgres on the next self-heal pass.
    # Fetch kind 9999 events once and share with both _get_kind9999_members and _sync_list_events.
    _kind9999_prefetched = await _fetch_kind9999_events()
    kind9999_members = await _get_kind9999_members(_prefetched=_kind9999_prefetched)
    pg_subscribers = db.get_directory_subscribers_full()
    pg_map = {s["pubkey"]: s for s in pg_subscribers}

    pg_member_pks = set(pg_map.keys())
    kind9999_pks = set(kind9999_members.keys())
    extra_attestations = kind9999_pks - pg_member_pks
    missing_attestations = pg_member_pks - kind9999_pks
    if extra_attestations:
        logger.warning("Directory membership conflict: %d kind-9999-only attestations ignored",
                       len(extra_attestations))
    if missing_attestations:
        logger.info("Directory membership: %d Postgres members missing kind-9999 attestation",
                    len(missing_attestations))
    all_member_pks = pg_member_pks

    if not all_member_pks:
        db.remove_stale_directory_profiles(set())
        logger.info("Directory indexed: 0 members (none listed)")
        return

    # Populate Redis directory_members SET (the authoritative set)
    db.populate_directory_members_redis(all_member_pks)

    # Build subscribers list — prefer Postgres data when available
    subscribers = []
    for pk in all_member_pks:
        if pk in pg_map:
            subscribers.append(pg_map[pk])
        else:
            # Kind-9999-only member (not in Postgres) — synthesize minimal entry
            subscribers.append({"pubkey": pk, "npub": "", "nip05_name": None,
                               "tags": [], "plan": "directory", "created_at": ""})

    logger.info(f"Directory members: {len(all_member_pks)} authoritative Postgres members "
                f"({len(kind9999_members)} public kind-9999 attestations observed)")

    pubkeys = [s["pubkey"] for s in subscribers]
    pubkey_set = set(pubkeys)

    # Step 1: Fetch profiles from local strfry + public relays (kind 0)
    profiles = await discovery.fetch_profiles(pubkeys)

    # Step 2: Fetch all data sources in parallel
    activity_task = _fetch_activity_batch(pubkeys)
    heatmap_task = _fetch_activity_heatmap(pubkeys)
    zap_task = _fetch_zaps(pubkeys)
    nip05_task = _check_nip05_batch(subscribers, profiles)
    lightning_task = _check_lightning_batch(profiles)

    results = await asyncio.gather(
        activity_task, heatmap_task, zap_task, nip05_task, lightning_task,
        return_exceptions=True,
    )

    activity = results[0] if not isinstance(results[0], Exception) else {}

    # Backfill activity from external relays for users with zero strfry activity
    # OR stale last_active (> 2 days old) — they may be active on other relays.
    # Capped at 50 per cycle, prioritizing zero-activity then stalest.
    _EXT_BACKFILL_MAX = 50
    if isinstance(activity, dict):
        stale_cutoff = int(time.time()) - 172800  # 2 days
        need_external = [s["pubkey"] for s in subscribers
                         if activity.get(s["pubkey"], {}).get("count", 0) == 0
                         or activity.get(s["pubkey"], {}).get("last_ts", 0) < stale_cutoff]
        # Prioritize: zero-activity first, then stalest
        need_external.sort(key=lambda pk: activity.get(pk, {}).get("last_ts", 0))
        need_external = need_external[:_EXT_BACKFILL_MAX]
        if need_external:
            try:
                ext_activity = await _fetch_external_activity(need_external)
                for pk, data in ext_activity.items():
                    if data["last_ts"] > activity.get(pk, {}).get("last_ts", 0):
                        activity[pk]["last_ts"] = data["last_ts"]
                    if data["count"] > activity.get(pk, {}).get("count", 0):
                        activity[pk]["count"] = data["count"]
            except Exception as e:
                logger.error(f"External activity backfill failed: {e}")

    heatmap_rows = results[1] if not isinstance(results[1], Exception) else []
    zap_rows = results[2] if not isinstance(results[2], Exception) else []
    nip05_results = results[3] if not isinstance(results[3], Exception) else {}
    lightning_results = results[4] if not isinstance(results[4], Exception) else {}

    # Log any exceptions
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            labels = ["activity", "heatmap", "zaps", "nip05", "lightning"]
            logger.error(f"Step 2 ({labels[i]}) failed: {r}")

    # Step 3: Persist heatmap and zaps
    if isinstance(heatmap_rows, list) and heatmap_rows:
        db.bulk_upsert_activity_heatmap(heatmap_rows)
        db.cleanup_old_heatmap(400)
        logger.info(f"Heatmap: {len(heatmap_rows)} day-entries written")
    if isinstance(zap_rows, list) and zap_rows:
        db.bulk_upsert_zaps(zap_rows)
        db.cleanup_old_zaps(7776000)
        logger.info(f"Zaps: {len(zap_rows)} zap records written")

    # Step 4: Read trust counts (follower edges within member set)
    trust_counts = _read_trust_counts(pubkeys)

    # Get heatmap data from DB for activity scoring
    heatmap_data = db.get_all_heatmap_data(pubkeys, days=90)

    # Step 5: Compute per-member scores and assemble rows
    rows = []
    for s in subscribers:
        pk = s["pubkey"]
        profile = profiles.get(pk, {})
        act = activity.get(pk, {}) if isinstance(activity, dict) else {}

        nip05_info = nip05_results.get(pk, {"verified": False, "display": ""}) if isinstance(nip05_results, dict) else {"verified": False, "display": ""}
        nip05_ok = nip05_info["verified"]
        nip05_display = nip05_info["display"]
        lightning_ok = lightning_results.get(pk, False) if isinstance(lightning_results, dict) else False

        badges = _compute_badges(s, profile, act, 0, nip05_ok, lightning_ok)

        activity_score = _compute_activity_score(act, heatmap_data.get(pk, {}))
        identity_score = _compute_identity_score(s, nip05_ok, lightning_ok, nip05_display)
        rep_score = _compute_profile_score(activity_score, identity_score)

        nip05_name = s.get("nip05_name")
        picture = profile.get("picture", "")
        about = profile.get("about", "")
        if len(about) > 200:
            about = about[:197] + "..."

        rows.append({
            "pubkey": pk,
            "npub": s["npub"],
            "name": profile.get("name", ""),
            "picture": picture,
            "nip05_display": nip05_display,
            "about": about,
            "lud16": profile.get("lud16", ""),
            "badges": json.dumps(badges),
            "event_count": act.get("count", 0),
            "last_active": act.get("last_ts", 0),
            "trust_count": trust_counts.get(pk, 0),
            "subscription_created": s.get("created_at", ""),
            "directory_tags": json.dumps(s.get("tags", [])),
            "card_url": f"/p/{nip05_name}" if nip05_name else f"/p/{s['npub']}",
            "indexed_at": int(time.time()),
            "reputation_score": rep_score,
            "activity_score": round(activity_score, 4),
            "identity_score": round(identity_score, 4),
        })

    db.bulk_upsert_directory_profiles(rows)
    db.remove_stale_directory_profiles(pubkey_set)

    # Publish attestation events (non-blocking, errors logged internally)
    try:
        import attestation
        await attestation.publish_attestations()
    except Exception:
        logger.exception("Attestation publishing failed")

    # Sync decentralized list events (kind 9999) with Postgres
    try:
        await _sync_list_events(pubkeys, rows, _prefetched=_kind9999_prefetched)
    except Exception:
        logger.exception("Decentralized list sync failed")

    # Save daily trust snapshots (once per day)
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        snap_rows = []
        for r in rows:
            snap_rows.append({
                "pubkey": r["pubkey"],
                "snapshot_date": today,
                "reputation_score": r["reputation_score"],
                "trust_score": 0,  # updated by GrapeRank separately
                "followers_count": r.get("trust_count", 0),
            })
        db.save_trust_snapshots(snap_rows)
    except Exception:
        logger.exception("Trust snapshot save failed")

    # Compute trust clusters
    try:
        import clustering
        member_info = [{
            "pubkey": r["pubkey"],
            "name": r["name"],
            "about": r.get("about", ""),
            "tags": r.get("directory_tags", "[]"),
            "trust_count": r.get("trust_count", 0),
        } for r in rows]
        edges = db.get_trust_edges()
        rep_scores = {r["pubkey"]: r.get("reputation_score", 0) for r in rows}
        assignments = clustering.detect_clusters([r["pubkey"] for r in rows], edges, reputation=rep_scores)
        cluster_meta = clustering.generate_cluster_labels(assignments, member_info)
        db.save_cluster_results(assignments, cluster_meta)
        n_clusters = len(cluster_meta)
        if n_clusters:
            logger.info(f"Clusters: {n_clusters} detected")
    except Exception:
        logger.exception("Cluster computation failed")

    elapsed = time.time() - started

    # --- Cycle metrics ---
    _metrics.profiles_refreshed = len(rows)
    m = _metrics.to_dict()
    m["elapsed_s"] = round(elapsed, 1)
    m["members"] = len(rows)

    logger.info(
        f"Directory indexed: {len(rows)} members in {elapsed:.1f}s | "
        f"relay_timeouts={m['relay_timeouts']} relay_errors={m['relay_errors']} "
        f"nip05_ok={m['nip05_checked'] - m['nip05_failed']}/{m['nip05_checked']} "
        f"lightning_ok={m['lightning_checked'] - m['lightning_failed']}/{m['lightning_checked']} "
        f"external_fetches={m['external_relay_fetches']}"
    )

    try:
        db.log_activation("system", "directory_indexed", m)
    except Exception:
        logger.debug("Failed to write index metrics to activation_log")


# ---------------------------------------------------------------------------
# Activity: recent events per pubkey from strfry
# ---------------------------------------------------------------------------

async def _fetch_activity_batch(pubkeys):
    """Fetch event counts and last activity for all pubkeys in a single connection.

    Uses one big REQ with all authors + since filter, then counts per pubkey in Python.
    This is much faster than per-pubkey queries for large member counts.
    Returns {pubkey: {"count": int, "last_ts": int, "kind_count": int}}
    """
    results = {pk: {"count": 0, "last_ts": 0, "kind_count": 0} for pk in pubkeys}
    kind_sets = {pk: set() for pk in pubkeys}
    if not pubkeys:
        return results

    since = int(time.time()) - (ACTIVE_WRITER_DAYS * 86400)

    try:
        async with websockets.connect(
            discovery.STRFRY_URL, open_timeout=5, close_timeout=5
        ) as ws:
            sub_id = f"dir-act-{int(time.time()) % 10000}"
            await ws.send(json.dumps(["REQ", sub_id, {
                "authors": pubkeys,
                "since": since,
                "limit": 50000,
            }]))

            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=15)
                except asyncio.TimeoutError:
                    if _metrics:
                        _metrics.relay_timeouts += 1
                    break
                data = json.loads(msg)
                if data[0] == "EVENT" and data[1] == sub_id:
                    ev = data[2]
                    pk = ev.get("pubkey", "")
                    if pk in results:
                        results[pk]["count"] += 1
                        ts = ev.get("created_at", 0)
                        if ts > results[pk]["last_ts"]:
                            results[pk]["last_ts"] = ts
                        kind_sets[pk].add(ev.get("kind", 0))
                elif data[0] == "EOSE":
                    break

            await ws.send(json.dumps(["CLOSE", sub_id]))
    except Exception as e:
        if _metrics:
            _metrics.relay_errors += 1
        logger.error(f"Activity batch fetch failed: {e}")

    for pk in pubkeys:
        results[pk]["kind_count"] = len(kind_sets.get(pk, set()))

    return results


from relay_constants import PUBLIC_RELAYS as EXTERNAL_RELAYS


_EXT_KINDS = [1, 6, 7, 9735, 30023]
_EXT_CHUNK_SIZE = 8   # authors per count query — keeps results under relay caps
_EXT_MAX_LATEST = 50  # max authors to query for last_active per cycle


async def _ext_latest_per_author(ws, pubkeys, results):
    """Fetch the most recent event per author via individual limit-1 queries.

    One REQ per author with limit=1 guarantees the relay returns that author's
    newest event regardless of how active other authors are. Uses a single
    WebSocket connection for all queries (sequential, fast).
    Capped at _EXT_MAX_LATEST per relay to stay scalable at thousands of members.
    """
    recent_cutoff = int(time.time()) - (7 * 86400)  # skip if found within 7 days
    queried = 0
    for pk in pubkeys:
        if results[pk]["last_ts"] > recent_cutoff:
            continue  # recent enough from a previous relay
        if queried >= _EXT_MAX_LATEST:
            break
        queried += 1
        sub_id = f"ext-l-{int(time.time()) % 10000}"
        await ws.send(json.dumps(["REQ", sub_id, {
            "authors": [pk],
            "kinds": _EXT_KINDS,
            "limit": 1,
        }]))
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                break
            data = json.loads(msg)
            if data[0] == "EVENT" and data[1] == sub_id:
                ts = data[2].get("created_at", 0)
                if ts > results[pk]["last_ts"]:
                    results[pk]["last_ts"] = ts
            elif data[0] == "EOSE":
                break
        try:
            await ws.send(json.dumps(["CLOSE", sub_id]))
        except Exception:
            pass


async def _ext_counts_chunked(ws, pubkeys, results):
    """Fetch 30-day event counts in chunked batches."""
    since_30d = int(time.time()) - (30 * 86400)
    chunks = [pubkeys[i:i + _EXT_CHUNK_SIZE]
              for i in range(0, len(pubkeys), _EXT_CHUNK_SIZE)]

    for chunk in chunks:
        sub_id = f"ext-c-{int(time.time()) % 10000}"
        await ws.send(json.dumps(["REQ", sub_id, {
            "authors": chunk,
            "kinds": _EXT_KINDS,
            "since": since_30d,
            "limit": len(chunk) * 50,
        }]))
        relay_counts = {pk: 0 for pk in chunk}
        while True:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                if _metrics:
                    _metrics.relay_timeouts += 1
                break
            data = json.loads(msg)
            if data[0] == "EVENT" and data[1] == sub_id:
                pk = data[2].get("pubkey", "")
                if pk in relay_counts:
                    relay_counts[pk] += 1
            elif data[0] == "EOSE":
                break
        try:
            await ws.send(json.dumps(["CLOSE", sub_id]))
        except Exception:
            pass
        for pk in chunk:
            if relay_counts[pk] > results[pk]["count"]:
                results[pk]["count"] = relay_counts[pk]


async def _fetch_external_activity(pubkeys):
    """Fetch activity from public relays.

    Strategy:
    - last_active: one limit=1 query per author (guarantees result regardless of
      other authors' activity). Skips authors already found on a previous relay.
    - event_count: chunked batch queries (8 authors per chunk) for 30-day counts.
    """
    if not pubkeys:
        return {}

    results = {pk: {"count": 0, "last_ts": 0, "kind_count": 0} for pk in pubkeys}

    for relay_url in EXTERNAL_RELAYS:
        if _metrics:
            _metrics.external_relay_fetches += 1
        try:
            async with websockets.connect(
                relay_url, open_timeout=5, close_timeout=5
            ) as ws:
                await _ext_latest_per_author(ws, pubkeys, results)
                await _ext_counts_chunked(ws, pubkeys, results)
        except Exception as e:
            if _metrics:
                _metrics.relay_errors += 1
            logger.warning(f"External activity fetch from {relay_url} failed: {e}")

    for pk in pubkeys:
        if results[pk]["count"] > 0:
            results[pk]["kind_count"] = 1

    logger.info(f"External activity: {sum(1 for pk in pubkeys if results[pk]['count'] > 0)}/{len(pubkeys)} directory-only users with recent activity")
    return results


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# NIP-05 verification
# ---------------------------------------------------------------------------

async def _check_nip05_single(nip05_name, expected_pubkey):
    """Verify a local NIP-05 resolves to the expected pubkey."""
    async with _http_sem:
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                r = await client.get(
                    f"https://{RELAY_DOMAIN}/.well-known/nostr.json?name={nip05_name}"
                )
                if r.status_code == 200:
                    data = r.json()
                    return data.get("names", {}).get(nip05_name) == expected_pubkey
        except Exception:
            pass
    return False


async def _check_nip05_external(nip05_addr, expected_pubkey):
    """Verify any NIP-05 address (user@domain) resolves to the expected pubkey."""
    if not nip05_addr or "@" not in nip05_addr:
        return False
    parts = nip05_addr.split("@", 1)
    if len(parts) != 2:
        return False
    name, domain = parts[0].strip(), parts[1].strip().lower()
    if not name or not domain:
        return False
    # Skip our own domain — handled by _check_nip05_single
    if domain == RELAY_DOMAIN:
        return False
    safe_ip = _resolve_domain_to_safe_ip(domain)
    if not safe_ip:
        return False
    async with _http_sem:
        try:
            is_ipv6 = ":" in safe_ip
            host = f"[{safe_ip}]" if is_ipv6 else safe_ip
            url = f"https://{host}/.well-known/nostr.json?name={name}"
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=False,
                                         headers={"Host": domain}) as client:
                r = await client.get(url, extensions={"sni_hostname": domain.encode()})
                if r.status_code == 200:
                    data = r.json()
                    return data.get("names", {}).get(name) == expected_pubkey
        except Exception:
            pass
    return False


async def _check_nip05_batch(subscribers, profiles):
    """Check NIP-05 for all members. Checks local domain first, then external.
    Returns {pubkey: {"verified": bool, "display": str}}."""
    local_tasks = {}
    external_tasks = {}

    for s in subscribers:
        pk = s["pubkey"]
        name = s.get("nip05_name")
        if name:
            local_tasks[pk] = _check_nip05_single(name, pk)

        profile = profiles.get(pk, {})
        ext_nip05 = profile.get("nip05", "")
        if ext_nip05 and "@" in ext_nip05:
            external_tasks[pk] = _check_nip05_external(ext_nip05, pk)

    local_results = {}
    if local_tasks:
        results = await asyncio.gather(*local_tasks.values(), return_exceptions=True)
        for pk, result in zip(local_tasks.keys(), results):
            local_results[pk] = result is True
            if _metrics:
                _metrics.nip05_checked += 1
                if result is not True:
                    _metrics.nip05_failed += 1

    ext_results = {}
    if external_tasks:
        results = await asyncio.gather(*external_tasks.values(), return_exceptions=True)
        for pk, result in zip(external_tasks.keys(), results):
            ext_results[pk] = result is True
            if _metrics:
                _metrics.nip05_checked += 1
                if result is not True:
                    _metrics.nip05_failed += 1

    # Merge: local wins, but external counts too
    combined = {}
    for s in subscribers:
        pk = s["pubkey"]
        nip05_name = s.get("nip05_name")
        profile = profiles.get(pk, {})
        ext_nip05 = profile.get("nip05", "")

        if local_results.get(pk):
            combined[pk] = {"verified": True, "display": f"{nip05_name}@{RELAY_DOMAIN}"}
        elif ext_results.get(pk):
            combined[pk] = {"verified": True, "display": ext_nip05}
        elif nip05_name:
            # Has local name but verification failed — still show it
            combined[pk] = {"verified": False, "display": f"{nip05_name}@{RELAY_DOMAIN}"}
        elif ext_nip05:
            combined[pk] = {"verified": False, "display": ext_nip05}
        else:
            combined[pk] = {"verified": False, "display": ""}

    return combined


# ---------------------------------------------------------------------------
# Lightning reachability
# ---------------------------------------------------------------------------

async def _check_lightning_single(lud16):
    """Check if a Lightning address responds with a valid LNURL callback."""
    if not lud16 or "@" not in lud16:
        return False
    try:
        name, domain = lud16.split("@", 1)
    except ValueError:
        return False
    safe_ip = _resolve_domain_to_safe_ip(domain)
    if not safe_ip:
        return False
    async with _http_sem:
        try:
            is_ipv6 = ":" in safe_ip
            host = f"[{safe_ip}]" if is_ipv6 else safe_ip
            url = f"https://{host}/.well-known/lnurlp/{name}"
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=False,
                                         headers={"Host": domain}) as client:
                r = await client.get(url, extensions={"sni_hostname": domain.encode()})
                if r.status_code == 200:
                    data = r.json()
                    return "callback" in data
        except Exception:
            pass
    return False


async def _check_lightning_batch(profiles):
    """Check Lightning for all profiles with lud16. Returns {pubkey: bool}."""
    tasks = {}
    for pk, prof in profiles.items():
        lud16 = prof.get("lud16", "")
        if lud16 and "@" in lud16 and not lud16.endswith("@npub.cash"):
            tasks[pk] = _check_lightning_single(lud16)

    results = {}
    if tasks:
        task_results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for pk, result in zip(tasks.keys(), task_results):
            results[pk] = result is True
            if _metrics:
                _metrics.lightning_checked += 1
                if result is not True:
                    _metrics.lightning_failed += 1
    return results


# ---------------------------------------------------------------------------
# Badge computation
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Activity heatmap: daily event counts over 365 days
# ---------------------------------------------------------------------------

async def _fetch_activity_heatmap(pubkeys):
    """Fetch daily event counts for all pubkeys over the past 365 days.

    Returns list of {"pubkey", "day", "event_count"} dicts for bulk upsert.
    """
    if not pubkeys:
        return []

    since = int(time.time()) - (365 * 86400)
    day_counts = {}  # (pubkey, day_str) -> count

    try:
        async with websockets.connect(
            discovery.STRFRY_URL, open_timeout=5, close_timeout=5
        ) as ws:
            sub_id = f"dir-hm-{int(time.time()) % 10000}"
            # Fetch in chunks to avoid huge single requests
            chunk_size = 50
            for i in range(0, len(pubkeys), chunk_size):
                chunk = pubkeys[i:i + chunk_size]
                heatmap_limit = int(os.environ.get("HEATMAP_FETCH_LIMIT", "500000"))
                await ws.send(json.dumps(["REQ", sub_id, {
                    "authors": chunk,
                    "since": since,
                    "limit": heatmap_limit,
                }]))

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        if _metrics:
                            _metrics.relay_timeouts += 1
                        break
                    data = json.loads(msg)
                    if data[0] == "EVENT" and data[1] == sub_id:
                        ev = data[2]
                        pk = ev.get("pubkey", "")
                        ts = ev.get("created_at", 0)
                        if pk and ts > 0:
                            day_str = time.strftime("%Y-%m-%d", time.gmtime(ts))
                            key = (pk, day_str)
                            day_counts[key] = day_counts.get(key, 0) + 1
                    elif data[0] == "EOSE":
                        break

                await ws.send(json.dumps(["CLOSE", sub_id]))
    except Exception as e:
        if _metrics:
            _metrics.relay_errors += 1
        logger.error(f"Heatmap fetch failed: {e}")

    return [{"pubkey": k[0], "day": k[1], "event_count": v} for k, v in day_counts.items()]


# ---------------------------------------------------------------------------
# Zap flow: NIP-57 kind 9735 receipts between directory members
# ---------------------------------------------------------------------------

async def _fetch_zaps(pubkeys):
    """Fetch kind 9735 zap receipts involving directory members.

    Parses NIP-57 structure to extract sender, receiver, and amount.
    Returns list of dicts for bulk_upsert_zaps.
    """
    if not pubkeys:
        return []

    pubkey_set = set(pubkeys)
    since = int(time.time()) - (90 * 86400)  # 90 days
    zap_rows = []

    try:
        zap_events = await discovery._fetch_from_relay(
            discovery.STRFRY_URL,
            {"kinds": [9735], "#p": pubkeys, "since": since, "limit": 50000},
            timeout=30,
        )
    except Exception as e:
        logger.error(f"Zap fetch failed: {e}")
        return []

    for ev in zap_events:
        try:
            event_id = ev.get("id", "")
            created_at = ev.get("created_at", 0)
            if not event_id or not created_at:
                continue

            # Receiver from p tag
            receiver = None
            for tag in ev.get("tags", []):
                if len(tag) >= 2 and tag[0] == "p":
                    receiver = tag[1]
                    break
            if not receiver or receiver not in pubkey_set:
                continue

            # Parse the embedded kind 9734 request from description tag
            description = None
            for tag in ev.get("tags", []):
                if len(tag) >= 2 and tag[0] == "description":
                    description = tag[1]
                    break
            if not description:
                continue

            try:
                zap_request = json.loads(description)
            except (json.JSONDecodeError, TypeError):
                continue

            # Verify embedded kind 9734 signature to prevent zap spoofing
            if not nostr_crypto.verify_event(zap_request):
                continue

            sender = zap_request.get("pubkey", "")
            if not sender or sender not in pubkey_set:
                continue
            if sender == receiver:
                continue

            # Extract amount from the zap request's amount tag (in msats)
            amount_msats = 0
            for tag in zap_request.get("tags", []):
                if len(tag) >= 2 and tag[0] == "amount":
                    try:
                        amount_msats = int(tag[1])
                    except (ValueError, TypeError):
                        pass
                    break

            bolt11_amount_msats = 0
            for tag in ev.get("tags", []):
                if len(tag) >= 2 and tag[0] == "bolt11":
                    bolt11_amount_msats = _parse_bolt11_amount(tag[1])
                    break

            if bolt11_amount_msats <= 0:
                continue
            if amount_msats <= 0:
                amount_msats = bolt11_amount_msats
            if amount_msats != bolt11_amount_msats:
                logger.warning("Zap receipt amount mismatch for %s", event_id[:16])
                continue

            if amount_msats <= 0:
                continue

            zap_rows.append({
                "event_id": event_id,
                "sender": sender,
                "receiver": receiver,
                "amount_msats": amount_msats,
                "created_at": created_at,
            })
        except Exception:
            continue

    logger.info(f"Parsed {len(zap_rows)} zaps between directory members")
    return zap_rows




def _parse_bolt11_amount(bolt11):
    """Extract amount in msats from a BOLT11 invoice string.

    BOLT11 format: ln{bc|tb}[amount][multiplier]1[data]
    Multipliers: m=milli(0.001), u=micro(0.000001), n=nano(0.000000001), p=pico
    """
    import re
    if not bolt11:
        return 0
    bolt11 = bolt11.lower().strip()
    match = re.match(r'^ln(?:bc|tb|tbs)(\d+)([munp])?1', bolt11)
    if not match:
        return 0
    amount_str = match.group(1)
    multiplier = match.group(2) or ''
    try:
        amount = int(amount_str)
    except ValueError:
        return 0
    # Convert to msats (1 BTC = 100_000_000_000 msats)
    BTC_TO_MSATS = 100_000_000_000
    if multiplier == 'm':
        return amount * (BTC_TO_MSATS // 1000)
    elif multiplier == 'u':
        return amount * (BTC_TO_MSATS // 1_000_000)
    elif multiplier == 'n':
        return amount * (BTC_TO_MSATS // 1_000_000_000)
    elif multiplier == 'p':
        # 1 pico-BTC = 0.1 msats; amount in pico-BTC → msats
        return amount // 10
    else:
        return amount * BTC_TO_MSATS


# ---------------------------------------------------------------------------
# Reputation Scoring
# ---------------------------------------------------------------------------
# Architecture:
#   EigenTrust (60%): Computed by indexer/worker.py, read from Postgres
#   Identity (25%): Bayesian Beta distribution from NIP-05, Lightning, age
#   Activity (15%): Multiplier only — high activity + zero trust = bot, not reward
#
# Final: (eigen^0.60 * identity^0.25 * activity^0.15) → exponential CDF → 0-100
# ---------------------------------------------------------------------------

# Lambda for exponential CDF mapping — lower = more spread across the 0-100 range.
# 4.0 saturated too fast (small community → everyone scored 90+). 2.0 spreads better.
SCORE_LAMBDA = 2.0


def _read_trust_counts(pubkeys):
    """Read follower counts from Postgres trust_edges table.

    Returns {pubkey: int count of followers in member set}.
    """
    if not pubkeys:
        return {}

    pg = db._get_pg()
    if not pg:
        return {pk: 0 for pk in pubkeys}

    try:
        import psycopg2.extras
        with pg.cursor() as cur:
            placeholders = ",".join(["%s"] * len(pubkeys))
            cur.execute(
                f"SELECT target_pubkey, COUNT(*) FROM trust_edges "
                f"WHERE edge_type = 'follow' "
                f"AND target_pubkey IN ({placeholders}) "
                f"AND source_pubkey IN ({placeholders}) "
                f"GROUP BY target_pubkey",
                pubkeys + pubkeys,
            )
            rows = cur.fetchall()
        counts = {r[0]: r[1] for r in rows}
        for pk in pubkeys:
            if pk not in counts:
                counts[pk] = 0
        return counts
    except Exception as e:
        logger.error(f"Failed to read trust counts: {e}")
        return {pk: 0 for pk in pubkeys}


def _compute_activity_score(activity, heatmap_days):
    """Compute activity quality score 0-1 using entropy-based consistency + diversity.

    Used as a MULTIPLIER (15% weight) — can boost a trusted pubkey but cannot
    carry a zero-trust pubkey. A bot posting consistently with zero EigenTrust
    still scores near zero.
    """
    if not heatmap_days or activity.get("count", 0) == 0:
        return 0.0

    # --- Consistency: Shannon entropy of temporal distribution ---
    week_counts = [0] * 13
    now_ts = time.time()
    for day_str, count in heatmap_days.items():
        try:
            day_ts = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
        age_days = (now_ts - day_ts) / 86400
        if age_days < 0 or age_days > 91:
            continue
        week_idx = min(12, int(age_days / 7))
        week_counts[week_idx] += count

    total = sum(week_counts)
    if total == 0:
        return 0.0

    H = 0.0
    for c in week_counts:
        if c > 0:
            p = c / total
            H -= p * math.log2(p)
    H_max = math.log2(13)
    consistency = H / H_max if H_max > 0 else 0.0

    kind_count = activity.get("kind_count", 0)
    diversity = min(1.0, kind_count / 5.0)

    volume = min(1.0, math.log2(1 + total) / 10.0)

    return 0.5 * consistency + 0.3 * diversity + 0.2 * volume


def _compute_identity_score(subscriber, nip05_live, lightning_ok, nip05_display):
    """Compute identity confidence score 0-1 using Bayesian Beta distribution."""
    alpha = 1.0
    beta = 1.0

    if nip05_live:
        alpha += 3.0
        if nip05_display:
            domain = nip05_display.split("@")[-1] if "@" in nip05_display else ""
            shared_providers = {RELAY_DOMAIN, "nostr.com", "getalby.com", "primal.net",
                                "iris.to", "snort.social", "current.fyi", "nostr.land"}
            if domain and domain not in shared_providers:
                alpha += 2.0
            else:
                alpha += 1.0
    else:
        beta += 1.0

    if lightning_ok:
        alpha += 2.0
    else:
        beta += 0.5

    created = subscriber.get("created_at", "")
    if created:
        try:
            age_days = (datetime.now(timezone.utc) - datetime.fromisoformat(created).replace(tzinfo=timezone.utc)).days
            alpha += min(4.0, age_days / 90.0)
        except Exception:
            pass

    return alpha / (alpha + beta)


def _compute_profile_score(activity_score, identity_score):
    """Compute profile completeness score 0-100 from identity and activity.

    This is NOT a trust score. Trust ranking comes from personalized GrapeRank
    scores in the personalized_scores table. This score reflects profile quality
    (verified NIP-05, lightning, account age, posting activity).
    """
    eps = 0.01
    a = max(eps, activity_score)
    i = max(eps, identity_score)

    # Weighted geometric mean: identity 65%, activity 35%
    R = (i ** 0.65) * (a ** 0.35)

    score = 100.0 * (1.0 - math.exp(-SCORE_LAMBDA * R))

    return min(100, max(0, int(round(score))))


def _compute_badges(subscriber, profile, activity, trust_count, nip05_live, lightning_ok):
    """Compute the badge list for a directory member."""
    badges = []

    if subscriber.get("plan") in ("monthly", "annual"):
        badges.append("relay-subscriber")

    if nip05_live:
        badges.append("nip05-live")

    if lightning_ok:
        badges.append("lightning-reachable")

    return badges
