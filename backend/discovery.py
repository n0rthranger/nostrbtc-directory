"""Nostr public key data discovery.

Smart relay discovery: reads a user's kind 10002 (relay list) to find where
their data actually lives, then syncs from those relays + well-known ones.
"""

import asyncio
import json
import logging
import secrets
import time

import websockets

import os

import nostr_crypto

logger = logging.getLogger("nostrbtc.discovery")

RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

from relay_constants import PUBLIC_RELAYS

# Well-known relays to bootstrap discovery (kind 10002 lookup)
BOOTSTRAP_RELAYS = ["wss://purplepag.es"] + PUBLIC_RELAYS + ["wss://offchain.pub"]

# Large relays with best data coverage — always queried for author events
# Ranked by verified event counts (tested March 2026)
ESSENTIAL_RELAYS = PUBLIC_RELAYS + [
    "wss://search.nos.today",       # search/index relay, good coverage
    "wss://offchain.pub",           # solid strfry relay
    "wss://nostr.mom",              # good interactions coverage
    "wss://relay.nostr.net",        # public strfry relay
]

STRFRY_URL = "ws://strfry:7777"
CONNECT_TIMEOUT = 5
RECV_TIMEOUT = 15

# Relays for fetching reactions/zaps/reposts (from other users)
REACTION_RELAYS = [
    STRFRY_URL,
] + PUBLIC_RELAYS + [
    "wss://nostr.mom",
    "wss://search.nos.today",
    "wss://offchain.pub",
]


async def _fetch_from_relay(relay_url, filters, timeout=RECV_TIMEOUT, limit_events=50000,
                            paginate=False):
    """Generic fetch: send REQ with filters, collect EVENTs until EOSE.

    If paginate=True, automatically pages through results using 'until' to
    get past relay-side limits (typically 500 events per query).
    """
    all_events = []
    seen_ids = set()
    current_filters = dict(filters)

    try:
        async with websockets.connect(relay_url, open_timeout=CONNECT_TIMEOUT, close_timeout=5) as ws:
            while len(all_events) < limit_events:
                sub_id = f"disc-{secrets.token_hex(4)}-{len(all_events)}"
                await ws.send(json.dumps(["REQ", sub_id, current_filters]))

                page_events = []
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    except asyncio.TimeoutError:
                        break
                    data = json.loads(msg)
                    if data[0] == "EVENT" and data[1] == sub_id:
                        ev = data[2]
                        eid = ev.get("id")
                        if eid and eid not in seen_ids and nostr_crypto.verify_event(ev):
                            seen_ids.add(eid)
                            page_events.append(ev)
                            all_events.append(ev)
                            if len(all_events) >= limit_events:
                                break
                    elif data[0] == "EOSE":
                        break

                try:
                    await ws.send(json.dumps(["CLOSE", sub_id]))
                except Exception:
                    pass

                # If not paginating, got no events, or fewer than a typical relay page,
                # we've got everything
                if not paginate or not page_events or len(page_events) < 450:
                    break

                # Page backward: set 'until' to oldest event in this page
                oldest_ts = min(ev.get("created_at", 0) for ev in page_events)
                current_filters = dict(filters)
                current_filters["until"] = oldest_ts

    except Exception as e:
        logger.debug(f"Fetch from {relay_url} failed: {e}")
    return all_events


async def discover_relays(pubkey: str) -> list[str]:
    """Discover a user's preferred relays from their kind 10002 relay list.

    Strategy:
    1. Check local strfry for kind 10002
    2. Check purplepag.es (relay list aggregator)
    3. Check 3 big relays in parallel
    4. Merge and return unique relay URLs
    """
    all_relay_events = []

    # Phase 1: local strfry + purplepag.es (fast, most likely to have it)
    phase1_relays = [STRFRY_URL, "wss://purplepag.es"]
    phase1_tasks = [
        _fetch_from_relay(r, {"kinds": [10002], "authors": [pubkey], "limit": 5}, timeout=8)
        for r in phase1_relays
    ]
    phase1_results = await asyncio.gather(*phase1_tasks)
    for events in phase1_results:
        all_relay_events.extend(events)

    # Phase 2: if we didn't find it, try big relays
    if not all_relay_events:
        phase2_relays = list(PUBLIC_RELAYS)
        phase2_tasks = [
            _fetch_from_relay(r, {"kinds": [10002], "authors": [pubkey], "limit": 5}, timeout=8)
            for r in phase2_relays
        ]
        phase2_results = await asyncio.gather(*phase2_tasks)
        for events in phase2_results:
            all_relay_events.extend(events)

    if not all_relay_events:
        logger.debug(f"No relay list found for {pubkey[:16]}, using bootstrap + essential relays")
        fallback = list(BOOTSTRAP_RELAYS)
        for r in ESSENTIAL_RELAYS:
            if r not in fallback:
                fallback.append(r)
        if STRFRY_URL not in fallback:
            fallback.insert(0, STRFRY_URL)
        return fallback

    # Pick the most recent kind 10002 event
    best = max(all_relay_events, key=lambda e: e.get("created_at", 0))

    # Parse relay URLs from "r" tags
    user_relays = []
    for tag in best.get("tags", []):
        if len(tag) >= 2 and tag[0] == "r":
            url = tag[1].rstrip("/")
            # Only add write relays or unspecified (both read+write)
            # tag[2] can be "read", "write", or absent (both)
            marker = tag[2] if len(tag) > 2 else ""
            if marker != "read":  # include write-only and unspecified
                user_relays.append(url)

    # Also add read relays — user's data may be there too
    for tag in best.get("tags", []):
        if len(tag) >= 2 and tag[0] == "r":
            url = tag[1].rstrip("/")
            if url not in user_relays:
                user_relays.append(url)

    # Cap user relays to prevent resource exhaustion from malicious relay lists
    user_relays = user_relays[:30]

    # Filter out unusable relays (Tor, localhost, private IPs, our own domain)
    import ipaddress
    import re
    import socket
    _PRIVATE_HOST_RE = re.compile(
        r'(localhost|127\.|0\.0\.0\.0|::1|\.local'
        r'|10\.\d+\.\d+\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+\.\d+|192\.168\.'
        r'|169\.254\.|fc00:|fd00:|fe80:|\[fc|\[fd|\[fe80|\[::1\])',
        re.IGNORECASE
    )

    async def _relay_resolves_safe(url: str) -> bool:
        """DNS-resolve relay hostname and reject private/reserved IPs (async)."""
        try:
            from urllib.parse import urlparse
            host = urlparse(url).hostname
            if not host:
                return False
            loop = asyncio.get_event_loop()
            addrs = await asyncio.wait_for(
                loop.getaddrinfo(host, None, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM),
                timeout=5,
            )
            for family, _, _, _, sockaddr in addrs:
                ip = ipaddress.ip_address(sockaddr[0])
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                    return False
        except (socket.gaierror, ValueError, OSError, asyncio.TimeoutError):
            return False
        return True

    filtered = []
    for r in user_relays:
        # Skip onion addresses (can't reach from container without Tor)
        if ".onion" in r:
            continue
        # Skip private/internal addresses (SSRF protection — string check)
        if _PRIVATE_HOST_RE.search(r):
            continue
        # Skip non-websocket URLs
        if not r.startswith(("wss://", "ws://")):
            continue
        # Only allow wss:// for external relays (ws:// only for internal strfry)
        if r.startswith("ws://") and r != STRFRY_URL:
            continue
        # Replace our own domain with local strfry
        if RELAY_DOMAIN in r:
            if STRFRY_URL not in filtered:
                filtered.append(STRFRY_URL)
            continue
        # DNS resolution check — reject URLs that resolve to private IPs
        if not await _relay_resolves_safe(r):
            logger.debug(f"Skipping relay {r}: resolves to private/reserved IP")
            continue
        filtered.append(r)
    user_relays = filtered

    # Merge with essential relays for maximum coverage
    for r in ESSENTIAL_RELAYS:
        if r not in user_relays:
            user_relays.append(r)

    # Always include local strfry
    if STRFRY_URL not in user_relays:
        user_relays.insert(0, STRFRY_URL)

    logger.info(f"Discovered {len(user_relays)} relays for {pubkey[:16]}")
    return user_relays


async def fetch_author_events(pubkey: str, relays: list[str], since: int = 0,
                               limit: int = 50000) -> list[dict]:
    """Fetch all events authored by pubkey from multiple relays in parallel.

    Also does a separate targeted fetch for replaceable events (kind 0, 3, 10002)
    which some relays omit from general author queries.
    """
    filters = {"authors": [pubkey], "limit": limit}
    if since > 0:
        filters["since"] = since

    # Replaceable event kinds that must not be missed
    replaceable_filters = {"authors": [pubkey], "kinds": [0, 3, 10002], "limit": 10}

    # Run general fetch + targeted replaceable fetch in parallel across all relays
    general_tasks = [_fetch_from_relay(r, filters, timeout=20, paginate=True) for r in relays]
    replaceable_tasks = [_fetch_from_relay(r, replaceable_filters, timeout=10) for r in relays]
    all_results = await asyncio.gather(*(general_tasks + replaceable_tasks))

    seen = set()
    events = []
    for relay_events in all_results:
        for ev in relay_events:
            eid = ev.get("id")
            if eid and eid not in seen:
                seen.add(eid)
                events.append(ev)

    return events


async def fetch_tagged_events(pubkey: str, relays: list[str], kinds: list[int],
                               since: int = 0, limit: int = 10000) -> list[dict]:
    """Fetch events that tag a pubkey (reactions, zaps, etc.) from multiple relays."""
    filters = {"kinds": kinds, "#p": [pubkey], "limit": limit}
    if since > 0:
        filters["since"] = since

    tasks = [_fetch_from_relay(r, filters, timeout=15, paginate=True) for r in relays]
    results = await asyncio.gather(*tasks)

    seen = set()
    events = []
    for relay_events in results:
        for ev in relay_events:
            eid = ev.get("id")
            if eid and eid not in seen:
                seen.add(eid)
                events.append(ev)

    return events


async def fetch_profiles(pubkeys: list[str], relays: list[str] = None) -> dict:
    """Fetch kind 0 profiles for a list of pubkeys. Returns {pubkey: {name, picture, nip05}}."""
    if not pubkeys:
        return {}

    if relays is None:
        from relay_constants import EXTENDED_RELAYS
        relays = [STRFRY_URL] + PUBLIC_RELAYS + [
            "wss://relay.ditto.pub",
            "wss://relay.noswhere.com",
            "wss://purplepag.es",
        ] + EXTENDED_RELAYS

    tasks = [
        _fetch_from_relay(r, {"kinds": [0], "authors": pubkeys, "limit": len(pubkeys) * 3}, timeout=8)
        for r in relays
    ]
    results = await asyncio.gather(*tasks)

    # Keep the most recent profile per pubkey
    best = {}
    for relay_events in results:
        for ev in relay_events:
            pk = ev.get("pubkey", "")
            if pk and (pk not in best or ev.get("created_at", 0) > best[pk].get("created_at", 0)):
                best[pk] = ev

    profiles = {}
    for pk, ev in best.items():
        try:
            meta = json.loads(ev.get("content", "{}"))
            profiles[pk] = {
                "name": meta.get("display_name") or meta.get("name") or "",
                "picture": meta.get("picture") or "",
                "nip05": meta.get("nip05") or "",
                "about": meta.get("about") or "",
                "lud16": meta.get("lud16") or meta.get("lud06") or "",
            }
        except (json.JSONDecodeError, AttributeError):
            pass

    return profiles


async def push_to_strfry(events: list[dict]) -> int:
    """Push events into local strfry relay. Returns count sent.

    Sorts events so the newest replaceable events (kind 0, 3, 10002) are pushed
    last, ensuring strfry keeps the most recent version.
    """
    if not events:
        return 0

    # Replaceable event kinds (NIP-01: kind 0, 3; NIP-65: 10002)
    REPLACEABLE_KINDS = {0, 3, 10002}

    # Sort: regular events first (by created_at asc), then replaceable events last (by created_at asc)
    # This ensures the newest replaceable event is the last one pushed for each kind
    regular = sorted(
        [e for e in events if e.get("kind", 0) not in REPLACEABLE_KINDS],
        key=lambda e: e.get("created_at", 0)
    )
    replaceable = sorted(
        [e for e in events if e.get("kind", 0) in REPLACEABLE_KINDS],
        key=lambda e: e.get("created_at", 0)
    )
    sorted_events = regular + replaceable

    sent = 0
    try:
        async with websockets.connect(STRFRY_URL, open_timeout=5, close_timeout=10) as ws:
            for event in sorted_events:
                await ws.send(json.dumps(["EVENT", event]))
                sent += 1
                try:
                    await asyncio.wait_for(ws.recv(), timeout=2)
                except asyncio.TimeoutError:
                    pass
                # Throttle: small pause every 50 events, longer pause every 500
                if sent % 500 == 0:
                    await asyncio.sleep(0.5)
                elif sent % 50 == 0:
                    await asyncio.sleep(0.1)
    except websockets.exceptions.ConnectionClosedOK:
        pass  # Normal close
    except Exception as e:
        if sent > 0:
            logger.debug(f"Push to strfry connection closed after {sent} events: {e}")
        else:
            logger.warning(f"Push to strfry failed: {e}")
    return sent


async def full_sync(pubkey: str, since: int = 0) -> tuple:
    """Full discovery + sync for a pubkey.

    1. Discover the user's relay list
    2. Fetch all their authored events from those relays
    3. Push to local strfry
    Returns total events pushed.
    """
    relays = await discover_relays(pubkey)
    events = await fetch_author_events(pubkey, relays, since=since)

    if not events:
        return 0, 0

    pushed = await push_to_strfry(events)
    max_ts = max(ev.get("created_at", 0) for ev in events)

    logger.info(f"Full sync for {pubkey[:16]}: {pushed} events from {len(relays)} relays")
    return pushed, max_ts
