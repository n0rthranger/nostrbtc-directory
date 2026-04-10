# GrapeRank Interpreter — Convert Nostr events into GrapeRank ratings
#
# This file contains code derived from GrapeRank.
# Copyright (c) Pretty-Good-Freedom-Tech / NosFabrica
#
# Original implementation (TypeScript):
#   https://github.com/Pretty-Good-Freedom-Tech/graperank-nodejs
#
# Reference implementation:
#   https://github.com/NosFabrica/brainstorm_graperank_algorithm
#
# Algorithm design: David Strayhorn (straycat)
#   https://njump.me/npub1u5njm6g5h5cpw4wy8xugu62e5s7f6fnysv0sj0z3a8rengt2zqhsxrldq3
#
# Licensed under AGPL-3.0 — see LICENSE file in project root.
#
# Python port and relay integration.

"""
Interpreter: converts Nostr social graph data into GrapeRank Rating objects.

Data sources:
- trust_edges table (Postgres): follows, reposts, reactions, zaps
- strfry WebSocket queries: kind 10000 mutes, kind 1984 reports
"""

import json
import logging
from typing import List, Optional

from graperank import (
    Rating,
    DEFAULT_FOLLOW_SCORE,
    DEFAULT_MUTE_SCORE,
    DEFAULT_REPORT_SCORE,
    DEFAULT_OBSERVER_CONFIDENCE,
    DEFAULT_FOLLOW_CONFIDENCE,
    DEFAULT_MUTE_CONFIDENCE,
    DEFAULT_REPORT_CONFIDENCE,
)

logger = logging.getLogger("directory.interpreter")


def build_ratings(
    observer: str,
    member_pubkeys: List[str],
    pg_conn,
    strfry_url: str = "ws://strfry:7777",
) -> List[Rating]:
    """
    Build a list of Rating objects from all available data sources.

    Args:
        observer: The relay operator pubkey (or any observer pubkey).
        member_pubkeys: List of directory member pubkeys to score.
        pg_conn: Active psycopg2 connection to Postgres.
        strfry_url: WebSocket URL for strfry relay.

    Returns:
        List of Rating objects ready for graperank().
    """
    ratings = []

    # 1. Follows from strfry's full kind 3 graph (the bulk of the data)
    ratings.extend(_ratings_from_follows(observer, member_pubkeys, pg_conn, strfry_url))

    # 2. Mutes from strfry (kind 10000)
    ratings.extend(_ratings_from_strfry(
        observer, member_pubkeys, strfry_url,
        kind=10000, score=DEFAULT_MUTE_SCORE, confidence=DEFAULT_MUTE_CONFIDENCE,
        label="mutes"
    ))

    # 3. Reports from strfry (kind 1984)
    ratings.extend(_ratings_from_strfry(
        observer, member_pubkeys, strfry_url,
        kind=1984, score=DEFAULT_REPORT_SCORE, confidence=DEFAULT_REPORT_CONFIDENCE,
        label="reports"
    ))

    logger.info(f"Interpreter: built {len(ratings)} ratings for {len(member_pubkeys)} members")
    return ratings


def _ratings_from_follows(
    observer: str,
    member_pubkeys: List[str],
    pg_conn,
    strfry_url: str = "ws://strfry:7777",
) -> List[Rating]:
    """Build follow ratings from the full Nostr follow graph in strfry.

    Queries strfry for ALL kind 3 (contact list) events that tag directory
    members. This captures the wide follow graph — any pubkey in the relay's
    event store that follows a directory member contributes a rating, not just
    edges between members.
    """
    if not member_pubkeys:
        return []

    ratings = []
    member_set = set(member_pubkeys)

    try:
        import asyncio
        import websockets

        async def _fetch_follows():
            """Fetch kind 3 events tagging directory members from strfry."""
            all_events = []

            async with websockets.connect(strfry_url, close_timeout=10, open_timeout=10) as ws:
                # Query kind 3 events that tag any directory member
                # strfry supports #p filter for p-tag lookups
                for i in range(0, len(member_pubkeys), 100):
                    batch = member_pubkeys[i:i + 100]
                    sid = f"interp-k3-{i}"
                    await ws.send(json.dumps(["REQ", sid, {
                        "kinds": [3],
                        "#p": batch,
                        "limit": 50000,
                    }]))
                    async for msg in ws:
                        data = json.loads(msg)
                        if data[0] == "EVENT" and data[1] == sid:
                            all_events.append(data[2])
                        elif data[0] == "EOSE" and data[1] == sid:
                            break
                    await ws.send(json.dumps(["CLOSE", sid]))

            return all_events

        # Run async fetch from sync context (called via asyncio.to_thread)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            events = asyncio.new_event_loop().run_until_complete(_fetch_follows())
        else:
            events = asyncio.run(_fetch_follows())

        # Each kind 3 event = one user's full contact list
        # Extract all p-tags that point to directory members
        seen_pairs = set()
        for ev in events:
            rater = ev.get("pubkey", "")
            if not rater:
                continue
            tags = ev.get("tags", [])
            for tag in tags:
                if tag[0] == "p" and len(tag) >= 2 and len(tag[1]) == 64:
                    ratee = tag[1]
                    if ratee in member_set:
                        key = (rater, ratee)
                        if key not in seen_pairs:
                            seen_pairs.add(key)
                            if rater == observer:
                                confidence = DEFAULT_OBSERVER_CONFIDENCE
                            else:
                                confidence = DEFAULT_FOLLOW_CONFIDENCE
                            ratings.append(Rating(rater, ratee, DEFAULT_FOLLOW_SCORE, confidence))

        unique_raters = len({r.rater for r in ratings})
        logger.info(f"Interpreter: {len(ratings)} follow ratings from {len(events)} kind 3 events ({unique_raters} unique raters)")

    except Exception as e:
        logger.error(f"Interpreter: follow graph fetch failed: {e}")

    return ratings


def _ratings_from_strfry(
    observer: str,
    member_pubkeys: List[str],
    strfry_url: str,
    kind: int,
    score: float,
    confidence: float,
    label: str,
) -> List[Rating]:
    """Fetch events from strfry and convert to ratings."""
    if not member_pubkeys:
        return []

    ratings = []
    member_set = set(member_pubkeys)

    try:
        import asyncio
        import websockets

        async def _fetch():
            async with websockets.connect(strfry_url, close_timeout=5, open_timeout=10) as ws:
                await ws.send(json.dumps(["REQ", f"interp-{kind}", {
                    "kinds": [kind],
                    "authors": member_pubkeys,
                    "limit": 50000,
                }]))
                evts = []
                async for msg in ws:
                    data = json.loads(msg)
                    if data[0] == "EVENT":
                        evts.append(data[2])
                    elif data[0] == "EOSE":
                        break
                await ws.send(json.dumps(["CLOSE", f"interp-{kind}"]))
                return evts

        # Run async fetch — works from sync context (called via asyncio.to_thread)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # We're inside asyncio.to_thread — create a new event loop in this thread
            events = asyncio.new_event_loop().run_until_complete(_fetch())
        else:
            events = asyncio.run(_fetch())

        # Process events: extract p-tags as targets
        for ev in events:
            rater = ev.get("pubkey", "")
            tags = ev.get("tags", [])
            for tag in tags:
                if tag[0] == "p" and len(tag) >= 2 and len(tag[1]) == 64:
                    ratee = tag[1]
                    if ratee in member_set:
                        c = confidence
                        ratings.append(Rating(rater, ratee, score, c))

        logger.info(f"Interpreter: {len(ratings)} {label} ratings from {len(events)} events")
    except ImportError:
        logger.warning("Interpreter: websockets not available, skipping strfry query")
    except Exception as e:
        logger.warning(f"Interpreter: {label} fetch failed: {e}")

    return ratings


def build_ratings_sync(
    observer: str,
    member_pubkeys: List[str],
    pg_conn,
    strfry_url: str = "ws://strfry:7777",
) -> List[Rating]:
    """Synchronous wrapper — same as build_ratings (already sync)."""
    return build_ratings(observer, member_pubkeys, pg_conn, strfry_url)
