"""Decentralized Lists (kind 9998/9999) for the nostrbtc directory.

Publishes a list header (kind 9998) defining the directory, and individual
list items (kind 9999) for each member. Any Nostr client can fetch these
events to discover the full directory without hitting our API.

Protocol reference: Decentralized Lists
  - kind 9998: list header (schema definition)
  - kind 9999: list item (one per member, references header via z-tag)
"""

import asyncio
import logging
import os

import db
import discovery
import nostr_auth
import nostr_crypto

logger = logging.getLogger("nostrbtc.decentralized_list")

RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

from relay_constants import PUBLIC_RELAYS

_RELAY_SEM = asyncio.Semaphore(3)


async def _publish_to_relay(relay_url, event):
    """Publish with semaphore to limit concurrent connections."""
    async with _RELAY_SEM:
        return await nostr_auth._send_to_relay(relay_url, event, timeout=8)


async def _publish_event(event):
    """Publish an event to strfry + public relays. Returns success count."""
    targets = [discovery.STRFRY_URL] + PUBLIC_RELAYS
    tasks = [_publish_to_relay(url, event) for url in targets]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return sum(1 for r in results if r is True)


# ---------------------------------------------------------------------------
# Step 1: List header (kind 9998)
# ---------------------------------------------------------------------------

async def publish_list_header() -> str:
    """Publish the directory list header event. Returns the event id."""
    if not nostr_auth.RELAY_PRIVATE_KEY:
        logger.warning("No relay private key, skipping list header")
        return ""

    tags = [
        ["names", "nostrbtc directory member", "nostrbtc directory members"],
        ["description",
         f"Paid subscriber directory for {RELAY_DOMAIN} \u2014 Bitcoin-native "
         "Nostr relay with personalized trust ranking powered by GrapeRank."],
        ["required", "p"],
        ["recommended", "name"],
        ["recommended", "nip05"],
    ]

    event = nostr_crypto.make_event(
        nostr_auth.RELAY_PRIVATE_KEY,
        kind=9998,
        content="",
        tags=tags,
    )

    ok = await _publish_event(event)
    if ok > 0:
        db.set_relay_state("list_header_event_id", event["id"])
        logger.info(f"Published list header: {event['id']} ({ok} relays)")
    else:
        logger.error("List header failed on all relays")

    return event["id"]


async def ensure_list_header() -> str:
    """Ensure the list header exists. Publish if not."""
    existing = db.get_relay_state("list_header_event_id")
    if existing:
        logger.info(f"List header already exists: {existing}")
        return existing
    return await publish_list_header()


# ---------------------------------------------------------------------------
# Step 2: Member items (kind 9999)
# ---------------------------------------------------------------------------

async def publish_member_item(pubkey: str, name: str, nip05: str,
                              list_header_id: str) -> str:
    """Publish a kind 9999 list item for a directory member. Returns event id."""
    if not nostr_auth.RELAY_PRIVATE_KEY:
        return ""
    if not list_header_id:
        logger.warning("No list header id, skipping member item publish")
        return ""

    tags = [
        ["z", list_header_id],
        ["p", pubkey],
    ]
    # Do not publish subscriber activity, join, expiry, or last-seen timestamps.
    # Public list items only attest current membership and optional profile labels.
    if name:
        tags.append(["name", name])
    if nip05:
        tags.append(["nip05", nip05])

    event = nostr_crypto.make_event(
        nostr_auth.RELAY_PRIVATE_KEY,
        kind=9999,
        content="",
        tags=tags,
    )

    ok = await _publish_event(event)
    if ok > 0:
        logger.info(f"Published member item for {pubkey[:16]}... ({ok} relays)")
    else:
        logger.debug(f"Member item for {pubkey[:16]}... failed on all relays")

    return event["id"]


async def publish_and_store_member_item(pubkey: str):
    """Publish kind 9999 for a member and store the event id in Postgres."""
    list_header_id = db.get_relay_state("list_header_event_id")
    if not list_header_id:
        logger.warning("No list header, skipping member item")
        return

    # Get profile data for name/nip05
    member = db.get_directory_member(pubkey)
    name = member["name"] if member else ""
    nip05 = member.get("nip05_display", "") if member else ""

    event_id = await publish_member_item(pubkey, name, nip05, list_header_id)
    if event_id:
        db.set_list_event_id(pubkey, event_id)


async def migrate_existing_members():
    """One-time migration: publish kind 9999 for all existing directory members."""
    if db.get_relay_state("members_migrated"):
        logger.info("Members already migrated to kind 9999")
        return

    list_header_id = db.get_relay_state("list_header_event_id")
    if not list_header_id:
        logger.warning("No list header, skipping member migration")
        return

    members = db.get_all_directory_members()
    if not members:
        logger.info("No members to migrate")
        return

    migrated = 0
    for m in members:
        try:
            event_id = await publish_member_item(
                m["pubkey"],
                m.get("name", ""),
                m.get("nip05_display", ""),
                list_header_id,
            )
            if event_id:
                db.set_list_event_id(m["pubkey"], event_id)
                migrated += 1
        except Exception:
            logger.exception(f"Failed to migrate {m['pubkey'][:16]}...")

    db.set_relay_state("members_migrated", "1")
    logger.info(f"Migrated {migrated}/{len(members)} members to kind 9999")


# ---------------------------------------------------------------------------
# Step 5: Deletion (NIP-09)
# ---------------------------------------------------------------------------

async def delete_member_item(pubkey: str):
    """Publish a NIP-09 deletion for a member's kind 9999 event."""
    if not nostr_auth.RELAY_PRIVATE_KEY:
        return

    event_id = db.get_list_event_id(pubkey)
    if not event_id:
        logger.debug(f"No list event id for {pubkey[:16]}..., nothing to delete")
        return

    event = nostr_crypto.make_event(
        nostr_auth.RELAY_PRIVATE_KEY,
        kind=5,
        content="removed from directory",
        tags=[["e", event_id]],
    )

    ok = await _publish_event(event)
    if ok > 0:
        db.set_list_event_id(pubkey, None)
        db.remove_directory_member_redis(pubkey)
        logger.info(f"Deleted member item for {pubkey[:16]}... ({ok} relays)")
    else:
        logger.warning(f"Deletion for {pubkey[:16]}... failed on all relays")
