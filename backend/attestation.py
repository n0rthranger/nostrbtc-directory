"""Publish relay-signed attestation events (kind 30078) for directory members.

Each directory member gets a parameterized replaceable event signed by the relay
keypair, attesting to their verification status. Published to local strfry and
a few public relays so any Nostr client can consume it as a trust primitive.
"""

import asyncio
import json
import logging
import os
import time

import db
import discovery
import nostr_auth
import nostr_crypto

logger = logging.getLogger("nostrbtc.attestation")

RELAY_DOMAIN = os.environ.get("RELAY_DOMAIN", "your-relay.example.com")

from relay_constants import PUBLIC_RELAYS
from decentralized_list import _publish_to_relay


async def publish_attestations():
    """Publish kind 30078 attestation for every directory member."""
    if not nostr_auth.RELAY_PRIVATE_KEY:
        logger.warning("No relay private key, skipping attestations")
        return

    members = db.get_all_directory_members()

    if not members:
        return

    published = 0
    now = int(time.time())

    for row in members:
        try:
            d_tag = f"verification:{row['pubkey']}"
            content = json.dumps({
                "type": "nostrbtc-verification",
                "version": 1,
                "pubkey": row["pubkey"],
                "npub": row["npub"],
                "name": row["name"],
                "badges": json.loads(row.get("badges", "[]")),
                "reputation_score": row.get("reputation_score", 0),
                "subscriber_since": row.get("subscription_created", ""),
                "last_active": row.get("last_active", 0),
                "event_count": row.get("event_count", 0),
                "verified_at": now,
            })

            tags = [
                ["d", d_tag],
                ["p", row["pubkey"]],
                ["L", RELAY_DOMAIN],
                ["l", "verified-subscriber", RELAY_DOMAIN],
            ]

            # Add badge labels as l tags for discoverability
            for badge in json.loads(row.get("badges", "[]")):
                tags.append(["l", badge, RELAY_DOMAIN])

            event = nostr_crypto.make_event(
                nostr_auth.RELAY_PRIVATE_KEY,
                kind=30078,
                content=content,
                tags=tags,
            )

            # Publish to local strfry + public relays
            targets = [discovery.STRFRY_URL] + PUBLIC_RELAYS
            tasks = [_publish_to_relay(url, event) for url in targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            ok = sum(1 for r in results if r is True)
            if ok > 0:
                published += 1
            else:
                logger.debug(f"Attestation for {row['npub'][:20]} failed on all relays")

        except Exception:
            logger.exception(f"Failed to build attestation for {row.get('npub', '?')[:20]}")

    logger.info(f"Published attestations: {published}/{len(members)} members")
